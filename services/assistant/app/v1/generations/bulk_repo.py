"""BulkGenerationJobRepo: submission, idempotent replay, the worker loop, cancel (E12, AST-D23).

Submission takes an ``Idempotency-Key``: the same key and body return the
same job; the same key with a different body is ``409``. The durable claim is
the ``(merchant_id, idempotency_key)`` unique (closes AST-Q4 without Redis).

The worker processes one item per transaction, checks for cancellation
between items, skips items already done (a redelivered task), counts
provider failures per item, and aborts the job when the budget is spent.
"""

import asyncio
import hashlib
import json
import logging
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clients.llm import LLMError, LLMProvider
from app.core.config import Settings
from app.core.enums import ContentType
from app.core.errors import BudgetExceeded, BulkJobNotFound, BulkJobTerminal, IdempotencyKeyReused
from app.v1.generations.budget_repo import BudgetRepo
from app.v1.generations.enums import BulkJobStatus
from app.v1.generations.models import BulkGenerationJob, Generation
from app.v1.generations.repo import (
    GenerationParams,
    GenerationRecord,
    GenerationRepo,
    ProductSnapshot,
    build_request,
)
from app.v1.generations.utils import TERMINAL, check_transition, final_status
from app.v1.prompts.repo import PromptRepo
from smartretail_common.db import Database
from smartretail_common.errors import DependencyUnavailable

logger = logging.getLogger("assistant.bulk")

ITEM_ATTEMPTS = 3


def request_hash(body: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()


class BulkJobRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def by_key(self, merchant_id: str, key: str) -> BulkGenerationJob | None:
        return await self.session.scalar(
            select(BulkGenerationJob).where(
                BulkGenerationJob.merchant_id == uuid.UUID(merchant_id),
                BulkGenerationJob.idempotency_key == key,
            )
        )

    async def replay(self, merchant_id: str, key: str, digest: str) -> BulkGenerationJob | None:
        job = await self.by_key(merchant_id, key)
        if job is not None and job.request_hash != digest:
            raise IdempotencyKeyReused()
        return job

    async def create(
        self,
        merchant_id: str,
        key: str,
        digest: str,
        params: GenerationParams,
        snapshots: list[ProductSnapshot],
    ) -> tuple[BulkGenerationJob, bool]:
        """Inserts the job; on a concurrent insert with the same key returns that job."""
        job = BulkGenerationJob(
            merchant_id=uuid.UUID(merchant_id),
            content_type=params.content_type,
            product_ids=[s.product_id for s in snapshots],
            status=BulkJobStatus.QUEUED,
            idempotency_key=key,
            request_hash=digest,
            inputs={
                "params": {**params.as_dict(), "content_type": str(params.content_type)},
                "products": [s.as_dict() for s in snapshots],
            },
        )
        self.session.add(job)
        try:
            await self.session.commit()
        except IntegrityError:
            await self.session.rollback()
            existing = await self.replay(merchant_id, key, digest)
            assert existing is not None
            return existing, False
        return job, True

    async def owned(self, job_id: uuid.UUID, merchant_id: str) -> BulkGenerationJob:
        job = await self.session.get(BulkGenerationJob, job_id)
        if job is None or str(job.merchant_id) != merchant_id:
            raise BulkJobNotFound()
        return job

    async def generations(self, job_id: uuid.UUID) -> list[Generation]:
        return list(
            (
                await self.session.scalars(
                    select(Generation)
                    .where(Generation.bulk_job_id == job_id)
                    .order_by(Generation.created_at)
                )
            ).all()
        )

    async def cancel(self, job_id: uuid.UUID, merchant_id: str) -> BulkGenerationJob:
        job = await self.owned(job_id, merchant_id)
        await self.session.refresh(job, with_for_update=True)
        assert job.status is not None
        if job.status in TERMINAL:
            raise BulkJobTerminal(f"The job is already {job.status}")
        check_transition(job.status, BulkJobStatus.CANCELLED)
        job.status = BulkJobStatus.CANCELLED
        job.finished_at = datetime.now(UTC)
        await self.session.commit()
        return job


async def run_bulk_job(
    db: Database, provider: LLMProvider, settings: Settings, job_id: uuid.UUID
) -> BulkJobStatus | None:
    """The body of ``generate_bulk_content``. Idempotent under redelivery."""
    async with db.sessionmaker() as session, session.begin():
        job = await session.get(BulkGenerationJob, job_id, with_for_update=True)
        if job is None or job.status in TERMINAL:
            return job.status if job else None
        if job.status == BulkJobStatus.QUEUED:
            check_transition(BulkJobStatus.QUEUED, BulkJobStatus.RUNNING)
            job.status = BulkJobStatus.RUNNING
        params_raw = dict(job.inputs["params"])
        params = GenerationParams(
            content_type=ContentType(params_raw["content_type"]),
            tone=params_raw.get("tone"),
            max_length=params_raw.get("max_length"),
            keywords=list(params_raw.get("keywords") or []),
        )
        snapshots = [ProductSnapshot.from_dict(p) for p in job.inputs["products"]]
        merchant_scope = f"user:{job.merchant_id}"

    for snap in snapshots:
        async with db.sessionmaker() as session:
            job = await session.get(BulkGenerationJob, job_id)
            assert job is not None
            if job.status == BulkJobStatus.CANCELLED:
                logger.info("bulk job cancelled", extra={"bulk_job_id": str(job_id)})
                return BulkJobStatus.CANCELLED
            done = await session.scalar(
                select(Generation.id).where(
                    Generation.bulk_job_id == job_id, Generation.product_id == snap.product_id
                )
            )
            if done is not None or str(snap.product_id) in (job.item_errors or {}):
                continue  # processed by an earlier delivery
            try:
                await BudgetRepo(session, settings).check(merchant_scope)
            except BudgetExceeded:
                await session.rollback()
                return await _finish(db, job_id, aborted="daily token budget exhausted")
            request, prompt_version = await build_request(
                PromptRepo(session, settings.prompts_dir), settings, snap, params
            )
        started = time.monotonic()
        error: Exception | None = None
        for attempt in range(ITEM_ATTEMPTS):
            try:
                result = await provider.complete(request)
                error = None
                break
            except (DependencyUnavailable, LLMError) as exc:
                error = exc
                if isinstance(exc, LLMError):
                    break
                await asyncio.sleep(min(0.5 * 2**attempt, 4.0))
        async with db.sessionmaker() as session, session.begin():
            job = await session.get(BulkGenerationJob, job_id, with_for_update=True)
            assert job is not None
            if job.status == BulkJobStatus.CANCELLED:
                return BulkJobStatus.CANCELLED
            if error is not None:
                job.failed += 1
                job.item_errors = {**(job.item_errors or {}), str(snap.product_id): str(error)}
                continue
            await GenerationRepo(session, settings).insert(
                GenerationRecord(
                    generation_id=uuid.uuid4(),
                    snapshot=snap,
                    params=params,
                    content=result.text,
                    prompt_tokens=result.input_tokens,
                    completion_tokens=result.output_tokens,
                    model=result.model,
                    finish_reason=result.finish_reason,
                    latency_ms=int((time.monotonic() - started) * 1000),
                    time_to_first_token_ms=None,
                    prompt_version=prompt_version,
                    correlation_id=f"bulk:{job_id}",
                    bulk_job_id=job_id,
                )
            )
            job.completed += 1
    return await _finish(db, job_id)


async def _finish(db: Database, job_id: uuid.UUID, *, aborted: str | None = None) -> BulkJobStatus:
    async with db.sessionmaker() as session, session.begin():
        job = await session.get(BulkGenerationJob, job_id, with_for_update=True)
        assert job is not None and job.status is not None
        if job.status in TERMINAL:
            return job.status
        target = BulkJobStatus.FAILED if aborted else final_status(job.completed, job.failed)
        check_transition(job.status, target)
        job.status = target
        job.finished_at = datetime.now(UTC)
        if aborted:
            job.item_errors = {**(job.item_errors or {}), "_job": aborted}
        logger.info(
            "bulk job finished",
            extra={"bulk_job_id": str(job_id), "status": str(target), "completed": job.completed},
        )
        return target
