"""Reindex from stored source documents: ``python -m app.v1.search.reindex`` (§3.1.1.4, AST-D36).

Rebuilds chunks for every non-removed product from ``index_state.source_document``
(the last event payload), with the same ``index_payload`` the indexer runs —
after a model change, a chunking change, or to repair. One transaction per
product, ordered by ``product_id`` with a cursor: resumable and idempotent,
and an unchanged product costs zero embedding calls (D6).

A full run holds a Postgres advisory lock, so a second full run is refused
(``409`` from the API). Products never seen by the indexer are backfilled by
the indexer itself: a new consumer group reads the compacted
``catalog.product.published`` topic from the earliest offset (AST-Q1, AST-D36).
"""

import argparse
import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

import httpx
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.core.db import create_database
from app.core.logger import init_logger
from app.v1.search.clients.embeddings import EmbeddingProvider, build_embeddings
from app.v1.search.enums import IndexOutcome, IndexStatus
from app.v1.search.indexing_repo import IndexingRepo, mark_stale_models
from app.v1.search.models import IndexState
from app.v1.search.utils import check_transition
from smartretail_common.db import Database
from smartretail_common.errors import DependencyUnavailable

logger = logging.getLogger("assistant.reindex")

REINDEX_LOCK_KEY = 0x41535452  # "ASTR"


@dataclass(frozen=True)
class ReindexScope:
    product_ids: tuple[uuid.UUID, ...] = ()
    merchant_id: uuid.UUID | None = None

    @property
    def full(self) -> bool:
        return not self.product_ids and self.merchant_id is None

    def as_dict(self) -> dict[str, object]:
        return {
            "product_ids": [str(p) for p in self.product_ids],
            "merchant_id": str(self.merchant_id) if self.merchant_id else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ReindexScope":
        merchant = data.get("merchant_id")
        return cls(
            product_ids=tuple(uuid.UUID(str(p)) for p in data.get("product_ids") or []),
            merchant_id=uuid.UUID(str(merchant)) if merchant else None,
        )


@dataclass
class ReindexReport:
    indexed: int = 0
    skipped: int = 0
    failed: int = 0
    running_elsewhere: bool = False
    errors: dict[str, str] = field(default_factory=dict)


async def reindex_running(session: AsyncSession) -> bool:
    held = await session.scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted"
            " AND objid = :key AND database = (SELECT oid FROM pg_database"
            " WHERE datname = current_database()))"
        ),
        {"key": REINDEX_LOCK_KEY},
    )
    return bool(held)


async def _mark_failed(db: Database, product_id: uuid.UUID, error: str) -> None:
    async with db.sessionmaker() as session, session.begin():
        state = await session.get(IndexState, product_id, with_for_update=True)
        if state is None or state.status not in (IndexStatus.INDEXED, IndexStatus.PENDING):
            return
        if state.status == IndexStatus.INDEXED:
            check_transition(state.status, IndexStatus.PENDING)
        check_transition(IndexStatus.PENDING, IndexStatus.FAILED)
        state.status = IndexStatus.FAILED
        state.error = error[:1000]


async def run_reindex(
    db: Database,
    embeddings: EmbeddingProvider,
    settings: Settings,
    scope: ReindexScope,
    *,
    batch_size: int = 100,
) -> ReindexReport:
    report = ReindexReport()
    async with db.engine.connect() as lock_conn:
        if scope.full:
            got = await lock_conn.scalar(
                text("SELECT pg_try_advisory_lock(:key)"), {"key": REINDEX_LOCK_KEY}
            )
            if not got:
                report.running_elsewhere = True
                return report
        try:
            async with db.sessionmaker() as session, session.begin():
                await mark_stale_models(session, settings.active_embedding_model)
            cursor: uuid.UUID | None = None
            while True:
                async with db.sessionmaker() as session:
                    query = select(IndexState.product_id).where(
                        IndexState.status != IndexStatus.REMOVED,
                        IndexState.source_document.is_not(None),
                    )
                    if scope.product_ids:
                        query = query.where(IndexState.product_id.in_(scope.product_ids))
                    if scope.merchant_id is not None:
                        query = query.where(IndexState.merchant_id == scope.merchant_id)
                    if cursor is not None:
                        query = query.where(IndexState.product_id > cursor)
                    batch = list(
                        (
                            await session.scalars(
                                query.order_by(IndexState.product_id).limit(batch_size)
                            )
                        ).all()
                    )
                if not batch:
                    break
                for product_id in batch:
                    await _reindex_one(db, embeddings, settings, product_id, report)
                cursor = batch[-1]
        finally:
            if scope.full:
                await lock_conn.execute(
                    text("SELECT pg_advisory_unlock(:key)"), {"key": REINDEX_LOCK_KEY}
                )
    logger.info(
        "reindex finished",
        extra={"indexed": report.indexed, "skipped": report.skipped, "failed": report.failed},
    )
    return report


async def _reindex_one(
    db: Database,
    embeddings: EmbeddingProvider,
    settings: Settings,
    product_id: uuid.UUID,
    report: ReindexReport,
) -> None:
    try:
        async with db.sessionmaker() as session, session.begin():
            state = await session.get(IndexState, product_id)
            if state is None or state.source_document is None:
                report.skipped += 1
                return
            payload = {**state.source_document, "version": state.catalog_version}
            result = await IndexingRepo(session, embeddings, settings).index_payload(payload)
    except DependencyUnavailable:
        raise  # provider down: stop here; a rerun resumes (idempotent)
    except Exception as exc:
        logger.exception("reindex failed", extra={"product_id": str(product_id)})
        report.failed += 1
        report.errors[str(product_id)] = str(exc)
        await _mark_failed(db, product_id, str(exc))
        return
    if result.outcome == IndexOutcome.INDEXED:
        report.indexed += 1
    else:
        report.skipped += 1


async def reindex_with_settings(settings: Settings, scope: ReindexScope) -> ReindexReport:
    db = create_database(settings)
    async with httpx.AsyncClient() as http:
        try:
            return await run_reindex(db, build_embeddings(settings, http), settings, scope)
        finally:
            await db.dispose()


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - CLI wiring
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--product-id", action="append", default=[], type=uuid.UUID)
    parser.add_argument("--merchant-id", type=uuid.UUID, default=None)
    args = parser.parse_args(argv)
    settings = get_settings()
    init_logger(settings)
    report = asyncio.run(
        reindex_with_settings(settings, ReindexScope(tuple(args.product_id), args.merchant_id))
    )
    if report.running_elsewhere:
        raise SystemExit("a full reindex is already running")
    print(f"indexed={report.indexed} skipped={report.skipped} failed={report.failed}")


if __name__ == "__main__":  # pragma: no cover
    main()
