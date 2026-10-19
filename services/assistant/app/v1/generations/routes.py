"""``/api/v1/assistant/generate*``, ``/generations*``, ``/usage*`` (§3.1.2.1, §3.1.2.2)."""

import asyncio
import uuid
from typing import Any

from fastapi import APIRouter, Depends, Header, Query, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.celery_app import GENERATE_BULK
from app.core.clients.catalog import CatalogClient
from app.core.config import Settings
from app.core.db import get_db
from app.core.dependencies import (
    Principal,
    bearer,
    get_catalog,
    get_settings,
    require_admin,
    require_merchant,
    require_user,
)
from app.core.enums import ContentType
from app.core.errors import StreamCapReached
from app.core.sse import SSE_HEADERS, Emit, pump
from app.v1.generations.budget_repo import GLOBAL, BudgetRepo, Usage
from app.v1.generations.bulk_repo import BulkJobRepo, request_hash
from app.v1.generations.models import Generation
from app.v1.generations.repo import GenerationRepo, ProductSnapshot, owned_product, snapshot
from app.v1.generations.schemas import (
    AdminUsageOut,
    BulkAccepted,
    BulkGenerateIn,
    BulkJobOut,
    GenerateIn,
    GenerateSyncOut,
    GenerationOut,
    UsageOut,
)
from app.v1.generations.service import Generator
from smartretail_common.context import get_request_id
from smartretail_common.errors import Unprocessable
from smartretail_common.pagination import Page, PageParams, page_params

router = APIRouter()


def _out(row: Generation) -> GenerationOut:
    return GenerationOut(
        id=row.id,
        product_id=row.product_id,
        content_type=str(row.content_type),
        parameters=row.parameters,
        content=row.content,
        prompt_tokens=row.prompt_tokens,
        completion_tokens=row.completion_tokens,
        cost_usd=row.cost_usd,
        model=row.model,
        applied=row.applied,
        bulk_job_id=row.bulk_job_id,
        created_at=row.created_at,
    )


def _usage(u: Usage) -> UsageOut:
    return UsageOut(
        scope=u.scope,
        tokens_used=u.tokens_used,
        limit=u.limit,
        requests=u.requests,
        cost_usd=u.cost_usd,
        resets_at=u.resets_at,
    )


async def _prepare(
    request: Request,
    body: GenerateIn,
    principal: Principal,
    session: AsyncSession,
    catalog: CatalogClient,
    settings: Settings,
) -> ProductSnapshot:
    """Budget, then ownership (with the merchant's own token), then the snapshot."""
    merchant_id = principal.merchant_id or principal.subject
    await BudgetRepo(session, settings).check(f"user:{principal.subject}")
    product = await owned_product(catalog, body.product_id, merchant_id, bearer(request) or "")
    snap = await snapshot(
        session, request.app.state.embeddings, settings, product, body.content_type
    )
    await session.rollback()
    return snap


@router.post("/generate", response_model=None)
async def generate(
    request: Request,
    body: GenerateIn,
    principal: Principal = Depends(require_merchant),
    session: AsyncSession = Depends(get_db),
    catalog: CatalogClient = Depends(get_catalog),
    settings: Settings = Depends(get_settings),
) -> StreamingResponse:
    snap = await _prepare(request, body, principal, session, catalog, settings)
    state = request.app.state
    if state.active_streams >= settings.max_concurrent_streams:
        raise StreamCapReached(retry_after=1)
    state.active_streams += 1
    generator: Generator = state.generator
    params = body.params()
    correlation = get_request_id()
    instance = request.url.path

    async def produce(emit: Emit) -> None:
        await generator.run(snap, params, emit=emit, correlation_id=correlation, instance=instance)

    def release() -> None:
        state.active_streams -= 1

    return StreamingResponse(
        pump(
            produce,
            keepalive_seconds=settings.sse_keepalive_seconds,
            is_disconnected=request.is_disconnected,
            on_close=release,
        ),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


@router.post("/generate/sync", response_model=GenerateSyncOut)
async def generate_sync(
    request: Request,
    body: GenerateIn,
    principal: Principal = Depends(require_merchant),
    session: AsyncSession = Depends(get_db),
    catalog: CatalogClient = Depends(get_catalog),
    settings: Settings = Depends(get_settings),
) -> GenerateSyncOut:
    snap = await _prepare(request, body, principal, session, catalog, settings)
    generator: Generator = request.app.state.generator
    outcome = await generator.run(
        snap, body.params(), emit=None, correlation_id=get_request_id(), instance=request.url.path
    )
    return GenerateSyncOut(
        generation_id=outcome.generation_id,
        content_type=body.content_type,
        content=outcome.content,
        filtered=outcome.filtered,
        finish_reason=str(outcome.finish_reason),
        usage=outcome.usage,
    )


@router.post("/generate/bulk", status_code=status.HTTP_202_ACCEPTED, response_model=BulkAccepted)
async def generate_bulk(
    request: Request,
    body: BulkGenerateIn,
    idempotency_key: str | None = Header(None, alias="Idempotency-Key", max_length=200),
    principal: Principal = Depends(require_merchant),
    session: AsyncSession = Depends(get_db),
    catalog: CatalogClient = Depends(get_catalog),
    settings: Settings = Depends(get_settings),
) -> Any:
    if not idempotency_key or not idempotency_key.strip():
        raise Unprocessable(
            violations=[
                {"field": "Idempotency-Key", "message": "header is required", "code": "missing"}
            ]
        )
    product_ids = list(dict.fromkeys(body.product_ids))
    if len(product_ids) > settings.bulk_max_products:
        raise Unprocessable(
            violations=[
                {
                    "field": "product_ids",
                    "message": f"at most {settings.bulk_max_products} products",
                    "code": "too_many",
                }
            ]
        )
    merchant_id = principal.merchant_id or principal.subject
    key = idempotency_key.strip()
    digest = request_hash(body.model_dump(mode="json"))
    repo = BulkJobRepo(session)
    job = await repo.replay(merchant_id, key, digest)
    created = False
    if job is None:
        await BudgetRepo(session, settings).check(f"user:{principal.subject}")
        token = bearer(request) or ""
        semaphore = asyncio.Semaphore(settings.hydration_concurrency)

        async def owned(pid: uuid.UUID) -> dict[str, Any]:
            async with semaphore:
                return await owned_product(catalog, pid, merchant_id, token)

        products = await asyncio.gather(*(owned(p) for p in product_ids))
        snapshots = [
            await snapshot(session, request.app.state.embeddings, settings, p, body.content_type)
            for p in products
        ]
        await session.rollback()
        job, created = await repo.create(merchant_id, key, digest, body.params(), snapshots)
        if created:
            request.app.state.enqueue(GENERATE_BULK, str(job.id))
    location = f"/api/v1/assistant/generate/jobs/{job.id}"
    return JSONResponse(
        BulkAccepted(job_id=job.id, status=str(job.status), status_url=location).model_dump(
            mode="json"
        ),
        status_code=status.HTTP_202_ACCEPTED,
        headers={"Location": location},
    )


@router.get("/generate/jobs/{job_id}", response_model=BulkJobOut)
async def get_job(
    job_id: uuid.UUID,
    principal: Principal = Depends(require_merchant),
    session: AsyncSession = Depends(get_db),
) -> BulkJobOut:
    repo = BulkJobRepo(session)
    job = await repo.owned(job_id, principal.merchant_id or principal.subject)
    return BulkJobOut(
        id=job.id,
        status=str(job.status),
        content_type=str(job.content_type),
        completed=job.completed,
        failed=job.failed,
        total=len(job.product_ids),
        errors=job.item_errors or {},
        created_at=job.created_at,
        finished_at=job.finished_at,
        generations=[_out(g) for g in await repo.generations(job.id)],
    )


@router.post("/generate/jobs/{job_id}/cancel", status_code=status.HTTP_202_ACCEPTED)
async def cancel_job(
    job_id: uuid.UUID,
    principal: Principal = Depends(require_merchant),
    session: AsyncSession = Depends(get_db),
) -> dict[str, str]:
    job = await BulkJobRepo(session).cancel(job_id, principal.merchant_id or principal.subject)
    return {"job_id": str(job.id), "status": str(job.status)}


@router.get("/generations", response_model=Page[GenerationOut])
async def list_generations(
    product_id: uuid.UUID | None = Query(None),
    content_type: ContentType | None = Query(None),
    params: PageParams = Depends(page_params),
    principal: Principal = Depends(require_merchant),
    session: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> Page[GenerationOut]:
    page = await GenerationRepo(session, settings).list_for(
        principal.merchant_id or principal.subject,
        params,
        product_id=product_id,
        content_type=content_type,
    )
    return page.to_page(_out)


@router.post("/generations/{generation_id}/applied", status_code=status.HTTP_204_NO_CONTENT)
async def mark_applied(
    generation_id: uuid.UUID,
    principal: Principal = Depends(require_merchant),
    session: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> Response:
    await GenerationRepo(session, settings).mark_applied(
        generation_id, principal.merchant_id or principal.subject
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/usage", response_model=UsageOut)
async def usage(
    principal: Principal = Depends(require_user),
    session: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> UsageOut:
    return _usage(await BudgetRepo(session, settings).usage(f"user:{principal.subject}"))


@router.get("/usage/admin", response_model=AdminUsageOut)
async def usage_admin(
    _: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> AdminUsageOut:
    repo = BudgetRepo(session, settings)
    return AdminUsageOut(
        global_usage=_usage(await repo.usage(GLOBAL)),
        scopes=[_usage(u) for u in await repo.today() if u.scope != GLOBAL],
    )
