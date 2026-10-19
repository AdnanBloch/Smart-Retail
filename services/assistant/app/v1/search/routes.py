"""``/api/v1/search/*`` (RFC 0009 §3.1.2.1; role matrix §3.1.2.2)."""

import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Query, Request, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.celery_app import RUN_REINDEX
from app.core.config import Settings
from app.core.db import get_db
from app.core.dependencies import (
    Principal,
    get_settings,
    optional_principal,
    require_admin,
    require_merchant_or_admin,
)
from app.core.enums import CallerRole
from app.core.errors import NotProductOwner, ProductNotIndexed, ReindexRunning
from app.v1.search.dependencies import search_service
from app.v1.search.enums import IndexStatus
from app.v1.search.models import EmbeddingCacheEntry, IndexState, ProductChunk
from app.v1.search.reindex import ReindexScope, reindex_running
from app.v1.search.repo import SearchFilters
from app.v1.search.schemas import (
    ChunkOut,
    IndexDetailOut,
    IndexStatusOut,
    MerchantScope,
    ProductScope,
    ReindexAccepted,
    ReindexIn,
    SearchItem,
    SearchOut,
    SemanticSearchIn,
)
from app.v1.search.service import SearchService
from smartretail_common.errors import Unprocessable

router = APIRouter()


@router.post("/semantic", response_model=SearchOut)
async def semantic(
    body: SemanticSearchIn,
    service: SearchService = Depends(search_service),
    settings: Settings = Depends(get_settings),
    principal: Principal | None = Depends(optional_principal),
) -> SearchOut:
    query = body.query.strip()
    if not query or len(query) > settings.query_max_chars:
        raise Unprocessable(
            violations=[
                {
                    "field": "query",
                    "message": f"must be 1 to {settings.query_max_chars} characters",
                    "code": "length",
                }
            ]
        )
    limit = min(body.limit or settings.search_default_limit, settings.search_max_limit)
    response = await service.search(
        query,
        filters=body.filters.to_filters() if body.filters else SearchFilters(),
        mode=body.mode,
        limit=limit,
        user_id=principal.subject if principal else None,
    )
    return SearchOut(
        items=[SearchItem.from_result(r) for r in response.results],
        mode=response.mode,
        degraded=response.degraded,
    )


@router.get("/similar/{product_id}", response_model=SearchOut)
async def similar(
    product_id: uuid.UUID,
    limit: int = Query(10, ge=1),
    service: SearchService = Depends(search_service),
    settings: Settings = Depends(get_settings),
    _: Principal | None = Depends(optional_principal),
) -> SearchOut:
    results = await service.similar(product_id, min(limit, settings.search_max_limit))
    if results is None:
        raise ProductNotIndexed()
    return SearchOut(items=[SearchItem.from_result(r) for r in results], mode="dense")


@router.get("/index/status", response_model=IndexStatusOut)
async def index_status(
    session: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
    _: Principal = Depends(require_admin),
) -> IndexStatusOut:
    counts = dict(
        (await session.execute(select(IndexState.status, func.count()).group_by(IndexState.status)))
        .tuples()
        .all()
    )
    by_status = {str(s): int(counts.get(s, 0)) for s in IndexStatus}
    oldest_pending = await session.scalar(
        select(func.min(IndexState.updated_at)).where(IndexState.status == IndexStatus.PENDING)
    )
    failed = (
        await session.execute(
            select(IndexState.product_id, IndexState.error, IndexState.updated_at)
            .where(IndexState.status == IndexStatus.FAILED)
            .order_by(IndexState.updated_at.desc())
            .limit(1)
        )
    ).first()
    cache = (
        await session.execute(
            select(func.count(), func.coalesce(func.sum(EmbeddingCacheEntry.hit_count), 0))
        )
    ).one()
    return IndexStatusOut(
        embedding_model=settings.active_embedding_model,
        indexed_products=by_status[IndexStatus.INDEXED],
        chunk_count=int(await session.scalar(select(func.count()).select_from(ProductChunk)) or 0),
        products_by_status=by_status,
        pending_lag_seconds=(
            (datetime.now(UTC) - oldest_pending).total_seconds() if oldest_pending else None
        ),
        last_error=(
            {"product_id": str(failed[0]), "error": failed[1], "at": failed[2].isoformat()}
            if failed
            else None
        ),
        cache_entries=int(cache[0]),
        cache_hits=int(cache[1]),
        reindex_running=await reindex_running(session),
    )


@router.post("/index/reindex", status_code=status.HTTP_202_ACCEPTED, response_model=ReindexAccepted)
async def reindex(
    request: Request,
    body: ReindexIn | None = None,
    session: AsyncSession = Depends(get_db),
    _: Principal = Depends(require_admin),
) -> ReindexAccepted:
    raw = body.scope if body is not None else "full"
    if isinstance(raw, ProductScope):
        scope = ReindexScope(product_ids=tuple(raw.product_ids))
    elif isinstance(raw, MerchantScope):
        scope = ReindexScope(merchant_id=raw.merchant_id)
    else:
        scope = ReindexScope()
    if scope.full and await reindex_running(session):
        raise ReindexRunning()
    job_id = uuid.uuid4()
    request.app.state.enqueue(RUN_REINDEX, {**scope.as_dict(), "job_id": str(job_id)})
    return ReindexAccepted(job_id=job_id, status_url="/api/v1/search/index/status")


@router.get("/index/{product_id}", response_model=IndexDetailOut)
async def index_detail(
    product_id: uuid.UUID,
    session: AsyncSession = Depends(get_db),
    principal: Principal = Depends(require_merchant_or_admin),
) -> IndexDetailOut:
    state = await session.get(IndexState, product_id)
    if state is None:
        raise ProductNotIndexed()
    if principal.role == CallerRole.MERCHANT and (
        state.merchant_id is None or str(state.merchant_id) != principal.merchant_id
    ):
        raise NotProductOwner()
    chunks = (
        await session.scalars(
            select(ProductChunk)
            .where(ProductChunk.product_id == product_id)
            .order_by(ProductChunk.chunk_index)
        )
    ).all()
    return IndexDetailOut(
        product_id=state.product_id,
        merchant_id=state.merchant_id,
        status=str(state.status),
        catalog_version=state.catalog_version,
        chunk_count=state.chunk_count,
        embedding_model=state.embedding_model,
        error=state.error,
        indexed_at=state.indexed_at,
        updated_at=state.updated_at,
        chunks=[
            ChunkOut(
                id=c.id,
                chunk_index=c.chunk_index,
                chunk_type=str(c.chunk_type),
                content=c.content,
                content_hash=c.content_hash,
                token_count=c.token_count,
                indexed_at=c.indexed_at,
            )
            for c in chunks
        ],
    )
