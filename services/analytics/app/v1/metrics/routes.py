"""Every ``/api/v1/analytics/*`` route (RFC 0008 §3.1.2.1). Parse, validate,
call the repo, serialise — no metric formula lives here.

Role matrix (§3.1.2.2): ``overview`` and ``failures`` are admin-only; the four
scoped reads admit a merchant (own scope only) and an admin (all, or one
``merchant_id``). The gateway's coarse check is never trusted (A5).
"""

from datetime import date
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import StringConstraints

from app.core.dependencies import require_admin, require_analyst
from app.v1.metrics import schemas
from app.v1.metrics.dependencies import get_repo
from app.v1.metrics.enums import FailureGroupBy, Granularity, PopularRankBy
from app.v1.metrics.repo import MetricsRepo
from app.v1.metrics.scoping import resolve, resolve_window
from smartretail_common.auth import Principal

router = APIRouter()

FROM = Query(None, alias="from", description="UTC date, inclusive; default to - 30 days")
TO = Query(None, alias="to", description="UTC date, exclusive; default tomorrow")
MERCHANT = Query(None, description="admin only: narrow to one merchant")
Currency = Annotated[str, StringConstraints(pattern=r"^[A-Z]{3}$")]


@router.get("/overview", response_model=schemas.Overview)
async def overview(
    _: Principal = Depends(require_admin), repo: MetricsRepo = Depends(get_repo)
) -> schemas.Overview:
    return schemas.Overview(as_of=await repo.as_of(), **await repo.overview())


@router.get("/orders", response_model=schemas.OrdersSeries)
async def orders(
    principal: Principal = Depends(require_analyst),
    repo: MetricsRepo = Depends(get_repo),
    start: date | None = FROM,
    end: date | None = TO,
    granularity: Granularity = Granularity.DAY,
    merchant_id: UUID | None = MERCHANT,
) -> schemas.OrdersSeries:
    scope = resolve(principal, merchant_id)
    series = await repo.orders(scope, resolve_window(start, end), granularity)
    return schemas.OrdersSeries(
        as_of=await repo.as_of(),
        scope=schemas.ScopeOut(merchant_id=scope.merchant_id),
        granularity=granularity,
        series=[schemas.OrdersPoint(**p) for p in series],
    )


@router.get("/revenue", response_model=schemas.RevenueSeries)
async def revenue(
    principal: Principal = Depends(require_analyst),
    repo: MetricsRepo = Depends(get_repo),
    start: date | None = FROM,
    end: date | None = TO,
    granularity: Granularity = Granularity.DAY,
    merchant_id: UUID | None = MERCHANT,
    currency: Currency | None = Query(None, description="ISO 4217 filter"),
) -> schemas.RevenueSeries:
    scope = resolve(principal, merchant_id)
    series, totals = await repo.revenue(scope, resolve_window(start, end), granularity, currency)
    return schemas.RevenueSeries(
        as_of=await repo.as_of(),
        scope=schemas.ScopeOut(merchant_id=scope.merchant_id),
        granularity=granularity,
        # Platform revenue includes shipping; a merchant's is its line totals (ANL-D24).
        basis="order_total" if scope.is_platform else "merchant_line_total",
        series=[schemas.RevenuePoint(**p) for p in series],
        window=[schemas.RevenueTotals(**t) for t in totals],
    )


@router.get("/products/popular", response_model=schemas.PopularProducts)
async def popular_products(
    principal: Principal = Depends(require_analyst),
    repo: MetricsRepo = Depends(get_repo),
    rank_by: PopularRankBy = PopularRankBy.QUANTITY,
    limit: int = Query(10, ge=1, le=100),
    start: date | None = FROM,
    end: date | None = TO,
    merchant_id: UUID | None = MERCHANT,
) -> schemas.PopularProducts:
    scope = resolve(principal, merchant_id)
    window = resolve_window(start, end) if (start or end) else None
    items = await repo.popular(scope, rank_by, limit, window)
    return schemas.PopularProducts(
        as_of=await repo.as_of(),
        scope=schemas.ScopeOut(merchant_id=scope.merchant_id),
        rank_by=rank_by,
        window=(
            schemas.PopularWindow(start=window.start.isoformat(), end=window.end.isoformat())
            if window
            else None
        ),
        items=[schemas.PopularProduct(**i) for i in items],
    )


@router.get("/inventory/stockouts", response_model=schemas.Stockouts)
async def stockouts(
    principal: Principal = Depends(require_analyst),
    repo: MetricsRepo = Depends(get_repo),
    start: date | None = FROM,
    end: date | None = TO,
    granularity: Granularity = Granularity.DAY,
    limit: int = Query(10, ge=1, le=100),
    merchant_id: UUID | None = MERCHANT,
) -> schemas.Stockouts:
    scope = resolve(principal, merchant_id)
    result = await repo.stockouts(scope, resolve_window(start, end), granularity, limit)
    return schemas.Stockouts(
        as_of=await repo.as_of(),
        scope=schemas.ScopeOut(merchant_id=scope.merchant_id),
        granularity=granularity,
        **result,
    )


@router.get("/failures", response_model=schemas.Failures)
async def failures(
    _: Principal = Depends(require_admin),
    repo: MetricsRepo = Depends(get_repo),
    start: date | None = FROM,
    end: date | None = TO,
    group_by: FailureGroupBy | None = Query(None, description="default: source and reason"),
) -> schemas.Failures:
    items = await repo.failures(resolve_window(start, end), group_by)
    return schemas.Failures(
        as_of=await repo.as_of(), items=[schemas.FailureItem(**i) for i in items]
    )
