"""``/api/v1/inventory`` (public and merchant/admin) and the internal stock
initialisation route (RFC 0006 §3.1.2.1).

``router`` is mounted under ``/api/v1/inventory``; ``internal_router`` under
``/internal/v1/inventory``. Both prefixes are owned by ``app/router.py``.
"""

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, Header, Query

from app.core.dependencies import require_internal, require_stock_manager
from app.v1.stock.dependencies import stock_repo
from app.v1.stock.enums import StockMovementReason
from app.v1.stock.repo import MovementFilters, StockFilters, StockRepo, VariantRef
from app.v1.stock.schemas import (
    MovementOut,
    PublicStock,
    StockAdjust,
    StockItemOut,
    StockItemsInit,
    StockItemsOut,
    StockSet,
)
from smartretail_common.auth import Principal
from smartretail_common.idempotency import HEADER, canonical_key
from smartretail_common.pagination import Page, PageParams, page_params

router = APIRouter()
internal_router = APIRouter()


@router.get("", response_model=Page[StockItemOut])
async def list_stock(
    low_stock: bool = False,
    out_of_stock: bool = False,
    product_id: uuid.UUID | None = None,
    merchant_id: uuid.UUID | None = Query(None, description="Admins only; ignored for merchants"),
    page: PageParams = Depends(page_params),
    principal: Principal = Depends(require_stock_manager),
    repo: StockRepo = Depends(stock_repo),
) -> Page[StockItemOut]:
    filters = StockFilters(low_stock, out_of_stock, product_id, merchant_id)
    items = await repo.list(principal, filters, page)
    return items.to_page(StockItemOut.model_validate)


@router.get("/{variant_id}", response_model=PublicStock)
async def get_stock(variant_id: uuid.UUID, repo: StockRepo = Depends(stock_repo)) -> PublicStock:
    """Public: a product page needs it. ``in_stock = available > 0``."""
    return PublicStock.from_item(await repo.get(variant_id))


@router.put("/{variant_id}", response_model=StockItemOut)
async def set_stock(
    variant_id: uuid.UUID,
    body: StockSet,
    if_match: int | None = Header(None, alias="If-Match", description="The row's version"),
    principal: Principal = Depends(require_stock_manager),
    repo: StockRepo = Depends(stock_repo),
) -> StockItemOut:
    item = await repo.set_on_hand(
        principal,
        variant_id,
        on_hand=body.on_hand,
        low_stock_threshold=body.low_stock_threshold,
        if_match=if_match,
    )
    return StockItemOut.model_validate(item)


@router.post("/{variant_id}/adjust", response_model=StockItemOut)
async def adjust_stock(
    variant_id: uuid.UUID,
    body: StockAdjust,
    idempotency_key: str | None = Header(None, alias=HEADER),
    principal: Principal = Depends(require_stock_manager),
    repo: StockRepo = Depends(stock_repo),
) -> StockItemOut:
    item = await repo.adjust(
        principal,
        variant_id,
        delta=body.delta,
        reason=body.reason,
        idempotency_key=canonical_key(idempotency_key) if idempotency_key is not None else None,
        reference_id=body.reference_id,
    )
    return StockItemOut.model_validate(item)


@router.get("/{variant_id}/movements", response_model=Page[MovementOut])
async def stock_movements(
    variant_id: uuid.UUID,
    reason: StockMovementReason | None = None,
    since: datetime | None = Query(None, alias="from"),
    until: datetime | None = Query(None, alias="to"),
    page: PageParams = Depends(page_params),
    principal: Principal = Depends(require_stock_manager),
    repo: StockRepo = Depends(stock_repo),
) -> Page[MovementOut]:
    movements = await repo.movements(
        principal, variant_id, MovementFilters(reason, since, until), page
    )
    return movements.to_page(MovementOut.model_validate)


@internal_router.post("/stock-items", response_model=StockItemsOut)
async def initialise_stock_items(
    body: StockItemsInit,
    principal: Principal = Depends(require_internal),
    repo: StockRepo = Depends(stock_repo),
) -> StockItemsOut:
    """INV-4: called by the publishing workflow's ``sync_downstream``."""
    items = await repo.initialise(
        body.product_id,
        body.merchant_id,
        [VariantRef(v.variant_id, v.sku) for v in body.variants],
        actor=principal.subject,
    )
    return StockItemsOut(items=[StockItemOut.model_validate(i) for i in items])
