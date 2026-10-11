"""``/api/v1/inventory`` and ``/internal/v1/inventory/stock-items`` models."""

import uuid
from datetime import datetime
from typing import Annotated

from pydantic import Field, model_validator

from app.core.schemas import OutModel, StrictModel
from app.stock.enums import AdjustReason, StockMovementReason
from app.stock.models import StockItem
from smartretail_common.enums import widen_for_response


class PublicStock(OutModel):
    variant_id: uuid.UUID
    on_hand: int
    reserved: int
    available: int
    in_stock: bool

    @classmethod
    def from_item(cls, item: StockItem) -> "PublicStock":
        return cls(
            variant_id=item.variant_id,
            on_hand=item.on_hand,
            reserved=item.reserved,
            available=item.available,
            in_stock=item.available > 0,
        )


class StockItemOut(OutModel):
    variant_id: uuid.UUID
    product_id: uuid.UUID
    merchant_id: uuid.UUID
    sku: str
    on_hand: int
    reserved: int
    available: int
    low_stock_threshold: int
    version: int
    updated_at: datetime


class StockSet(StrictModel):
    on_hand: Annotated[int, Field(ge=0)]
    low_stock_threshold: Annotated[int, Field(ge=0)] | None = None
    reason_note: Annotated[str, Field(max_length=500)] | None = None


class StockAdjust(StrictModel):
    delta: int
    reason: AdjustReason
    reference_id: uuid.UUID | None = None

    @model_validator(mode="after")
    def _delta_rules(self) -> "StockAdjust":
        if self.delta == 0:
            raise ValueError("delta must not be zero")
        if self.reason in (AdjustReason.RESTOCK, AdjustReason.RETURN) and self.delta < 0:
            raise ValueError(f"{self.reason} requires a positive delta")
        return self


class MovementOut(OutModel):
    id: int
    delta_on_hand: int
    delta_reserved: int
    reason: Annotated[StockMovementReason | str, widen_for_response(StockMovementReason)]
    reference_type: str | None
    reference_id: uuid.UUID | None
    actor: str
    correlation_id: str | None
    created_at: datetime


class VariantIn(StrictModel):
    variant_id: uuid.UUID
    sku: Annotated[str, Field(min_length=1, max_length=64)]


class StockItemsInit(StrictModel):
    product_id: uuid.UUID
    merchant_id: uuid.UUID
    variants: Annotated[list[VariantIn], Field(max_length=500)]


class StockItemsOut(OutModel):
    items: list[StockItemOut]
