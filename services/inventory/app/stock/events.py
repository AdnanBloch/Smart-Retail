"""Outbox payloads for ``inventory.*`` (RFC 0006 §3.1.2.3, INV-7).

Aggregate is the stock item, keyed by ``variant_id`` (C3). ``client_id`` is
always null: inventory events come from workflows, schedulers and consumers.
"""

import uuid
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession

from app.stock.enums import InventoryEvent, StockoutTrigger
from app.stock.models import StockItem
from smartretail_common.outbox import write_outbox

if TYPE_CHECKING:
    from app.reservations.models import Reservation

AGGREGATE = "stock_item"


def _write(
    session: AsyncSession,
    event: InventoryEvent,
    item: StockItem,
    payload: dict[str, object],
    correlation_id: str | None,
) -> None:
    write_outbox(
        session,
        event_type=event.value,
        aggregate_type=AGGREGATE,
        aggregate_id=item.variant_id,
        payload=payload,
        correlation_id=correlation_id,
    )


def reserved(
    session: AsyncSession,
    item: StockItem,
    reservation: "Reservation",
    available_after: int,
    correlation_id: str | None = None,
) -> None:
    _write(
        session,
        InventoryEvent.RESERVED,
        item,
        {
            "reservation_id": reservation.id,
            "order_id": reservation.order_id,
            "variant_id": item.variant_id,
            "product_id": item.product_id,
            "merchant_id": item.merchant_id,
            "quantity": reservation.quantity,
            "available_after": available_after,
            "expires_at": reservation.expires_at,
        },
        correlation_id,
    )


def released(
    session: AsyncSession,
    item: StockItem,
    reservation: "Reservation",
    available_after: int,
    reason: str,
    correlation_id: str | None = None,
) -> None:
    _write(
        session,
        InventoryEvent.RELEASED,
        item,
        {
            "reservation_id": reservation.id,
            "order_id": reservation.order_id,
            "variant_id": item.variant_id,
            "product_id": item.product_id,
            "merchant_id": item.merchant_id,
            "quantity": reservation.quantity,
            "available_after": available_after,
            "reason": reason,
        },
        correlation_id,
    )


def stockout(
    session: AsyncSession,
    item: StockItem,
    trigger: StockoutTrigger,
    available: int,
    *,
    requested: int | None = None,
    order_id: uuid.UUID | None = None,
    correlation_id: str | None = None,
) -> None:
    payload: dict[str, object] = {
        "variant_id": item.variant_id,
        "product_id": item.product_id,
        "merchant_id": item.merchant_id,
        "sku": item.sku,
        "trigger": trigger.value,
        "available": available,
        "low_stock_threshold": item.low_stock_threshold,
    }
    if requested is not None:
        payload["requested"] = requested
        payload["order_id"] = order_id
    _write(session, InventoryEvent.STOCKOUT, item, payload, correlation_id)
