"""Outbox payloads for ``order.*``, ``payment.*`` and ``shipment.status_changed``
(RFC 0005 §3.1.2.3, ORD-17).

Aggregate and Kafka key are the order id, so every event of one order shares
a partition and arrives in order (C3). ``client_id`` comes from the persisted
``origin_client_id`` (C21) — null for carrier-driven events. Identifiers and
the minimum needed to render and aggregate: never card data.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.v1.orders.enums import OrderEvent
from app.v1.orders.models import Order, OrderItem
from smartretail_common.outbox import OutboxEvent, write_outbox

AGGREGATE = "order"


def _items(items: list[OrderItem], *, names: bool) -> list[dict[str, Any]]:
    return [
        {
            "product_id": i.product_id,
            "variant_id": i.variant_id,
            "merchant_id": i.merchant_id,
            **({"product_name": i.product_name} if names else {}),
            "sku": i.sku,
            "quantity": i.quantity,
            "unit_price": i.unit_price,
            "line_total": i.line_total,
        }
        for i in items
    ]


async def created_event_id(session: AsyncSession, order_id: uuid.UUID) -> str | None:
    """The causation of every later event of the order."""
    event_id = await session.scalar(
        select(OutboxEvent.event_id).where(
            OutboxEvent.aggregate_id == order_id,
            OutboxEvent.event_type == OrderEvent.CREATED.value,
        )
    )
    return str(event_id) if event_id else None


def write(
    session: AsyncSession,
    event: OrderEvent,
    order: Order,
    payload: dict[str, Any],
    *,
    correlation_id: str | None,
    causation_id: str | None = None,
    client_id: str | None = None,
) -> uuid.UUID:
    return write_outbox(
        session,
        event_type=event.value,
        aggregate_type=AGGREGATE,
        aggregate_id=order.id,
        payload=payload,
        correlation_id=correlation_id,
        causation_id=causation_id,
        client_id=client_id,
    )


def created(
    session: AsyncSession, order: Order, items: list[OrderItem], correlation_id: str | None
) -> None:
    write(
        session,
        OrderEvent.CREATED,
        order,
        {
            "order_id": order.id,
            "order_number": order.order_number,
            "customer_id": order.customer_id,
            "status": str(order.status),
            "currency": order.currency,
            "subtotal": order.subtotal,
            "shipping_fee": order.shipping_fee,
            "total": order.total,
            "item_count": sum(i.quantity for i in items),
            "items": _items(items, names=False),
            "placed_at": order.placed_at,
        },
        correlation_id=correlation_id,
        client_id=order.origin_client_id,
    )


def payloads(order: Order, event: OrderEvent, context: dict[str, Any]) -> dict[str, Any]:
    """The payload of every event written by a state change (``OrderRepo.advance``)."""
    now = datetime.now(UTC)
    base = {"order_id": order.id, "customer_id": order.customer_id}
    if event == OrderEvent.CONFIRMED:
        return {
            **base,
            "order_number": order.order_number,
            "currency": order.currency,
            "subtotal": order.subtotal,
            "shipping_fee": order.shipping_fee,
            "total": order.total,
            "items": _items(order.items, names=True),
            "payment_reference": order.payment_reference,
            "shipment_reference": order.shipment_reference,
            "tracking_number": order.tracking_number,
            "placed_at": order.placed_at,
            "confirmed_at": order.confirmed_at,
        }
    if event == OrderEvent.FAILED:
        return {
            **base,
            "order_number": order.order_number,
            "failure_reason": order.failure_reason,
            "failed_step": context.get("stage"),
            "payment_status": str(order.payment_status),
            "currency": order.currency,
            "total": order.total,
            "merchant_ids": sorted({str(i.merchant_id) for i in order.items}),
            "failed_at": now,
        }
    if event == OrderEvent.CANCELLED:
        return {
            **base,
            "order_number": order.order_number,
            "cancelled_by": context.get("actor"),
            "reason": context.get("reason"),
            "stage": context.get("stage"),
            "payment_status": str(order.payment_status),
            "reservations_released": True,
            "items": [{"variant_id": i.variant_id, "quantity": i.quantity} for i in order.items],
            "cancelled_at": now,
        }
    if event == OrderEvent.PAYMENT_SUCCEEDED:
        return {
            **base,
            "payment_reference": order.payment_reference,
            "amount": order.total,
            "currency": order.currency,
            "succeeded_at": now,
        }
    if event == OrderEvent.PAYMENT_FAILED:
        return {
            **base,
            "reason": context.get("failure_reason"),
            "amount": order.total,
            "currency": order.currency,
            "failed_at": now,
        }
    assert event == OrderEvent.SHIPMENT_STATUS_CHANGED
    return {
        **base,
        "order_number": order.order_number,
        "from_status": context.get("from_shipment"),
        "to_status": str(order.shipment_status),
        "shipment_reference": order.shipment_reference,
        "tracking_number": order.tracking_number,
        "carrier_event_id": context.get("carrier_event_id"),
        "occurred_at": now,
    }
