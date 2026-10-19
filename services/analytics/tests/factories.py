"""Envelopes and payloads shaped exactly like the producers' (each factory's
payload validates against the registered Avro schema — see test_contract)."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from smartretail_common.events.envelope import Envelope


def uid() -> str:
    return str(uuid.uuid4())


def envelope(
    event_type: str,
    payload: dict[str, Any],
    *,
    occurred_at: datetime | None = None,
    event_id: uuid.UUID | None = None,
) -> Envelope:
    return Envelope(
        event_id=event_id or uuid.uuid4(),
        event_type=event_type,
        event_version=1,
        occurred_at=occurred_at or datetime.now(UTC),
        producer="test",
        correlation_id="corr-1",
        payload=payload,
    )


def user_registered(role: str = "customer") -> dict[str, Any]:
    return {
        "user_id": uid(),
        "role": role,
        "full_name": "Test User",
        "registered_at": "2026-10-15T00:00:00Z",
        "merchant_status": "pending" if role == "merchant" else None,
    }


def product_published(product_id: str | None = None, merchant_id: str | None = None) -> dict:
    return {
        "product_id": product_id or uid(),
        "merchant_id": merchant_id or uid(),
        "version": 1,
        "sku": "P-1",
        "name": "Product",
        "status": "published",
        "category_id": None,
        "base_price": "10.00",
        "currency": "USD",
        "published_at": None,
        "variants": [],
        "search_document": None,
        "changed_fields": [],
    }


def product_unpublished(product_id: str | None = None, merchant_id: str | None = None) -> dict:
    return {
        "product_id": product_id or uid(),
        "merchant_id": merchant_id or uid(),
        "version": 2,
        "previous_status": "published",
        "new_status": "inactive",
        "reason": "merchant_unpublished",
        "variant_ids": [],
        "unpublished_at": "2026-10-15T00:00:00Z",
    }


def item(
    product_id: str,
    merchant_id: str,
    quantity: int,
    unit_price: str,
    *,
    variant_id: str | None = None,
    name: str | None = "Product",
) -> dict[str, Any]:
    return {
        "product_id": product_id,
        "variant_id": variant_id or uid(),
        "merchant_id": merchant_id,
        "product_name": name,
        "sku": "SKU",
        "quantity": quantity,
        "unit_price": unit_price,
        "line_total": str(Decimal(unit_price) * quantity),
    }


def _totals(items: list[dict[str, Any]], shipping: str) -> dict[str, str]:
    subtotal = sum((Decimal(i["line_total"]) for i in items), Decimal(0))
    return {
        "subtotal": str(subtotal),
        "shipping_fee": shipping,
        "total": str(subtotal + Decimal(shipping)),
    }


def order_created(
    items: list[dict[str, Any]],
    *,
    order_id: str | None = None,
    currency: str = "USD",
    shipping: str = "0.00",
) -> dict[str, Any]:
    return {
        "order_id": order_id or uid(),
        "order_number": "SR-1",
        "customer_id": uid(),
        "status": "pending",
        "currency": currency,
        **_totals(items, shipping),
        "item_count": len(items),
        "items": items,
        "placed_at": "2026-10-15T00:00:00Z",
    }


def order_confirmed(
    items: list[dict[str, Any]],
    *,
    order_id: str | None = None,
    currency: str = "USD",
    shipping: str = "0.00",
) -> dict[str, Any]:
    return {
        "order_id": order_id or uid(),
        "customer_id": uid(),
        "order_number": "SR-1",
        "currency": currency,
        **_totals(items, shipping),
        "items": items,
        "payment_reference": None,
        "shipment_reference": None,
        "tracking_number": None,
        "placed_at": "2026-10-15T00:00:00Z",
        "confirmed_at": None,
    }


def order_failed(
    merchant_ids: list[str], reason: str | None = "payment_declined", currency: str = "USD"
) -> dict[str, Any]:
    return {
        "order_id": uid(),
        "customer_id": uid(),
        "order_number": "SR-1",
        "failure_reason": reason,
        "failed_step": "payment",
        "payment_status": "failed",
        "currency": currency,
        "total": "10.00",
        "merchant_ids": merchant_ids,
        "failed_at": "2026-10-15T00:00:00Z",
    }


def payment_failed(reason: str | None = "card_declined") -> dict[str, Any]:
    return {
        "order_id": uid(),
        "customer_id": uid(),
        "reason": reason,
        "amount": "10.00",
        "currency": "USD",
        "failed_at": "2026-10-15T00:00:00Z",
    }


def stockout(
    variant_id: str, product_id: str, merchant_id: str, trigger: str = "depleted"
) -> dict[str, Any]:
    return {
        "variant_id": variant_id,
        "product_id": product_id,
        "merchant_id": merchant_id,
        "sku": "SKU",
        "trigger": trigger,
        "available": 0,
        "low_stock_threshold": 5,
        "requested": None,
        "order_id": None,
    }


def assistant(
    kind: str = "chat",
    *,
    answered: bool = True,
    finish_reason: str = "stop",
    merchant_id: str | None = None,
    user_id: str | None = None,
    tokens: tuple[int, int] = (100, 50),
    cost: str = "0.0015",
    latency_ms: int = 1200,
) -> dict[str, Any]:
    return {
        "interaction_id": uid(),
        "kind": kind,
        "conversation_id": uid() if kind == "chat" else None,
        "generation_id": uid() if kind == "generation" else None,
        "user_id": user_id,
        "merchant_id": merchant_id,
        "intent": None,
        "content_type": None,
        "answered": answered,
        "finish_reason": finish_reason,
        "source_product_ids": [],
        "model": "test-model",
        "input_tokens": tokens[0],
        "output_tokens": tokens[1],
        "cost_usd": cost,
        "latency_ms": latency_ms,
        "time_to_first_token_ms": None,
        "occurred_at": "2026-10-15T00:00:00Z",
    }


def shipment(to_status: str = "shipped") -> dict[str, Any]:
    return {
        "order_id": uid(),
        "customer_id": uid(),
        "order_number": "SR-1",
        "from_status": None,
        "to_status": to_status,
        "shipment_reference": None,
        "tracking_number": None,
        "carrier_event_id": None,
        "occurred_at": "2026-10-15T00:00:00Z",
    }
