"""The inventory consumer end to end against its database: Avro message →
``EventConsumer`` (dedupe, transaction, offset commit) → the entity handlers
(RFC 0006 §3.1.2.4, RFC 0001 §3.1.1.1)."""

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import func, select

from app.core.consumer import build
from app.v1.reservations.models import Reservation
from app.v1.stock.models import StockItem, StockMovement
from smartretail_common.events import registry
from smartretail_common.events.consumer import EventConsumer, Outcome
from smartretail_common.events.dedupe import ProcessedEvent
from smartretail_common.events.envelope import Envelope
from smartretail_common.events.schema_registry import InMemorySchemaRegistry
from smartretail_common.events.serde import AvroCodec
from smartretail_common.outbox import OutboxEvent
from tests.integration.conftest import init_stock, reserve

PUBLISHED = "catalog.product.published"
CANCELLED = "order.cancelled"


@dataclass
class Msg:
    _topic: str
    _value: bytes

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 0

    def key(self) -> bytes:
        return b"k"

    def value(self) -> bytes:
        return self._value

    def headers(self) -> list[tuple[str, bytes]]:
        return []

    def error(self) -> None:
        return None


@dataclass
class Source:
    committed: list[Msg] = field(default_factory=list)

    def poll(self, timeout: float) -> None:
        return None

    def commit(self, message: Msg) -> None:
        self.committed.append(message)

    def close(self) -> None:
        return None


class NoDlq:
    def produce(self, *args: Any, **kwargs: Any) -> None:
        raise AssertionError("nothing should be dead-lettered")

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


@pytest.fixture
async def consumer(app, settings) -> EventConsumer:  # `app` truncates tables afterwards
    schemas = InMemorySchemaRegistry()
    for event_type in (PUBLISHED, CANCELLED):
        spec = registry.catalogue().spec(event_type)
        schemas.register_now(spec.subject, registry.message_schema_str(event_type))
    built = build(settings, AvroCodec(schemas), NoDlq())
    yield built


async def deliver(consumer: EventConsumer, event_type: str, payload: dict, event_id=None):
    envelope = Envelope(
        event_id=event_id or uuid.uuid4(),
        event_type=event_type,
        event_version=1,
        occurred_at=datetime.now(UTC),
        producer="test",
        correlation_id="corr-1",
        payload=payload,
    )
    msg = Msg(registry.catalogue().topic(event_type), await consumer.codec.encode(envelope))
    source = Source()
    outcome = await consumer.process(msg, source)
    assert source.committed == [msg]  # the offset is committed in every outcome
    return outcome, envelope.event_id


def published_payload(product_id, merchant_id, variants) -> dict:
    return {
        "product_id": str(product_id),
        "merchant_id": str(merchant_id),
        "version": 1,
        "sku": "P",
        "name": "Product",
        "status": "published",
        "category_id": None,
        "base_price": "10.00",
        "currency": "USD",
        "published_at": None,
        "variants": [
            {
                "variant_id": str(v),
                "sku": f"SKU-{i}",
                "options": {},
                "price_override": None,
                "is_active": active,
            }
            for i, (v, active) in enumerate(variants)
        ],
        "search_document": None,
        "changed_fields": [],
    }


def cancelled_payload(order_id) -> dict:
    return {
        "order_id": str(order_id),
        "customer_id": str(uuid.uuid4()),
        "order_number": "SR-1",
        "cancelled_by": "customer",
        "reason": None,
        "stage": "awaiting_payment",
        "payment_status": "none",
        "reservations_released": True,
        "items": [],
        "cancelled_at": "2026-10-14T00:00:00Z",
    }


async def count(app, model, *where) -> int:
    async with app.state.db.sessionmaker() as session:
        return int(await session.scalar(select(func.count()).select_from(model).where(*where)) or 0)


async def test_product_published_creates_stock_for_active_variants_once(app, consumer) -> None:
    product, merchant = uuid.uuid4(), uuid.uuid4()
    active, inactive = uuid.uuid4(), uuid.uuid4()
    payload = published_payload(product, merchant, [(active, True), (inactive, False)])

    outcome, event_id = await deliver(consumer, PUBLISHED, payload)
    assert outcome == Outcome.HANDLED
    assert await count(app, StockItem, StockItem.product_id == product) == 1
    assert await count(app, StockMovement, StockMovement.variant_id == active) == 1

    # Redelivery (relay restart, consumer restart): skipped by processed_events.
    again, _ = await deliver(consumer, PUBLISHED, payload, event_id=event_id)
    assert again == Outcome.DUPLICATE
    # A *new* event for the same product (a replay past dedupe retention on the
    # compacted topic): idempotent by state, existing rows untouched.
    fresh, _ = await deliver(consumer, PUBLISHED, payload)
    assert fresh == Outcome.HANDLED
    assert await count(app, StockItem, StockItem.product_id == product) == 1
    assert await count(app, StockMovement, StockMovement.variant_id == active) == 1
    assert await count(app, ProcessedEvent, ProcessedEvent.consumer_group == "inventory") == 2


async def test_existing_stock_from_the_workflow_is_left_alone(
    app, client, callers, consumer
) -> None:
    merchant = str(uuid.uuid4())
    [variant] = await init_stock(client, callers, merchant, on_hand=7)
    async with app.state.db.sessionmaker() as session:
        product = await session.scalar(
            select(StockItem.product_id).where(StockItem.variant_id == uuid.UUID(variant))
        )
    payload = published_payload(product, merchant, [(variant, True)])
    assert (await deliver(consumer, PUBLISHED, payload))[0] == Outcome.HANDLED
    async with app.state.db.sessionmaker() as session:
        item = await session.scalar(
            select(StockItem).where(StockItem.variant_id == uuid.UUID(variant))
        )
    assert item is not None and item.on_hand == 7


async def test_order_cancelled_releases_held_reservations_once(
    app, client, callers, consumer
) -> None:
    merchant = str(uuid.uuid4())
    v1, v2 = await init_stock(client, callers, merchant, variants=2, on_hand=10)
    order_id = uuid.uuid4()
    r = await reserve(client, callers, str(order_id), [(v1, 2), (v2, 3)])
    assert r.status_code in (200, 201), r.text

    outcome, event_id = await deliver(consumer, CANCELLED, cancelled_payload(order_id))
    assert outcome == Outcome.HANDLED
    async with app.state.db.sessionmaker() as session:
        statuses = set(
            await session.scalars(
                select(Reservation.status).where(Reservation.order_id == order_id)
            )
        )
        reserved = list(await session.scalars(select(StockItem.reserved)))
    assert statuses == {"released"}
    assert reserved == [0, 0]
    assert await count(app, OutboxEvent, OutboxEvent.event_type == "inventory.released") == 2

    again, _ = await deliver(consumer, CANCELLED, cancelled_payload(order_id), event_id=event_id)
    assert again == Outcome.DUPLICATE
    assert await count(app, OutboxEvent, OutboxEvent.event_type == "inventory.released") == 2


async def test_order_cancelled_after_the_saga_released_is_a_no_op(
    app, client, callers, consumer
) -> None:
    order_id = uuid.uuid4()  # an order with no reservations left to release
    outcome, _ = await deliver(consumer, CANCELLED, cancelled_payload(order_id))
    assert outcome == Outcome.HANDLED
    assert await count(app, OutboxEvent) == 0


async def test_committed_reservations_are_not_released(app, client, callers, consumer) -> None:
    merchant = str(uuid.uuid4())
    [v1] = await init_stock(client, callers, merchant, on_hand=5)
    order_id = uuid.uuid4()
    r = await reserve(client, callers, str(order_id), [(v1, 1)])
    reservation_id = r.json()["reservations"][0]["id"]
    r = await client.post(
        f"/internal/v1/inventory/reservations/{reservation_id}/commit", headers=callers.service
    )
    assert r.status_code == 200, r.text

    assert (await deliver(consumer, CANCELLED, cancelled_payload(order_id)))[0] == Outcome.HANDLED
    async with app.state.db.sessionmaker() as session:
        status = await session.scalar(
            select(Reservation.status).where(Reservation.order_id == order_id)
        )
    assert status == "committed"
