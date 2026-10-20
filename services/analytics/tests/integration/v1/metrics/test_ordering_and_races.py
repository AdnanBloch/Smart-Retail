"""Out-of-order and concurrent delivery per aggregate (ANL-1, ANL-G3).

Kafka orders events per partition only, and two consumer replicas can apply
different events for the same product or bucket at the same time. Every
rollup is ``$inc`` / ``$max``, so the end state must not depend on the
delivery order, and concurrent transactions on one document must not lose an
increment (a write conflict is a transient error that reruns the transaction).
The *same* event racing on two replicas is covered in ``test_consumer.py``.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pymongo.errors import DuplicateKeyError

from app.v1.metrics.consumers import build, context, rollup_handler
from app.v1.metrics.consumers.dedupe import MongoDedupeStore
from app.v1.metrics.utils import bucket_date, bucket_id, orders_metric, to_decimal
from smartretail_common.events.consumer import Outcome
from tests import factories as f
from tests.integration.conftest import Source, codec, deliver, message, no_sleep


def noon_today() -> datetime:
    # Whole seconds (Mongo keeps milliseconds) and far from a UTC day boundary.
    return datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)


def confirmed_orders(product: str, merchant: str, n: int) -> list:
    """``n`` confirmations of ``product``, the i-th for i+1 units at 10.00, a minute apart."""
    base = noon_today()
    return [
        f.envelope(
            "order.confirmed",
            f.order_confirmed([f.item(product, merchant, i + 1, "10.00")]),
            occurred_at=base + timedelta(minutes=i),
        )
        for i in range(n)
    ]


async def product_stats(mongo, product: str) -> dict:
    doc = await mongo.db.product_stats.find_one({"_id": product})
    assert doc is not None
    return doc


def as_utc(moment: datetime) -> datetime:
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


async def test_reversed_delivery_reaches_the_same_documents(consumer, mongo) -> None:
    """``last_sold_at`` is the latest sale, not the last one delivered."""
    product, merchant = f.uid(), f.uid()
    events = confirmed_orders(product, merchant, 3)
    assert await deliver(consumer, *reversed(events)) == [Outcome.HANDLED] * 3

    stats = await product_stats(mongo, product)
    assert stats["quantity_sold"] == 1 + 2 + 3
    assert stats["order_count"] == 3
    assert to_decimal(stats["revenue"]) == Decimal("60.00")
    assert as_utc(stats["last_sold_at"]) == events[-1].occurred_at

    day = bucket_date(events[0].occurred_at)
    for merchant_id in (None, merchant):
        bucket = await mongo.db.daily_metrics.find_one(
            {"_id": bucket_id(day, orders_metric("USD"), merchant_id)}
        )
        assert bucket["orders_confirmed"] == 3
        assert to_decimal(bucket["revenue"]) == Decimal("60.00")


async def test_unpublish_before_publish_nets_to_zero(consumer, mongo) -> None:
    """An unpublish delivered first drives the counter negative for a moment;
    the publish brings it back. Neither event is lost or clamped."""
    product, merchant = f.uid(), f.uid()
    unpublished = f.envelope(
        "catalog.product.unpublished", f.product_unpublished(product, merchant)
    )
    published = f.envelope("catalog.product.published", f.product_published(product, merchant))

    assert await deliver(consumer, unpublished) == [Outcome.HANDLED]
    assert (await mongo.db.counters.find_one({"_id": "total_products"}))["value"] == -1
    assert await deliver(consumer, published) == [Outcome.HANDLED]
    assert (await mongo.db.counters.find_one({"_id": "total_products"}))["value"] == 0


async def test_two_replicas_applying_different_events_for_one_product_lose_nothing(
    settings, mongo, dlq
) -> None:
    """Concurrent transactions ``$inc`` the same product, bucket and counter
    documents; a write conflict reruns the loser, so every increment lands once."""
    replicas = [build(settings, mongo, codec(), dlq) for _ in range(2)]
    for replica in replicas:
        replica.sleep = no_sleep
    product, merchant = f.uid(), f.uid()
    events = confirmed_orders(product, merchant, 8)
    messages = [await message(replicas[0], env) for env in events]

    async def process(i: int) -> Outcome:
        return await replicas[i % 2].process(messages[i], Source())

    outcomes = await asyncio.gather(*(process(i) for i in range(len(events))))
    assert outcomes == [Outcome.HANDLED] * len(events)
    assert dlq.produced == []

    stats = await product_stats(mongo, product)
    assert stats["quantity_sold"] == sum(range(1, 9))
    assert stats["order_count"] == 8
    assert to_decimal(stats["revenue"]) == Decimal("360.00")
    assert as_utc(stats["last_sold_at"]) == max(e.occurred_at for e in events)
    assert (await mongo.db.counters.find_one({"_id": "orders_total"}))["value"] == 8
    assert to_decimal(
        (await mongo.db.counters.find_one({"_id": "revenue_total"}))["value"]
    ) == Decimal("360.00")
    assert await mongo.db.processed_events.count_documents({}) == 8
    assert await mongo.db.raw_events.count_documents({}) == 8


async def test_a_duplicate_key_inside_a_projection_is_an_error_not_a_duplicate(
    settings, mongo
) -> None:
    """Only a duplicate ``processed_events`` id means "already applied". Any
    other duplicate key propagates, and the whole transaction — dedupe record
    included — rolls back, so a corrected retry still applies the event."""
    store = MongoDedupeStore(mongo, processed_ttl_days=settings.processed_events_ttl_days)
    env = f.envelope("identity.user.registered", f.user_registered("customer"))

    async def colliding(envelope, session) -> None:
        await mongo.db.counters.insert_one({"_id": "clash", "value": 1}, session=session)
        await mongo.db.counters.insert_one({"_id": "clash", "value": 2}, session=session)

    with pytest.raises(DuplicateKeyError):
        await store.run("analytics", env, colliding)
    assert await mongo.db.processed_events.count_documents({}) == 0
    assert await mongo.db.counters.count_documents({}) == 0

    handler = rollup_handler(mongo, context(settings))
    assert await store.run("analytics", env, handler) == Outcome.HANDLED.value
    assert (await mongo.db.counters.find_one({"_id": "total_customers"}))["value"] == 1
