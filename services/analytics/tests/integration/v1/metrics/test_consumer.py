"""The consumer end to end against a real replica set: Avro message →
``EventConsumer`` → ``MongoDedupeStore`` transaction → rollups (ANL-1, ANL-2,
ANL-G3, ANL-G4)."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from app.core.db import Mongo, require_replica_set
from app.core.errors import MongoUnavailable, TransactionsUnavailable
from app.v1.metrics.consumers import context, rollup_handler
from app.v1.metrics.consumers.dedupe import MongoDedupeStore
from app.v1.metrics.enums import Collection
from smartretail_common.events.consumer import Outcome
from tests import factories as f
from tests.integration.conftest import Source, deliver, message, snapshot


async def counter(mongo: Mongo, name: str) -> int | None:
    doc = await mongo.db.counters.find_one({"_id": name})
    return None if doc is None else int(doc["value"])


def scripted_day() -> list:
    a, b = f.uid(), f.uid()
    shoe, hat = f.uid(), f.uid()
    items = [f.item(shoe, a, 2, "30.00"), f.item(hat, b, 1, "15.00")]
    order = f.uid()
    return [
        f.envelope("identity.user.registered", f.user_registered("customer")),
        f.envelope("identity.user.registered", f.user_registered("merchant")),
        f.envelope("catalog.product.published", f.product_published(shoe, a)),
        f.envelope("order.created", f.order_created(items, order_id=order)),
        f.envelope("order.confirmed", f.order_confirmed(items, order_id=order, shipping="5")),
        f.envelope("order.failed", f.order_failed([a], "payment_declined")),
        f.envelope("payment.failed", f.payment_failed("card_declined")),
        f.envelope("inventory.stockout", f.stockout(f.uid(), shoe, a, "depleted")),
        f.envelope("assistant.interaction", f.assistant()),
        f.envelope("order.cancelled", {**_cancelled()}),
    ]


def _cancelled() -> dict:
    return {
        "order_id": f.uid(),
        "customer_id": f.uid(),
        "order_number": "SR-2",
        "cancelled_by": None,
        "reason": None,
        "stage": None,
        "payment_status": "none",
        "reservations_released": True,
        "items": [],
        "cancelled_at": "2026-10-15T00:00:00Z",
    }


async def test_one_event_moves_its_counter_and_is_archived(consumer, mongo) -> None:
    env = f.envelope("identity.user.registered", f.user_registered("customer"))
    assert await deliver(consumer, env) == [Outcome.HANDLED]
    assert await counter(mongo, "total_customers") == 1
    raw = await mongo.db.raw_events.find_one({"event_id": str(env.event_id)})
    assert raw["event_type"] == "identity.user.registered"
    assert raw["payload"]["role"] == "customer"
    assert raw["occurred_at"] == env.occurred_at.replace(microsecond=raw["occurred_at"].microsecond)
    processed = await mongo.db.processed_events.find_one()
    assert processed["_id"] == {"group": "analytics", "event_id": str(env.event_id)}
    assert processed["event_type"] == "identity.user.registered"


async def test_the_same_event_twice_moves_the_counter_once(consumer, mongo) -> None:
    """Crash after commit, before the offset commit: the redelivery is a duplicate."""
    env = f.envelope("catalog.product.published", f.product_published())
    assert await deliver(consumer, env, env) == [Outcome.HANDLED, Outcome.DUPLICATE]
    assert await counter(mongo, "total_products") == 1
    assert await mongo.db.raw_events.count_documents({}) == 1


async def test_full_replay_changes_nothing(consumer, mongo) -> None:
    """ANL-G3: re-consume everything; every document is unchanged."""
    events = scripted_day()
    assert set(await deliver(consumer, *events)) == {Outcome.HANDLED}
    before = await snapshot(mongo)
    assert before["daily_metrics"] and before["product_stats"] and before["failure_stats"]
    assert await deliver(consumer, *events) == [Outcome.DUPLICATE] * len(events)
    assert await snapshot(mongo) == before


async def test_a_fault_inside_the_transaction_rolls_everything_back(settings, mongo) -> None:
    """ANL-G4: a crash after the $inc, before commit, leaves no trace; the retry applies once."""
    store = MongoDedupeStore(mongo, processed_ttl_days=settings.processed_events_ttl_days)
    apply_ops = rollup_handler(mongo, context(settings))
    env = f.envelope("order.confirmed", f.order_confirmed([f.item(f.uid(), f.uid(), 1, "9")]))

    async def crashing(envelope, session) -> None:
        await apply_ops(envelope, session)
        raise RuntimeError("killed before commit")

    with pytest.raises(RuntimeError):
        await store.run("analytics", env, crashing)
    for name in Collection:
        assert await mongo.db[name].count_documents({}) == 0, name

    assert await store.run("analytics", env, apply_ops) == "handled"
    assert await counter(mongo, "orders_total") == 1


async def test_the_consumer_retries_a_failed_handler_then_applies_once(consumer, mongo) -> None:
    calls = []
    real = consumer.handlers["identity.user.registered"]

    async def flaky(envelope, session) -> None:
        calls.append(1)
        await real(envelope, session)
        if len(calls) == 1:
            raise RuntimeError("transient handler failure")

    consumer.handlers = {**consumer.handlers, "identity.user.registered": flaky}
    env = f.envelope("identity.user.registered", f.user_registered("merchant"))
    assert await deliver(consumer, env) == [Outcome.HANDLED]
    assert len(calls) == 2
    assert await counter(mongo, "total_merchants") == 1


async def test_two_replicas_racing_on_one_event_apply_it_once(settings, mongo) -> None:
    store = MongoDedupeStore(mongo, processed_ttl_days=settings.processed_events_ttl_days)
    handler = rollup_handler(mongo, context(settings))
    env = f.envelope("catalog.product.published", f.product_published())
    outcomes = await asyncio.gather(*(store.run("analytics", env, handler) for _ in range(2)))
    assert sorted(outcomes) == ["duplicate", "handled"]
    assert await counter(mongo, "total_products") == 1


async def test_an_event_older_than_the_dedupe_window_is_stale(consumer, mongo) -> None:
    """ANL-2: a compacted replay 40 days old never re-increments total_products."""
    old = f.envelope(
        "catalog.product.published",
        f.product_published(),
        occurred_at=datetime.now(UTC) - timedelta(days=40),
    )
    assert await deliver(consumer, old) == [Outcome.STALE]
    assert await counter(mongo, "total_products") is None
    assert await mongo.db.processed_events.count_documents({}) == 0


def test_stale_guard_boundary(settings) -> None:
    now = datetime(2026, 10, 15, 12, tzinfo=UTC)
    store = MongoDedupeStore(None, processed_ttl_days=30, clock=lambda: now)  # type: ignore[arg-type]
    edge = now - timedelta(days=29)
    assert not store.is_stale(f.envelope("x", {}, occurred_at=edge))
    assert store.is_stale(f.envelope("x", {}, occurred_at=edge - timedelta(microseconds=1)))
    assert not store.is_stale(f.envelope("x", {}, occurred_at=edge.replace(tzinfo=None)))


async def test_mongo_down_makes_the_consumer_wait_never_dead_letter(
    consumer, mongo, dead_mongo, dlq
) -> None:
    store = consumer.store
    assert isinstance(store, MongoDedupeStore)
    store.mongo = dead_mongo
    waits: list[float] = []

    async def recover(delay: float) -> None:
        waits.append(delay)
        if len(waits) == 2:
            store.mongo = mongo  # Mongo is back

    consumer.sleep = recover
    env = f.envelope("identity.user.registered", f.user_registered("customer"))
    assert await deliver(consumer, env) == [Outcome.HANDLED]
    assert waits == [0.5, 1.0]
    assert dlq.produced == []
    assert await counter(mongo, "total_customers") == 1


async def test_a_dead_store_raises_dependency_unavailable(settings, dead_mongo) -> None:
    store = MongoDedupeStore(dead_mongo, processed_ttl_days=30)
    with pytest.raises(MongoUnavailable):
        await store.run(
            "analytics", f.envelope("x", {}), rollup_handler(dead_mongo, context(settings))
        )


async def test_a_malformed_message_is_dead_lettered_and_the_next_is_processed(
    consumer, mongo, dlq
) -> None:
    good = f.envelope("identity.user.registered", f.user_registered("customer"))
    bad = await message(consumer, good)
    bad._value = b"\x00garbage"
    source = Source()
    assert await consumer.process(bad, source) == Outcome.DEAD_LETTERED
    assert [m.topic() for m in dlq.produced] == ["smartretail.identity.user.registered.dlq"]
    assert await deliver(consumer, good) == [Outcome.HANDLED]


async def test_a_type_without_a_formula_is_archived_only(consumer, mongo) -> None:
    env = f.envelope("shipment.status_changed", f.shipment("delivered"))
    assert await deliver(consumer, env) == [Outcome.HANDLED]
    assert await mongo.db.raw_events.count_documents({}) == 1
    assert await mongo.db.daily_metrics.count_documents({}) == 0


async def test_a_standalone_server_is_refused() -> None:
    class Admin:
        async def command(self, name: str) -> dict:
            return {"isWritablePrimary": True}  # no setName: stand-alone mongod

    class Client:
        admin = Admin()

    with pytest.raises(TransactionsUnavailable, match="replica set"):
        await require_replica_set(Mongo(client=Client(), db=None))  # type: ignore[arg-type]


async def test_the_raw_archive_keeps_the_first_copy(settings, mongo) -> None:
    """A second group writing the same event id never overwrites the archive."""
    env = f.envelope("identity.user.registered", f.user_registered("customer"))
    handler = rollup_handler(mongo, context(settings))
    for group in ("analytics", "analytics.other"):
        store = MongoDedupeStore(mongo, processed_ttl_days=30)
        assert await store.run(group, env, handler) == "handled"
    assert await mongo.db.raw_events.count_documents({"event_id": str(env.event_id)}) == 1
    assert uuid.UUID((await mongo.db.raw_events.find_one())["event_id"]) == env.event_id
