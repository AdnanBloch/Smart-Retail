"""The notification consumer end to end against its database and Redis: Avro
message → ``EventConsumer`` (dedupe, transaction, offset commit) → recording →
after-commit fan-out and enqueue (RFC 0007 §3.1.2.4, G3, G5)."""

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from typing import Any

import pytest
from sqlalchemy import func, select, text

from app.core.consumer import GROUP, build
from app.v1.notifications.consumers.after_commit import after_commit
from app.v1.notifications.models import Notification
from smartretail_common.events import registry
from smartretail_common.events.consumer import EventConsumer, Outcome
from smartretail_common.events.dedupe import ProcessedEvent
from smartretail_common.events.schema_registry import InMemorySchemaRegistry
from smartretail_common.events.serde import AvroCodec
from smartretail_common.redis import create_redis
from tests.integration.conftest import (
    cancelled,
    confirmed,
    envelope,
    failed,
    payment_failed,
    payment_succeeded,
    registered,
    shipment,
    stockout,
)

CONSUMED = (
    "identity.user.registered",
    "order.confirmed",
    "order.failed",
    "order.cancelled",
    "shipment.status_changed",
    "inventory.stockout",
    "payment.succeeded",
    "payment.failed",
)


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


@dataclass
class Dlq:
    produced: list[dict[str, Any]] = field(default_factory=list)

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.produced.append({"topic": topic, **kwargs})

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


@dataclass
class Enqueued:
    ids: list[uuid.UUID] = field(default_factory=list)
    fail: bool = False

    async def __call__(self, notification_id: uuid.UUID) -> None:
        if self.fail:
            raise ConnectionError("rabbitmq down")
        self.ids.append(notification_id)


@pytest.fixture
def enqueued() -> Enqueued:
    return Enqueued()


@pytest.fixture
def dlq() -> Dlq:
    return Dlq()


@pytest.fixture
async def consumer(app, settings, enqueued, dlq):  # `app` truncates tables afterwards
    schemas = InMemorySchemaRegistry()
    for event_type in CONSUMED:
        spec = registry.catalogue().spec(event_type)
        schemas.register_now(spec.subject, registry.message_schema_str(event_type))
    redis = create_redis(settings.redis_url)
    built = build(settings, AvroCodec(schemas), dlq, redis=redis, enqueue=enqueued)
    built.sleep = _no_sleep
    yield built
    await redis.aclose()


async def _no_sleep(_: float) -> None:
    return None


async def deliver(consumer: EventConsumer, env) -> Outcome:
    msg = Msg(registry.catalogue().topic(env.event_type), await consumer.codec.encode(env))
    source = Source()
    outcome = await consumer.process(msg, source)
    assert source.committed == [msg]  # the offset is committed in every outcome
    return outcome


async def rows(app, *where) -> list[Notification]:
    async with app.state.db.sessionmaker() as session:
        return list(
            await session.scalars(select(Notification).where(*where).order_by(Notification.seq))
        )


async def count(app, model, *where) -> int:
    async with app.state.db.sessionmaker() as session:
        return int(await session.scalar(select(func.count()).select_from(model).where(*where)) or 0)


CASES = [
    ("identity.user.registered", registered, "welcome", 2, "Welcome to SmartRetail"),
    ("order.confirmed", confirmed, "order_confirmed", 2, "Order confirmed"),
    ("order.failed", failed, "order_failed", 2, "Order could not be completed"),
    ("order.cancelled", cancelled, "order_cancelled", 2, "Order cancelled"),
    (
        "shipment.status_changed",
        lambda u: shipment(u, "delivered"),
        "shipment_update",
        2,
        "Your order has been delivered",
    ),
    ("inventory.stockout", stockout, "stockout", 1, "Out of stock"),
]


@pytest.mark.parametrize(("event_type", "payload", "kind", "channels", "title"), CASES)
async def test_each_event_type_records_its_notification(
    app, consumer, enqueued, event_type, payload, kind, channels, title
) -> None:
    user = str(uuid.uuid4())
    env = envelope(event_type, payload(user), client_id="client-origin-1")
    assert await deliver(consumer, env) == Outcome.HANDLED

    recorded = await rows(app, Notification.user_id == uuid.UUID(user))
    assert len(recorded) == channels
    assert (
        {str(r.channel) for r in recorded} == {"in_app", "email"} if channels == 2 else {"in_app"}
    )
    in_app = next(r for r in recorded if r.channel == "in_app")
    assert in_app.type == kind and in_app.title == title
    assert in_app.payload["event_id"] == str(env.event_id)
    assert in_app.payload["payload"] == json.loads(json.dumps(env.payload))
    assert in_app.correlation_id == "corr-1"
    assert in_app.origin_client_id == "client-origin-1"
    assert in_app.read_at is None and in_app.delivered_at is None
    # email rows are enqueued for dispatch after commit; in_app never is (NTF-4).
    assert enqueued.ids == [r.id for r in recorded if r.channel == "email"]


async def test_scope_is_derived_and_stored(app, consumer) -> None:
    user = str(uuid.uuid4())
    await deliver(consumer, envelope("order.failed", failed(user), client_id="client-origin-1"))
    await deliver(consumer, envelope("identity.user.registered", registered(user)))
    scopes = {
        (str(r.type), str(r.delivery_scope))
        for r in await rows(app, Notification.user_id == uuid.UUID(user))
    }
    # origin present → the type's default; absent → all_devices (I2).
    assert scopes == {("order_failed", "origin_first"), ("welcome", "all_devices")}


async def test_a_malformed_client_id_is_treated_as_absent(app, consumer) -> None:
    user = str(uuid.uuid4())
    await deliver(consumer, envelope("order.confirmed", confirmed(user), client_id="bad id!"))
    [row, _] = await rows(app, Notification.user_id == uuid.UUID(user))
    assert row.origin_client_id is None and row.delivery_scope == "all_devices"


@pytest.mark.parametrize(
    ("event_type", "payload"),
    [
        ("payment.succeeded", payment_succeeded),
        ("payment.failed", payment_failed),
        ("shipment.status_changed", lambda u: shipment(u, "created")),
    ],
)
async def test_events_that_notify_nobody_are_still_deduped(
    app, consumer, event_type, payload
) -> None:
    """NTF-7: consumed and recorded in processed_events, but no notification."""
    env = envelope(event_type, payload(str(uuid.uuid4())))
    assert await deliver(consumer, env) == Outcome.HANDLED
    assert await count(app, Notification) == 0
    assert await count(app, ProcessedEvent, ProcessedEvent.event_id == env.event_id) == 1
    assert await deliver(consumer, env) == Outcome.DUPLICATE


async def test_duplicate_delivery_and_replay_past_the_dedupe_table(app, consumer, enqueued) -> None:
    """G3: redelivery is a DUPLICATE; with processed_events truncated (a group
    reset past retention, a replay tool) the NTF-1 index still holds (I1)."""
    user = str(uuid.uuid4())
    env = envelope("order.confirmed", confirmed(user))
    assert await deliver(consumer, env) == Outcome.HANDLED
    before = [(r.id, r.seq) for r in await rows(app)]
    assert await deliver(consumer, env) == Outcome.DUPLICATE

    async with app.state.db.engine.begin() as conn:
        await conn.execute(text("TRUNCATE processed_events"))
    assert await deliver(consumer, env) == Outcome.HANDLED
    assert [(r.id, r.seq) for r in await rows(app)] == before
    assert await count(app, ProcessedEvent, ProcessedEvent.consumer_group == GROUP) == 1


async def test_a_payload_missing_a_required_field_is_dead_lettered(app, consumer, dlq) -> None:
    """C6: rendering to nobody or with no content is worse than not rendering."""
    payload = confirmed(str(uuid.uuid4()))
    payload["customer_id"] = "not-a-uuid"
    env = envelope("order.confirmed", payload)
    assert await deliver(consumer, env) == Outcome.DEAD_LETTERED
    assert [p["topic"] for p in dlq.produced] == ["smartretail.order.confirmed.dlq"]
    assert await count(app, Notification) == 0
    assert await count(app, ProcessedEvent) == 0  # rolled back with the effect
    # The next event is processed normally.
    assert await deliver(consumer, envelope("order.confirmed", confirmed(str(uuid.uuid4())))) == (
        Outcome.HANDLED
    )


async def test_after_commit_publishes_the_in_app_row_with_the_unread_count(
    app, consumer, settings
) -> None:
    user = str(uuid.uuid4())
    listener = create_redis(settings.redis_url)
    pubsub = listener.pubsub()
    await pubsub.subscribe(f"nt:user:{user}")
    try:
        await deliver(consumer, envelope("order.confirmed", confirmed(user)))
        await deliver(consumer, envelope("order.failed", failed(user)))
        messages = []
        for _ in range(50):
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.2)
            if message is not None:
                messages.append(json.loads(message["data"]))
            if len(messages) == 2:
                break
    finally:
        await pubsub.aclose()
        await listener.aclose()
    assert [m["kind"] for m in messages] == ["notification", "notification"]
    assert [m["type"] for m in messages] == ["order_confirmed", "order_failed"]
    assert [m["unread_count"] for m in messages] == [1, 2]
    in_app = await rows(app, Notification.channel == "in_app")
    assert [m["id"] for m in messages] == [str(r.id) for r in in_app]
    assert [m["seq"] for m in messages] == [r.seq for r in in_app]
    assert set(messages[0]) >= {"id", "seq", "type", "title", "body", "created_at", "unread_count"}


async def test_after_commit_failures_never_fail_the_event(app, consumer, enqueued) -> None:
    """A dead Redis and a dead broker are logged; the rows stay and NTF-6 recovers."""
    enqueued.fail = True
    dead = create_redis("redis://127.0.0.1:1/0")
    consumer.after_commit = after_commit(app.state.db.sessionmaker, dead, enqueued)
    user = str(uuid.uuid4())
    assert await deliver(consumer, envelope("order.confirmed", confirmed(user))) == Outcome.HANDLED
    assert len(await rows(app, Notification.user_id == uuid.UUID(user))) == 2
    await dead.aclose()


async def test_after_commit_survives_a_database_error(app, settings, enqueued, caplog) -> None:
    class Broken:
        def __call__(self):
            raise OSError("database down")

    run = after_commit(Broken(), app.state.redis, enqueued)  # type: ignore[arg-type]
    await run(envelope("order.confirmed", confirmed(str(uuid.uuid4()))))
    assert enqueued.ids == []
    assert any("re-read failed" in r.getMessage() for r in caplog.records)


async def test_a_database_outage_waits_instead_of_dead_lettering(app, consumer, dlq) -> None:
    """NTF-14: connection errors make the base wait; nothing reaches the DLQ."""
    calls = {"n": 0}
    real = consumer.store.run  # type: ignore[union-attr]

    async def flaky(group, env, handler):
        calls["n"] += 1
        if calls["n"] <= 4:
            raise OSError("connection refused")
        return await real(group, env, handler)

    consumer.store.run = flaky  # type: ignore[union-attr,method-assign]
    env = envelope("order.confirmed", confirmed(str(uuid.uuid4())))
    assert await deliver(consumer, env) == Outcome.HANDLED
    assert calls["n"] == 5 and dlq.produced == []


async def test_concurrent_redelivery_inserts_once(app, consumer) -> None:
    env = envelope("order.cancelled", cancelled(str(uuid.uuid4())))
    outcomes = await asyncio.gather(
        *(deliver(consumer, env) for _ in range(4)), return_exceptions=True
    )
    assert Outcome.HANDLED in outcomes
    assert await count(app, Notification) == 2
