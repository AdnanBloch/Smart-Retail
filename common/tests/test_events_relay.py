"""outbox-relay against a real ``outbox`` table (``make test-db``): ordering,
acknowledged-only marking, poison holds, SKIP LOCKED, leadership, retention."""

import asyncio
import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from smartretail_common.events import relay as relay_mod
from smartretail_common.events.relay import OutboxRelay, parse_sources
from smartretail_common.outbox import OutboxEvent, write_outbox
from tests.events_fakes import FakePublisher
from tests.test_events_serde import registered_codec

EVENT = "payment.failed"
TOPIC = "smartretail.payment.failed"


def payload(order_id: uuid.UUID) -> dict[str, object]:
    return {
        "order_id": order_id,
        "customer_id": uuid.uuid4(),
        "reason": None,
        "amount": Decimal("5.00"),
        "currency": "USD",
        "failed_at": "2026-10-14T00:00:00Z",
    }


async def add_rows(engine: AsyncEngine, aggregates: list[uuid.UUID]) -> None:
    async with async_sessionmaker(engine)() as session, session.begin():
        for aggregate in aggregates:
            write_outbox(
                session,
                event_type=EVENT,
                aggregate_type="order",
                aggregate_id=aggregate,
                payload=payload(aggregate),
                correlation_id=None,
            )


async def published(engine: AsyncEngine) -> list[bool]:
    async with engine.connect() as conn:
        rows = await conn.execute(select(OutboxEvent.published_at).order_by(OutboxEvent.id))
        return [r.published_at is not None for r in rows]


def make(engine: AsyncEngine, publisher: FakePublisher | None = None, **kw) -> OutboxRelay:
    return OutboxRelay(
        name="order",
        engine=engine,
        publisher=publisher or FakePublisher(),
        codec=registered_codec(EVENT),
        **kw,
    )


async def test_rows_are_published_in_order_keyed_by_aggregate_and_marked(events_engine) -> None:
    a, b = uuid.uuid4(), uuid.uuid4()
    await add_rows(events_engine, [a, b, a])
    relay = make(events_engine)
    stats = await relay.run_once()
    assert stats.published == 3
    produced = relay.publisher.produced  # type: ignore[attr-defined]
    assert [p["key"] for p in produced] == [str(a).encode(), str(b).encode(), str(a).encode()]
    assert {p["topic"] for p in produced} == {TOPIC}
    assert dict(produced[0]["headers"])["event_type"] == EVENT.encode()
    assert await published(events_engine) == [True, True, True]
    # Nothing left: the next cycle is empty.
    assert (await relay.run_once()).published == 0


async def test_a_row_without_correlation_id_is_correlated_to_itself(events_engine) -> None:
    await add_rows(events_engine, [uuid.uuid4()])
    relay = make(events_engine)
    await relay.run_once()
    value = relay.publisher.produced[0]["value"]  # type: ignore[attr-defined]
    decoded = await relay.codec.decode(TOPIC, value)
    async with events_engine.connect() as conn:
        event_id = await conn.scalar(select(OutboxEvent.event_id))
    assert decoded.correlation_id == str(event_id) and decoded.producer == "order"


async def test_only_acknowledged_rows_are_marked(events_engine) -> None:
    good, bad = uuid.uuid4(), uuid.uuid4()
    await add_rows(events_engine, [good, bad])
    relay = make(events_engine, FakePublisher(fail_keys={str(bad).encode()}))
    stats = await relay.run_once()
    assert (stats.published, stats.failed) == (1, 1)
    assert await published(events_engine) == [True, False]
    # The broker recovers: the unacknowledged row goes out next cycle.
    relay.publisher.fail_keys.clear()  # type: ignore[attr-defined]
    assert (await relay.run_once()).published == 1


async def test_a_broker_error_rolls_the_batch_back(events_engine) -> None:
    await add_rows(events_engine, [uuid.uuid4()])
    relay = make(events_engine, FakePublisher(raise_on_flush=RuntimeError("broker down")))
    with pytest.raises(RuntimeError):
        await relay.run_once()
    assert await published(events_engine) == [False]


async def test_a_poison_row_holds_its_aggregate_but_not_others(events_engine) -> None:
    poisoned, healthy = uuid.uuid4(), uuid.uuid4()
    await add_rows(events_engine, [poisoned, healthy, poisoned])
    # Corrupt the first row behind the writer's back (a hand-edited row).
    async with events_engine.begin() as conn:
        await conn.execute(
            update(OutboxEvent).where(OutboxEvent.id == 1).values(payload={"order_id": "x"})
        )
    now = [0.0]
    relay = make(events_engine, clock=lambda: now[0], hold_retry_seconds=60)
    stats = await relay.run_once()
    assert (stats.published, stats.held) == (1, 2)
    # Row 3 waits behind row 1: per-aggregate order is preserved.
    assert await published(events_engine) == [False, True, False]
    assert relay.held_aggregates == 1

    # Fixed by hand; after the hold expires both rows go out, in order.
    async with events_engine.begin() as conn:
        await conn.execute(
            update(OutboxEvent).where(OutboxEvent.id == 1).values(payload=_jsonable(poisoned))
        )
    assert (await relay.run_once()).held == 2  # still held before 60 s
    now[0] = 61
    assert (await relay.run_once()).published == 2
    assert await published(events_engine) == [True, True, True]


def _jsonable(aggregate: uuid.UUID) -> dict[str, object]:
    from pydantic_core import to_jsonable_python

    data: dict[str, object] = to_jsonable_python(payload(aggregate))
    return data


async def test_skip_locked_lets_a_second_relay_pass_over_locked_rows(events_engine) -> None:
    await add_rows(events_engine, [uuid.uuid4(), uuid.uuid4()])
    async with events_engine.connect() as holder:
        await holder.execute(
            text("SELECT id FROM outbox WHERE id = 1 FOR UPDATE")
        )  # another relay's batch, in flight
        stats = await asyncio.wait_for(make(events_engine).run_once(), timeout=5)
        assert stats.published == 1
        await holder.rollback()
    assert await published(events_engine) == [False, True]


async def test_one_relay_per_database_holds_the_advisory_lock(events_engine) -> None:
    async with events_engine.connect() as first, events_engine.connect() as second:
        assert await relay_mod._try_lock(first) is True
        assert await relay_mod._try_lock(second) is False


async def test_the_leader_publishes_until_stopped(events_engine) -> None:
    await add_rows(events_engine, [uuid.uuid4()])
    relay = make(events_engine, poll_interval=0.01)
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))
    for _ in range(200):
        if await published(events_engine) == [True]:
            break
        await asyncio.sleep(0.02)
    stop.set()
    await asyncio.wait_for(task, timeout=5)
    assert await published(events_engine) == [True]


async def test_a_failing_batch_backs_off_and_recovers(events_engine) -> None:
    await add_rows(events_engine, [uuid.uuid4()])
    publisher = FakePublisher(raise_on_flush=RuntimeError("broker down"))
    relay = make(events_engine, publisher, poll_interval=0.01)
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))
    await asyncio.sleep(0.2)
    publisher.raise_on_flush = None  # broker back
    for _ in range(200):
        if await published(events_engine) == [True]:
            break
        await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=5)
    assert await published(events_engine) == [True]


async def test_published_rows_past_retention_are_swept_and_unpublished_kept(events_engine) -> None:
    await add_rows(events_engine, [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()])
    async with events_engine.begin() as conn:
        await conn.execute(
            text("UPDATE outbox SET published_at = now() - interval '8 days' WHERE id = 1")
        )
        await conn.execute(text("UPDATE outbox SET published_at = now() WHERE id = 2"))
    relay = make(events_engine)
    await relay._maybe_sweep()
    async with events_engine.connect() as conn:
        ids = list(await conn.scalars(select(OutboxEvent.id).order_by(OutboxEvent.id)))
    assert ids == [2, 3]
    await relay._maybe_sweep()  # once a day: a second call does nothing
    assert await relay.sweep_published() == 0


def test_sources_are_parsed_from_the_environment_format() -> None:
    assert parse_sources("catalog=postgresql://a, order=postgresql://b,") == {
        "catalog": "postgresql://a",
        "order": "postgresql://b",
    }
    with pytest.raises(ValueError, match="bad OUTBOX_SOURCES"):
        parse_sources("catalog")
