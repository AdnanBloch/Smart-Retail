"""Races on the shared event-pipeline resources, against the scratch database
(``make test-db``): relay replicas contending for one database's advisory
lock (PLT-2), a poison row under concurrent writers (PLT-4), the relay's
full-batch loop, and duplicate deliveries racing for the same dedupe claim (C4)."""

import asyncio
import logging
import uuid

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from smartretail_common.events import relay as relay_mod
from smartretail_common.events.consumer import Outcome
from smartretail_common.events.envelope import Envelope
from smartretail_common.outbox import OutboxEvent, write_outbox
from tests.events_fakes import FakeSource
from tests.test_events_consumer import effects, message
from tests.test_events_consumer import make as make_consumer
from tests.test_events_relay import EVENT, add_rows, make, payload, published
from tests.test_events_serde import envelope


@pytest.fixture
def fast_waits(monkeypatch):
    """Caps the relay's waits (5 s standby, backoff) so a test runs in
    milliseconds; ``stop`` still interrupts them as in production."""
    real = relay_mod._wait

    async def capped(stop: asyncio.Event, seconds: float) -> None:
        await real(stop, min(seconds, 0.02))

    monkeypatch.setattr(relay_mod, "_wait", capped)


async def wait_until(predicate, *, within: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + within
    while not await predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.02)


async def all_published(engine: AsyncEngine) -> bool:
    flags = await published(engine)
    return bool(flags) and all(flags)


@pytest.fixture
async def effects_table(events_engine):
    async with events_engine.begin() as conn:
        await conn.execute(text("CREATE TABLE IF NOT EXISTS effects (event_id uuid NOT NULL)"))
    yield
    async with events_engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS effects"))


# --- leadership --------------------------------------------------------------------


async def test_of_two_relay_replicas_only_the_leader_publishes(
    events_engine, events_db_url, fast_waits, caplog
) -> None:
    # Two replicas = two processes = two engines on the same database.
    other_engine = create_async_engine(events_db_url, pool_size=2)
    first = make(events_engine, poll_interval=0.01)
    second = make(other_engine, poll_interval=0.01)
    stop = asyncio.Event()
    try:
        with caplog.at_level(logging.INFO, logger="outbox_relay"):
            tasks = [asyncio.create_task(r.run(stop)) for r in (first, second)]
            aggregates = [uuid.uuid4() for _ in range(4)]
            for aggregate in aggregates:  # rows keep arriving while both run
                await add_rows(events_engine, [aggregate, aggregate])
                await asyncio.sleep(0.01)
            await wait_until(lambda: all_published(events_engine))
            stop.set()
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
    finally:
        await other_engine.dispose()

    produced = [len(r.publisher.produced) for r in (first, second)]  # type: ignore[attr-defined]
    # Exactly one replica published everything; the other stayed a hot standby.
    assert sorted(produced) == [0, 8]
    assert [r.message for r in caplog.records].count("relay active") == 1
    leader = first if produced[0] else second
    keys = [p["key"] for p in leader.publisher.produced]  # type: ignore[attr-defined]
    assert keys == [str(a).encode() for a in aggregates for _ in range(2)]  # commit order


async def test_a_standby_takes_over_when_the_leader_dies(
    events_engine, events_db_url, fast_waits
) -> None:
    leader_engine = create_async_engine(events_db_url, pool_size=2)
    leader = make(leader_engine, poll_interval=0.01)
    standby = make(events_engine, poll_interval=0.01)
    leader_stop, standby_stop = asyncio.Event(), asyncio.Event()
    leader_task = asyncio.create_task(leader.run(leader_stop))
    await add_rows(events_engine, [uuid.uuid4()])
    await wait_until(lambda: all_published(events_engine))
    standby_task = asyncio.create_task(standby.run(standby_stop))
    await asyncio.sleep(0.1)  # several standby cycles while the leader holds the lock
    assert standby.publisher.produced == []  # type: ignore[attr-defined]

    # The leader's process exits: its sessions close and the lock is released.
    leader_stop.set()
    await asyncio.wait_for(leader_task, timeout=5)
    await leader_engine.dispose()

    await add_rows(events_engine, [uuid.uuid4()])
    await wait_until(lambda: all_published(events_engine))
    standby_stop.set()
    await asyncio.wait_for(standby_task, timeout=5)
    assert len(leader.publisher.produced) == 1  # type: ignore[attr-defined]
    assert len(standby.publisher.produced) == 1  # type: ignore[attr-defined]


async def test_an_unreachable_database_is_retried_until_stopped(fast_waits, caplog) -> None:
    engine = create_async_engine("postgresql+asyncpg://u:p@127.0.0.1:1/nowhere")
    relay = make(engine)
    stop = asyncio.Event()
    with caplog.at_level(logging.ERROR, logger="outbox_relay"):
        task = asyncio.create_task(relay.run(stop))
        await asyncio.sleep(0.2)
        stop.set()
        await asyncio.wait_for(task, timeout=5)
    await engine.dispose()
    failures = [r for r in caplog.records if r.message == "relay cycle failed"]
    assert len(failures) >= 2  # it kept trying instead of giving up
    # The backoff doubles between attempts.
    assert [r.backoff for r in failures[:2]] == [0.5, 1.0]


# --- batches -----------------------------------------------------------------------


async def test_a_full_batch_is_followed_at_once_by_the_next(events_engine, fast_waits) -> None:
    await add_rows(events_engine, [uuid.uuid4() for _ in range(7)])
    # A poll interval far longer than the test: only the full-batch path is fast.
    relay = make(events_engine, batch_size=2, poll_interval=30)
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))
    await wait_until(lambda: all_published(events_engine), within=3)
    stop.set()
    await asyncio.wait_for(task, timeout=5)
    assert len(relay.publisher.produced) == 7  # type: ignore[attr-defined]


async def test_the_correlation_id_travels_as_a_header(events_engine) -> None:
    async with async_sessionmaker(events_engine)() as session, session.begin():
        aggregate = uuid.uuid4()
        write_outbox(
            session,
            event_type=EVENT,
            aggregate_type="order",
            aggregate_id=aggregate,
            payload=payload(aggregate),
            correlation_id="corr-42",
        )
    relay = make(events_engine)
    await relay.run_once()
    headers = dict(relay.publisher.produced[0]["headers"])  # type: ignore[attr-defined]
    assert headers["correlation_id"] == b"corr-42"


# --- poison rows -------------------------------------------------------------------


async def test_a_poison_row_holds_only_its_aggregate_while_writers_keep_writing(
    events_engine, fast_waits
) -> None:
    poisoned = uuid.uuid4()
    await add_rows(events_engine, [poisoned])
    async with events_engine.begin() as conn:  # corrupted behind the writer's back
        await conn.execute(
            text('UPDATE outbox SET payload = \'{"order_id": "x"}\' WHERE aggregate_id = :a'),
            {"a": poisoned},
        )
    relay = make(events_engine, poll_interval=0.01, hold_retry_seconds=3600)
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))

    healthy = [uuid.uuid4() for _ in range(5)]

    async def writer(aggregates: list[uuid.UUID]) -> None:
        for aggregate in aggregates:
            await add_rows(events_engine, [aggregate])
            await asyncio.sleep(0.005)

    # Two writers commit concurrently with the relay: healthy aggregates and
    # further (valid) events of the held aggregate.
    await asyncio.gather(writer(healthy), writer([poisoned] * 3))

    async def healthy_out() -> bool:
        async with events_engine.connect() as conn:
            pending = await conn.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None), OutboxEvent.aggregate_id != poisoned)
            )
        return pending == 0

    await wait_until(healthy_out)
    await asyncio.sleep(0.1)  # several more cycles: the held rows must stay put
    stop.set()
    await asyncio.wait_for(task, timeout=5)

    async with events_engine.connect() as conn:
        held = (
            await conn.execute(
                select(OutboxEvent.published_at).where(OutboxEvent.aggregate_id == poisoned)
            )
        ).all()
    assert len(held) == 4 and all(r.published_at is None for r in held)
    keys = {p["key"] for p in relay.publisher.produced}  # type: ignore[attr-defined]
    assert keys == {str(a).encode() for a in healthy}
    assert relay.held_aggregates == 1


# --- consumer dedupe ---------------------------------------------------------------


class SlowRecorder:
    """Writes the effect, then holds its transaction open so a concurrent
    duplicate reaches the claim while the first is still uncommitted."""

    def __init__(self, hold: float) -> None:
        self.hold = hold
        self.calls = 0

    async def __call__(self, event: Envelope, session) -> None:
        self.calls += 1
        await session.execute(text("INSERT INTO effects VALUES (:e)"), {"e": event.event_id})
        await asyncio.sleep(self.hold)


async def test_concurrent_duplicate_deliveries_apply_the_effect_once(
    events_engine, effects_table
) -> None:
    handler = SlowRecorder(hold=0.2)
    # After a rebalance two members of the group can hold the same message.
    consumers = [make_consumer(events_engine, handler)[0] for _ in range(4)]
    env = envelope()
    messages = [await message(c, env) for c in consumers]
    outcomes = await asyncio.gather(
        *(c.process(m, FakeSource()) for c, m in zip(consumers, messages, strict=True))
    )
    assert sorted(outcomes) == sorted([Outcome.HANDLED] + [Outcome.DUPLICATE] * 3)
    assert await effects(events_engine) == 1
    # The losers blocked on the winner's uncommitted claim instead of running.
    assert handler.calls == 1


async def test_a_duplicate_racing_a_failed_attempt_handles_the_event(
    events_engine, effects_table
) -> None:
    """The first delivery's handler fails and rolls back its claim while a
    duplicate waits on it: the duplicate must then handle the event, or it
    would be lost."""
    first_in = asyncio.Event()

    class FailsFirst:
        calls = 0

        async def __call__(self, event: Envelope, session) -> None:
            self.calls += 1
            await session.execute(text("INSERT INTO effects VALUES (:e)"), {"e": event.event_id})
            if self.calls == 1:
                first_in.set()
                await asyncio.sleep(0.2)  # the duplicate is now blocked on our claim
                raise ValueError("handler bug on this attempt")

    handler = FailsFirst()
    failing, _ = make_consumer(events_engine, handler, max_attempts=1)
    duplicate, _ = make_consumer(events_engine, handler)
    env = envelope()

    async def second() -> Outcome:
        await first_in.wait()
        return await duplicate.process(await message(duplicate, env), FakeSource())

    first_outcome, second_outcome = await asyncio.gather(
        failing.process(await message(failing, env), FakeSource()), second()
    )
    assert first_outcome is Outcome.DEAD_LETTERED
    assert second_outcome is Outcome.HANDLED
    assert await effects(events_engine) == 1
