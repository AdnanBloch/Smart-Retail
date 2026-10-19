"""The consumer base: dedupe, retry, DLQ, dependency waits, manual commit
(A11, C4, C6, PLT-5). Runs against the scratch database (``make test-db``)."""

import asyncio
import uuid

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from smartretail_common.errors import DependencyUnavailable
from smartretail_common.events import dedupe
from smartretail_common.events.consumer import EventConsumer, Outcome
from smartretail_common.events.dedupe import ProcessedEvent
from smartretail_common.events.envelope import Envelope
from tests.events_fakes import FakeMessage, FakePublisher, FakeSource
from tests.test_events_serde import PUBLISHED, envelope, registered_codec

TOPIC = "smartretail.catalog.product.published"


class Recorder:
    """A handler that writes a marker row in the consumer's transaction, so a
    test can see whether the effect committed."""

    def __init__(self, failures: list[BaseException] | None = None) -> None:
        self.failures = list(failures or [])
        self.calls = 0

    async def __call__(self, event: Envelope, session: AsyncSession) -> None:
        self.calls += 1
        await session.execute(text("CREATE TABLE IF NOT EXISTS effects (event_id uuid NOT NULL)"))
        await session.execute(text("INSERT INTO effects VALUES (:e)"), {"e": event.event_id})
        if self.failures:
            raise self.failures.pop(0)


async def no_sleep(_: float) -> None:
    return None


def make(events_engine, handler, **kw) -> tuple[EventConsumer, FakePublisher]:
    dlq = kw.pop("dlq", FakePublisher())
    kw.setdefault("sleep", no_sleep)
    consumer = EventConsumer(
        group="inventory",
        handlers={PUBLISHED: handler},
        sessionmaker=async_sessionmaker(events_engine, expire_on_commit=False),
        codec=registered_codec(PUBLISHED),
        dlq=dlq,
        **kw,
    )
    return consumer, dlq


async def message(consumer: EventConsumer, env: Envelope | None = None) -> FakeMessage:
    env = env or envelope()
    return FakeMessage(
        TOPIC, await consumer.codec.encode(env), _key=b"k", _headers=[("event_id", b"x")]
    )


async def effects(engine) -> int:
    async with engine.connect() as conn:
        exists = await conn.scalar(text("SELECT to_regclass('effects') IS NOT NULL"))
        if not exists:
            return 0
        return int(await conn.scalar(text("SELECT count(*) FROM effects")) or 0)


@pytest.fixture(autouse=True)
async def _drop_effects(events_engine):
    yield
    async with events_engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS effects"))


async def test_a_new_event_is_handled_recorded_and_committed(events_engine) -> None:
    consumer, _ = make(events_engine, Recorder())
    source = FakeSource()
    msg = await message(consumer)
    assert await consumer.process(msg, source) == Outcome.HANDLED
    assert source.committed == [msg]
    assert await effects(events_engine) == 1
    async with events_engine.connect() as conn:
        group = await conn.scalar(select(ProcessedEvent.consumer_group))
    assert group == "inventory"


async def test_a_redelivered_event_is_skipped_but_still_committed(events_engine) -> None:
    handler = Recorder()
    consumer, _ = make(events_engine, handler)
    source = FakeSource()
    env = envelope()
    first, again = await message(consumer, env), await message(consumer, env)
    assert await consumer.process(first, source) == Outcome.HANDLED
    assert await consumer.process(again, source) == Outcome.DUPLICATE
    assert handler.calls == 1 and await effects(events_engine) == 1
    assert source.committed == [first, again]
    assert consumer.counts[Outcome.DUPLICATE] == 1


async def test_another_group_handles_the_same_event_independently(events_engine) -> None:
    env = envelope()
    a, _ = make(events_engine, Recorder())
    b, _ = make(events_engine, Recorder())
    b.group = "analytics"
    assert await a.process(await message(a, env), FakeSource()) == Outcome.HANDLED
    assert await b.process(await message(b, env), FakeSource()) == Outcome.HANDLED


async def test_a_transient_failure_is_retried_and_rolls_back_the_attempt(events_engine) -> None:
    handler = Recorder(failures=[RuntimeError("blip")])
    consumer, dlq = make(events_engine, handler)
    assert await consumer.process(await message(consumer), FakeSource()) == Outcome.HANDLED
    # The failed attempt's effect and claim rolled back; the retry wrote both once.
    assert handler.calls == 2 and await effects(events_engine) == 1
    assert dlq.produced == []


async def test_after_three_failures_the_event_is_dead_lettered_and_committed(events_engine) -> None:
    handler = Recorder(failures=[ValueError("bad")] * 3)
    consumer, dlq = make(events_engine, handler)
    source = FakeSource()
    msg = await message(consumer)
    assert await consumer.process(msg, source) == Outcome.DEAD_LETTERED
    assert handler.calls == 3
    assert source.committed == [msg]  # the partition moves on (C6)
    [parked] = dlq.produced
    assert parked["topic"] == TOPIC + ".dlq"
    assert parked["key"] == b"k" and parked["value"] == msg.value()
    headers = dict(parked["headers"])
    assert headers["event_id"] == b"x"  # original headers kept
    assert headers["dlq.error.class"] == b"ValueError"
    assert headers["dlq.attempts"] == b"3"
    assert headers["dlq.consumer_group"] == b"inventory"
    # Nothing was recorded as processed: a DLQ replay will be handled.
    async with events_engine.connect() as conn:
        assert await conn.scalar(select(func.count()).select_from(ProcessedEvent)) == 0


async def test_a_dependency_outage_waits_without_counting_attempts(events_engine) -> None:
    waits: list[float] = []

    async def sleep(seconds: float) -> None:
        waits.append(seconds)

    outage = [DependencyUnavailable("db busy")] * 5
    handler = Recorder(failures=outage)
    consumer, dlq = make(events_engine, handler, sleep=sleep)
    assert await consumer.process(await message(consumer), FakeSource()) == Outcome.HANDLED
    assert handler.calls == 6 and dlq.produced == []  # five waits, never dead-lettered
    assert waits == [0.5, 1.0, 2.0, 4.0, 8.0]  # doubling, capped at 8 s


async def test_an_undecodable_message_is_dead_lettered_at_once(events_engine) -> None:
    handler = Recorder()
    consumer, dlq = make(events_engine, handler)
    source = FakeSource()
    poison = FakeMessage(TOPIC, b"not avro")
    assert await consumer.process(poison, source) == Outcome.DEAD_LETTERED
    assert handler.calls == 0 and source.committed == [poison]
    assert dict(dlq.produced[0]["headers"])["dlq.attempts"] == b"0"


async def test_an_event_type_without_a_handler_is_committed_and_ignored(events_engine) -> None:
    consumer, _ = make(events_engine, Recorder())
    consumer.handlers = {}
    source = FakeSource()
    assert await consumer.process(await message(consumer), source) == Outcome.IGNORED
    assert len(source.committed) == 1


async def test_a_failed_dead_letter_publish_leaves_the_offset_uncommitted(events_engine) -> None:
    consumer, _ = make(
        events_engine, Recorder(failures=[ValueError("bad")] * 3), dlq=FakePublisher(fail_all=True)
    )
    source = FakeSource()
    with pytest.raises(RuntimeError, match="dead-letter publish failed"):
        await consumer.process(await message(consumer), source)
    assert source.committed == []  # redelivered rather than lost


async def test_after_commit_runs_only_for_a_handled_event(events_engine) -> None:
    seen: list[uuid.UUID] = []

    async def after(event: Envelope) -> None:
        seen.append(event.event_id)

    consumer, _ = make(events_engine, Recorder(), after_commit=after)
    env = envelope()
    await consumer.process(await message(consumer, env), FakeSource())
    await consumer.process(await message(consumer, env), FakeSource())  # duplicate
    assert seen == [env.event_id]


async def test_run_processes_until_stopped_and_skips_poll_errors(events_engine) -> None:
    consumer, _ = make(events_engine, Recorder())
    good = await message(consumer)
    source = FakeSource([FakeMessage(TOPIC, None, _error="broker hiccup"), good])
    stop = asyncio.Event()

    original = source.commit

    def commit_then_stop(msg: FakeMessage) -> None:
        original(msg)
        stop.set()

    source.commit = commit_then_stop  # type: ignore[method-assign]
    await asyncio.wait_for(consumer.run(source, stop), timeout=5)
    assert source.committed == [good]
    assert consumer.topics == [TOPIC]


async def test_run_raises_when_a_message_cannot_be_processed(events_engine) -> None:
    consumer, _ = make(
        events_engine, Recorder(failures=[ValueError("bad")] * 3), dlq=FakePublisher(fail_all=True)
    )
    source = FakeSource([await message(consumer)])
    with pytest.raises(RuntimeError):
        await asyncio.wait_for(consumer.run(source, asyncio.Event()), timeout=5)


async def test_processed_events_older_than_retention_are_swept(events_engine) -> None:
    maker = async_sessionmaker(events_engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        fresh = envelope()
        assert await dedupe.claim(session, "g", envelope())
        assert await dedupe.claim(session, "g", fresh)
        await session.execute(
            text(
                "UPDATE processed_events SET processed_at = now() - interval '31 days' "
                "WHERE event_id <> :keep"
            ),
            {"keep": fresh.event_id},
        )
    async with maker() as session:
        assert await dedupe.sweep(session) == 1
        assert await dedupe.seen(session, "g", fresh.event_id)


# --- pluggable dedupe store (RFC 0001: "analytics supplies a Mongo store") ---------


class ScriptedStore:
    """A store that answers from a script; ``Exception`` entries are raised."""

    def __init__(self, *answers) -> None:
        self.answers = list(answers)
        self.calls = 0

    async def run(self, group, envelope, handler) -> str:
        self.calls += 1
        answer = self.answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer


def with_store(store) -> EventConsumer:
    return EventConsumer(
        group="analytics",
        handlers={PUBLISHED: Recorder()},
        codec=registered_codec(PUBLISHED),
        dlq=FakePublisher(),
        store=store,
        sleep=no_sleep,
    )


async def test_a_custom_store_decides_the_outcome_including_stale() -> None:
    consumer = with_store(ScriptedStore("stale"))
    source = FakeSource()
    assert await consumer.process(await message(consumer), source) == Outcome.STALE
    assert len(source.committed) == 1  # a stale event is skipped, never redelivered


async def test_a_lost_connection_is_waited_out_not_dead_lettered() -> None:
    # NTF-14: OSError / OperationalError from the store are infrastructure, not the message.
    from sqlalchemy.exc import OperationalError

    store = ScriptedStore(
        OSError("connection refused"),
        OperationalError("stmt", {}, Exception("server closed the connection")),
        OSError("connection refused"),
        OSError("connection refused"),
        "handled",
    )
    consumer = with_store(store)
    assert await consumer.process(await message(consumer), FakeSource()) == Outcome.HANDLED
    assert store.calls == 5  # more failures than max_attempts, yet never dead-lettered
    assert consumer.dlq.produced == []  # type: ignore[attr-defined]


def test_a_consumer_needs_a_store_or_a_sessionmaker() -> None:
    with pytest.raises(ValueError, match="store or a sessionmaker"):
        EventConsumer(
            group="g", handlers={}, codec=registered_codec(PUBLISHED), dlq=FakePublisher()
        )
