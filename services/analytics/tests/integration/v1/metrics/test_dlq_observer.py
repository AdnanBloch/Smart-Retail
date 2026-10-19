"""The DLQ observer (ANL-9): counts arrivals from headers only, dedupes on the
Kafka coordinate, waits on a Mongo outage and never re-dead-letters."""

import asyncio
from datetime import UTC, datetime

from app.v1.metrics.consumers import dlq_observer as module
from app.v1.metrics.consumers.dlq_observer import COUNTED, DUPLICATE, SKIPPED, DlqObserver
from app.v1.metrics.utils import failure_id
from smartretail_common.events.consumer import Outcome
from tests import factories as f
from tests.integration.conftest import Msg, Source, message

TOPIC = "smartretail.order.confirmed.dlq"


def dlq_msg(offset: int = 0, error_class: str = "SchemaError", failed_at: str | None = None):
    headers = [("dlq.error.class", error_class.encode())]
    if failed_at is not None:
        headers.append(("dlq.failed_at", failed_at.encode()))
    return Msg(TOPIC, b"\x00poison-never-decoded", headers, offset)


async def failures(mongo) -> list[dict]:
    return [d async for d in mongo.db.failure_stats.find({})]


async def test_a_dlq_arrival_is_counted_once_by_its_coordinate(mongo) -> None:
    observer = DlqObserver(mongo)
    source = Source()
    at = "2026-10-14T09:00:00+00:00"
    assert await observer.process(dlq_msg(7, failed_at=at), source) == COUNTED
    assert await observer.process(dlq_msg(7, failed_at=at), source) == DUPLICATE
    assert await observer.process(dlq_msg(8, failed_at=at), source) == COUNTED
    assert len(source.committed) == 3  # every outcome commits
    [doc] = await failures(mongo)
    assert doc["_id"] == failure_id("2026-10-14", "dlq", "smartretail.order.confirmed:SchemaError")
    assert doc["count"] == 2
    assert observer.counts == {COUNTED: 2, DUPLICATE: 1, SKIPPED: 0}


async def test_missing_headers_fall_back_to_unknown_and_today(mongo) -> None:
    now = datetime(2026, 10, 15, 3, tzinfo=UTC)
    observer = DlqObserver(mongo, clock=lambda: now)
    msg = Msg(TOPIC, b"x", [("dlq.failed_at", b"not-a-date")], 1)
    assert await observer.process(msg, Source()) == COUNTED
    [doc] = await failures(mongo)
    assert doc["_id"] == failure_id("2026-10-15", "dlq", "smartretail.order.confirmed:unknown")


async def test_a_consumer_dead_letter_reaches_failure_stats(consumer, mongo, dlq) -> None:
    """A malformed event goes to the DLQ; the observer counts it without decoding it."""
    good = f.envelope("order.confirmed", f.order_confirmed([f.item(f.uid(), f.uid(), 1, "1")]))
    bad = await message(consumer, good)
    bad._value = b"\x00\x00\x00\x00\x01broken"
    assert await consumer.process(bad, Source()) == Outcome.DEAD_LETTERED
    [parked] = dlq.produced
    assert await DlqObserver(mongo).process(parked, Source()) == COUNTED
    [doc] = await failures(mongo)
    assert doc["_id"]["source"] == "dlq"
    assert doc["_id"]["reason"] == "smartretail.order.confirmed:SerializationError"


async def test_mongo_down_waits_then_counts(mongo, dead_mongo) -> None:
    waits: list[float] = []
    observer = DlqObserver(dead_mongo)

    async def recover(delay: float) -> None:
        waits.append(delay)
        observer.mongo = mongo

    observer.sleep = recover
    assert await observer.process(dlq_msg(), Source()) == COUNTED
    assert waits == [0.5]


async def test_an_unexpected_failure_is_skipped_never_re_dead_lettered(mongo, monkeypatch) -> None:
    async def boom(*args, **kwargs):
        raise ValueError("bug")

    monkeypatch.setattr(module, "execute", boom)
    source = Source()
    assert await DlqObserver(mongo).process(dlq_msg(), source) == SKIPPED
    assert len(source.committed) == 1
    assert await failures(mongo) == []


async def test_the_poll_loop_processes_until_stopped(mongo) -> None:
    class Errored(Msg):
        def error(self):
            return "broker hiccup"

    stop = asyncio.Event()
    source = Source(queue=[dlq_msg(1), Errored(TOPIC, b""), dlq_msg(2)], stop=stop)
    observer = DlqObserver(mongo)
    await asyncio.wait_for(observer.run(source, stop), 10)
    assert observer.counts[COUNTED] == 2
    assert len(source.committed) == 2
