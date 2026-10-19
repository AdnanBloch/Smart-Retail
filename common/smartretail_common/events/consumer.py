"""The consumer base (RFC 0001 §3.1.1.1, A11, C4, C6, PLT-5).

Processing one message:

1. **Decode.** A message that cannot be decoded is poison: it goes to
   ``<topic>.dlq`` at once and its offset is committed. Waiting cannot fix it.
2. **Claim and handle in one transaction.** ``processed_events`` is claimed
   with ``ON CONFLICT DO NOTHING``; a conflict means the group has handled this
   ``event_id`` already (a redelivery), so the effect is skipped. Otherwise the
   handler runs in the same transaction, which then commits.
3. **Commit the offset** — manually (``enable.auto.commit=false``), and only
   after step 2 committed or the message was dead-lettered. A crash before
   this point redelivers the message; the claim makes the redelivery a no-op.

Failures in step 2 are classified, not counted blindly:

* ``DependencyUnavailable`` (lock contention, a downstream outage): the
  message is fine and the world is not. Roll back, wait (0.5 s doubling to
  8 s), try again; no attempt is counted and nothing is dead-lettered. The
  consumer stays on this message, so nothing behind it is processed out of
  order.
* Anything else: roll back and retry with exponential backoff, at most
  ``max_attempts`` (3) in all, then dead-letter with the error in ``dlq.*``
  headers and commit, so one bad message cannot stop its partition.
"""

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from smartretail_common.errors import DependencyUnavailable, is_connection_error
from smartretail_common.events.dedupe import DedupeStore, PostgresDedupeStore
from smartretail_common.events.envelope import Envelope
from smartretail_common.events.producer import Publisher
from smartretail_common.events.registry import DLQ_SUFFIX
from smartretail_common.events.serde import AvroCodec

logger = logging.getLogger("events.consumer")

CONSUMER_CONFIG: dict[str, Any] = {
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",  # a new group reads history
    "partition.assignment.strategy": "cooperative-sticky",
    "max.poll.interval.ms": 300_000,
}

# The second argument is the store's transaction: an AsyncSession for the
# default Postgres store, a Mongo session for analytics' store.
Handler = Callable[[Envelope, Any], Awaitable[None]]


class Outcome(StrEnum):
    HANDLED = "handled"
    DUPLICATE = "duplicate"
    DEAD_LETTERED = "dead_lettered"
    IGNORED = "ignored"  # a type this group subscribes to but does not act on
    STALE = "stale"  # older than the store's dedupe memory: skipped, never applied


class Message(Protocol):
    """The slice of ``confluent_kafka.Message`` the base uses."""

    def topic(self) -> str | None: ...
    def partition(self) -> int | None: ...
    def offset(self) -> int | None: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...
    def headers(self) -> list[tuple[str, bytes]] | None: ...
    def error(self) -> Any: ...


class MessageSource(Protocol):
    def poll(self, timeout: float) -> Message | None: ...
    def commit(self, message: Message) -> None: ...
    def close(self) -> None: ...


@dataclass
class EventConsumer:
    group: str
    handlers: Mapping[str, Handler]  # event_type → handler
    codec: AvroCodec
    dlq: Publisher
    sessionmaker: async_sessionmaker[AsyncSession] | None = None
    store: DedupeStore | None = None  # default: PostgresDedupeStore(sessionmaker)
    max_attempts: int = 3
    retry_base: float = 0.5
    dependency_backoff_max: float = 8.0
    after_commit: Callable[[Envelope], Awaitable[None]] | None = None
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    counts: dict[Outcome, int] = field(default_factory=lambda: dict.fromkeys(Outcome, 0))

    def __post_init__(self) -> None:
        if self.store is None:
            if self.sessionmaker is None:
                raise ValueError("EventConsumer needs a store or a sessionmaker")
            self.store = PostgresDedupeStore(self.sessionmaker)

    @property
    def topics(self) -> list[str]:
        from smartretail_common.events.registry import catalogue

        return sorted(catalogue().topic(event_type) for event_type in self.handlers)

    async def process(self, message: Message, source: MessageSource) -> Outcome:
        """Handles one message end to end, including its offset commit."""
        outcome = await self._outcome(message)
        await asyncio.to_thread(source.commit, message)
        self.counts[outcome] += 1
        return outcome

    async def _outcome(self, message: Message) -> Outcome:
        topic = message.topic() or ""
        try:
            envelope = await self.codec.decode(topic, message.value())
        except Exception as exc:
            await self._dead_letter(message, exc, attempts=0)
            return Outcome.DEAD_LETTERED
        handler = self.handlers.get(envelope.event_type)
        if handler is None:
            return Outcome.IGNORED

        attempts, waits = 0, 0
        while True:
            try:
                outcome = await self._attempt(envelope, handler)
            except Exception as exc:
                if isinstance(exc, DependencyUnavailable) or is_connection_error(exc):
                    # The world is down, not the message: wait, count nothing (PLT-D40,
                    # NTF-14 — a database outage is never dead-lettered).
                    delay = min(self.retry_base * 2**waits, self.dependency_backoff_max)
                    waits += 1
                    logger.warning(
                        "dependency unavailable; waiting",
                        extra={
                            "group": self.group,
                            "event_id": str(envelope.event_id),
                            "delay": delay,
                            "error": str(exc),
                        },
                    )
                    await self.sleep(delay)
                    continue
                attempts += 1
                if attempts >= self.max_attempts:
                    logger.exception(
                        "handler failed; dead-lettering",
                        extra={"group": self.group, "event_id": str(envelope.event_id)},
                    )
                    await self._dead_letter(message, exc, attempts=attempts)
                    return Outcome.DEAD_LETTERED
                await self.sleep(self.retry_base * 2 ** (attempts - 1) * (1 + random.random()))  # noqa: S311
                continue
            if outcome == Outcome.HANDLED and self.after_commit is not None:
                await self.after_commit(envelope)
            return outcome

    async def _attempt(self, envelope: Envelope, handler: Handler) -> Outcome:
        assert self.store is not None
        return Outcome(await self.store.run(self.group, envelope, handler))

    async def _dead_letter(self, message: Message, exc: BaseException, *, attempts: int) -> None:
        headers = list(message.headers() or [])
        headers += [
            ("dlq.error.class", type(exc).__name__.encode()),
            ("dlq.error.message", str(exc)[:1000].encode()),
            ("dlq.attempts", str(attempts).encode()),
            ("dlq.consumer_group", self.group.encode()),
            ("dlq.failed_at", datetime.now(UTC).isoformat().encode()),
        ]
        errors: list[Any] = []
        self.dlq.produce(
            (message.topic() or "") + DLQ_SUFFIX,
            value=message.value(),
            key=message.key(),
            headers=headers,
            on_delivery=lambda err, _msg: errors.append(err) if err is not None else None,
        )
        await asyncio.to_thread(self.dlq.flush, 10.0)
        if errors:
            # Never commit past a message we could not park: raising leaves the
            # offset uncommitted, so the message is redelivered.
            raise RuntimeError(f"dead-letter publish failed: {errors[0]}")

    async def run(self, source: MessageSource, stop: asyncio.Event) -> None:
        """Polls until ``stop`` is set. A failure to process a message (DLQ down,
        database down) leaves its offset uncommitted and is retried after a pause."""
        while not stop.is_set():
            message = await asyncio.to_thread(source.poll, 1.0)
            if message is None:
                continue
            if message.error() is not None:
                logger.warning(
                    "poll error", extra={"group": self.group, "error": str(message.error())}
                )
                continue
            try:
                await self.process(message, source)
            except Exception:
                logger.exception("message not processed; will be redelivered")
                await self.sleep(5.0)
                raise  # restart the consumer so the uncommitted offset is redelivered


Runner = Callable[[asyncio.Event], Coroutine[Any, Any, None]]


async def run_until_signal(*runners: Runner) -> None:  # pragma: no cover - process wiring
    """Runs several loops in one process until SIGTERM/SIGINT; any loop failing
    stops the others, so the container restarts as a whole (RFC 0001 run_consumers)."""
    import signal

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    tasks = [asyncio.create_task(runner(stop)) for runner in runners]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in done:
            task.result()
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def consumer_runner(  # pragma: no cover - process wiring
    consumer: EventConsumer, bootstrap_servers: str
) -> Runner:
    """A ``Runner`` for ``run_until_signal``: subscribe, process until stopped, close."""

    async def run(stop: asyncio.Event) -> None:
        source = kafka_source(bootstrap_servers, consumer.group, consumer.topics)
        logger.info("consumer started", extra={"group": consumer.group, "topics": consumer.topics})
        try:
            await consumer.run(source, stop)
        finally:
            source.close()

    return run


async def serve(  # pragma: no cover - process wiring, exercised by the e2e suite
    build: Callable[[AvroCodec, Publisher], EventConsumer],
    *,
    bootstrap_servers: str,
    schema_registry_url: str,
) -> None:
    """Runs a service's consumer process until SIGTERM."""
    from smartretail_common.events.producer import kafka_producer
    from smartretail_common.events.schema_registry import HttpSchemaRegistry

    codec = AvroCodec(HttpSchemaRegistry(schema_registry_url))
    consumer = build(codec, kafka_producer(bootstrap_servers, "dlq"))
    await run_until_signal(consumer_runner(consumer, bootstrap_servers))


def kafka_source(
    bootstrap_servers: str, group: str, topics: list[str]
) -> MessageSource:  # pragma: no cover
    """A subscribed ``confluent_kafka.Consumer`` with the platform's settings."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {**CONSUMER_CONFIG, "bootstrap.servers": bootstrap_servers, "group.id": group}
    )
    consumer.subscribe(topics)

    class _Source:
        def poll(self, timeout: float) -> Message | None:
            message: Message | None = consumer.poll(timeout)
            return message

        def commit(self, message: Message) -> None:
            consumer.commit(message=message, asynchronous=False)

        def close(self) -> None:
            consumer.close()

    return _Source()
