"""``outbox-relay``: publishes outbox rows to Kafka (RFC 0001 §3.1.1.2, A10).

One loop per producing database. Each cycle:

1. ``SELECT … WHERE published_at IS NULL ORDER BY id LIMIT n FOR UPDATE SKIP LOCKED``
   — the backlog in commit order, skipping rows another relay holds.
2. Produce each row keyed by ``aggregate_id`` (C3), then ``flush``.
3. ``UPDATE … SET published_at = now()`` for the rows Kafka **acknowledged**,
   and commit. Unacknowledged rows stay unpublished and go first next cycle.

A crash between 2 and 3 republishes the batch: at-least-once, absorbed by the
consumers' ``processed_events`` dedupe (C4). There is no path that marks a row
published without an acknowledgement.

* **Leadership (PLT-2).** A session-level ``pg_try_advisory_lock`` makes one
  relay per database active; the others wait as hot standbys, so batches are
  never interleaved and per-aggregate order holds across relay replicas.
* **Poison rows (PLT-4).** A row that cannot be serialised puts its
  aggregate on hold: that row and every later row of the same aggregate are
  skipped (order is preserved, other aggregates flow), and the hold is retried
  every ``hold_retry_seconds``.
* **Retention (PLT-18).** Once a day the leader deletes rows published more
  than ``retention_days`` ago. Unpublished rows are never deleted.
"""

import asyncio
import contextlib
import logging
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, cast

from sqlalchemy import Row, Table, delete, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from smartretail_common.events import registry
from smartretail_common.events.envelope import Envelope
from smartretail_common.events.producer import Publisher
from smartretail_common.events.registry import UnknownEvent
from smartretail_common.events.serde import AvroCodec, SerializationError
from smartretail_common.outbox import OutboxEvent

logger = logging.getLogger("outbox_relay")

OUTBOX = cast(Table, OutboxEvent.__table__)
# Any constant shared by every relay replica; one lock per database because
# each database has its own lock space.
ADVISORY_LOCK_KEY = 0x5352_4F42  # "SROB"


@dataclass
class RelayStats:
    published: int = 0
    held: int = 0
    failed: int = 0


@dataclass
class OutboxRelay:
    name: str  # the producing service, which is the envelope's ``producer``
    engine: AsyncEngine
    publisher: Publisher
    codec: AvroCodec
    batch_size: int = 100
    poll_interval: float = 0.5
    flush_timeout: float = 10.0
    hold_retry_seconds: float = 60.0
    retention_days: int = 7
    clock: Callable[[], float] = time.monotonic
    _held: dict[uuid.UUID, float] = field(default_factory=dict)
    _last_sweep: float | None = None

    # --- one cycle -----------------------------------------------------------------

    async def run_once(self) -> RelayStats:
        """Publishes one batch; returns what happened to it."""
        self._release_expired_holds()
        stats = RelayStats()
        async with self.engine.begin() as conn:
            rows = (
                await conn.execute(
                    select(OUTBOX)
                    .where(OUTBOX.c.published_at.is_(None))
                    .order_by(OUTBOX.c.id)
                    .limit(self.batch_size)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            if not rows:
                return stats
            acked: list[int] = []
            failed: list[int] = []
            for row in rows:
                if row.aggregate_id in self._held:
                    stats.held += 1
                    continue
                try:
                    value = await self.codec.encode(Envelope.from_outbox(row, producer=self.name))
                except (SerializationError, UnknownEvent) as exc:
                    self._hold(row, exc)
                    stats.held += 1
                    continue
                self.publisher.produce(
                    registry.catalogue().topic(row.event_type),
                    value=value,
                    key=str(row.aggregate_id).encode(),
                    headers=_headers(row),
                    on_delivery=_on_delivery(row.id, acked, failed),
                )
            # librdkafka calls the delivery callbacks from flush, on this thread.
            await asyncio.to_thread(self.publisher.flush, self.flush_timeout)
            if acked:
                await conn.execute(
                    update(OUTBOX).where(OUTBOX.c.id.in_(acked)).values(published_at=func.now())
                )
            stats.published, stats.failed = len(acked), len(failed)
        if stats.failed:
            logger.warning("rows not acknowledged", extra={"db": self.name, "count": stats.failed})
        return stats

    def _hold(self, row: Row[Any], exc: Exception) -> None:
        self._held[row.aggregate_id] = self.clock()
        logger.error(
            "poison outbox row: aggregate held",
            extra={
                "db": self.name,
                "outbox_id": row.id,
                "event_type": row.event_type,
                "correlation_id": row.correlation_id,
                "error": str(exc),
            },
        )

    def _release_expired_holds(self) -> None:
        now = self.clock()
        for aggregate_id, since in list(self._held.items()):
            if now - since >= self.hold_retry_seconds:
                del self._held[aggregate_id]

    @property
    def held_aggregates(self) -> int:
        return len(self._held)

    # --- retention -----------------------------------------------------------------

    async def sweep_published(self, *, batch: int = 5000) -> int:
        removed = 0
        while True:
            async with self.engine.begin() as conn:
                doomed = (
                    select(OUTBOX.c.id)
                    .where(
                        OUTBOX.c.published_at
                        < func.now() - func.make_interval(0, 0, 0, self.retention_days)
                    )
                    .limit(batch)
                )
                result = await conn.execute(delete(OUTBOX).where(OUTBOX.c.id.in_(doomed)))
            count = int(result.rowcount or 0)
            removed += count
            if count < batch:
                return removed

    async def _maybe_sweep(self) -> None:
        now = self.clock()
        if self._last_sweep is None or now - self._last_sweep >= 86_400:
            self._last_sweep = now
            removed = await self.sweep_published()
            if removed:
                logger.info(
                    "swept published outbox rows", extra={"db": self.name, "count": removed}
                )

    # --- the loop ------------------------------------------------------------------

    async def run(self, stop: asyncio.Event) -> None:
        """Waits for leadership, then publishes until ``stop`` is set."""
        backoff = 0.5
        while not stop.is_set():
            try:
                async with self.engine.connect() as lock_conn:
                    if not await _try_lock(lock_conn):
                        await _wait(stop, 5.0)  # hot standby
                        continue
                    logger.info("relay active", extra={"db": self.name})
                    await self._lead(stop)
                    return
            except Exception:
                logger.exception("relay cycle failed", extra={"db": self.name, "backoff": backoff})
                await _wait(stop, backoff)
                backoff = min(backoff * 2, 30.0)

    async def _lead(self, stop: asyncio.Event) -> None:
        backoff = 0.5
        while not stop.is_set():
            try:
                await self._maybe_sweep()
                stats = await self.run_once()
                backoff = 0.5
            except Exception:
                # Broker or registry down: the transaction rolled back, the rows are
                # unlocked, and they are retried after the backoff (0.5 s → 30 s).
                logger.exception("relay batch failed", extra={"db": self.name, "backoff": backoff})
                await _wait(stop, backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            full = stats.published + stats.held + stats.failed >= self.batch_size
            if not full:
                await _wait(stop, self.poll_interval)


def _headers(row: Row[Any]) -> list[tuple[str, bytes]]:
    headers = [("event_id", str(row.event_id).encode()), ("event_type", row.event_type.encode())]
    if row.correlation_id:
        headers.append(("correlation_id", row.correlation_id.encode()))
    return headers


def _on_delivery(row_id: int, acked: list[int], failed: list[int]) -> Callable[[Any, Any], None]:
    def callback(err: Any, _msg: Any) -> None:
        (failed if err is not None else acked).append(row_id)

    return callback


async def _try_lock(conn: AsyncConnection) -> bool:
    locked = await conn.scalar(text("SELECT pg_try_advisory_lock(:k)"), {"k": ADVISORY_LOCK_KEY})
    await conn.commit()  # the lock is session-level: it outlives this transaction
    return bool(locked)


async def _wait(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


# --- process entry point: python -m smartretail_common.events.relay ----------------


def parse_sources(spec: str) -> dict[str, str]:
    """``OUTBOX_SOURCES=catalog=postgresql+asyncpg://…,order=…`` → {name: url}."""
    sources: dict[str, str] = {}
    for part in filter(None, (p.strip() for p in spec.split(","))):
        name, _, url = part.partition("=")
        if not name or not url:
            raise ValueError(f"bad OUTBOX_SOURCES entry: {part!r}")
        sources[name] = url
    return sources


async def main() -> None:  # pragma: no cover - wiring, exercised by the e2e suite
    import signal

    from smartretail_common.events.producer import kafka_producer
    from smartretail_common.events.schema_registry import HttpSchemaRegistry

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    sources = parse_sources(os.environ["OUTBOX_SOURCES"])
    codec = AvroCodec(HttpSchemaRegistry(os.environ["SCHEMA_REGISTRY_URL"]))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    relays = [
        OutboxRelay(
            name=name,
            engine=create_async_engine(url, pool_size=2, max_overflow=0, pool_pre_ping=True),
            publisher=kafka_producer(os.environ["KAFKA_BOOTSTRAP_SERVERS"], f"outbox-relay-{name}"),
            codec=codec,
            batch_size=int(os.environ.get("OUTBOX_BATCH_SIZE", "100")),
            poll_interval=int(os.environ.get("OUTBOX_POLL_INTERVAL_MS", "500")) / 1000,
        )
        for name, url in sources.items()
    ]
    await asyncio.gather(*(relay.run(stop) for relay in relays))
    for relay in relays:
        await relay.engine.dispose()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
