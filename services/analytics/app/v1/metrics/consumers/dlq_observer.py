"""The DLQ observer (ANL-9): group ``analytics-dlq-observer`` on ``^smartretail\\..*\\.dlq$``.

Counts each dead-lettered message into ``failure_stats {date, "dlq",
"<original_topic>:<dlq.error.class>"}`` from **headers only** — the payload is
poison by definition and is never deserialised. The DLQ message has no event
id the observer can trust, so its dedupe key is the Kafka coordinate
``<topic>:<partition>:<offset>``, recorded in ``processed_events`` in the same
transaction as the ``$inc``.

Failures never go to a DLQ of their own (a DLQ must not feed itself): a Mongo
outage waits and retries; anything else is logged and the offset committed.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from pymongo.errors import DuplicateKeyError

from app.core.db import Mongo, Session, in_transaction
from app.core.errors import MongoUnavailable
from app.v1.metrics.enums import UNKNOWN_REASON, Collection, FailureSource
from app.v1.metrics.models import collection, execute
from app.v1.metrics.rollups import failure
from app.v1.metrics.utils import bucket_date
from smartretail_common.events.consumer import Message, MessageSource
from smartretail_common.events.registry import DLQ_SUFFIX

logger = logging.getLogger("analytics.dlq_observer")

COUNTED, DUPLICATE, SKIPPED = "counted", "duplicate", "skipped"


def _headers(message: Message) -> dict[str, str]:
    out: dict[str, str] = {}
    for name, value in message.headers() or []:
        out[name] = value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)
    return out


def _failed_at(headers: dict[str, str], fallback: datetime) -> datetime:
    raw = headers.get("dlq.failed_at")
    if raw:
        try:
            parsed = datetime.fromisoformat(raw)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            pass
    return fallback


@dataclass
class DlqObserver:
    mongo: Mongo
    group: str = "analytics-dlq-observer"
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    backoff_base: float = 0.5
    backoff_max: float = 8.0
    counts: dict[str, int] = field(
        default_factory=lambda: dict.fromkeys((COUNTED, DUPLICATE, SKIPPED), 0)
    )

    async def process(self, message: Message, source: MessageSource) -> str:
        outcome = await self._observe(message)
        await asyncio.to_thread(source.commit, message)
        self.counts[outcome] += 1
        return outcome

    async def _observe(self, message: Message) -> str:
        topic = message.topic() or ""
        headers = _headers(message)
        original = topic.removesuffix(DLQ_SUFFIX)
        reason = f"{original}:{headers.get('dlq.error.class') or UNKNOWN_REASON}"
        day = bucket_date(_failed_at(headers, self.clock()))
        coordinate = f"{topic}:{message.partition()}:{message.offset()}"

        async def work(session: Session) -> str:
            await collection(self.mongo, Collection.PROCESSED_EVENTS).insert_one(
                {
                    "_id": {"group": self.group, "event_id": coordinate},
                    "event_type": f"{original}{DLQ_SUFFIX}",
                    "processed_at": self.clock(),
                },
                session=session,
            )
            await execute(self.mongo, [failure(day, FailureSource.DLQ, reason)], session)
            return COUNTED

        waits = 0
        while True:
            try:
                return await in_transaction(self.mongo, work)
            except DuplicateKeyError:
                return DUPLICATE
            except MongoUnavailable as exc:
                delay = min(self.backoff_base * 2**waits, self.backoff_max)
                waits += 1
                logger.warning(
                    "mongo unavailable; waiting", extra={"delay": delay, "error": str(exc)}
                )
                await self.sleep(delay)
            except Exception:
                logger.exception("dlq observation failed; skipped", extra={"topic": topic})
                return SKIPPED

    async def run(self, source: MessageSource, stop: asyncio.Event) -> None:
        while not stop.is_set():
            message = await asyncio.to_thread(source.poll, 1.0)
            if message is None:
                continue
            if message.error() is not None:
                logger.warning("poll error", extra={"error": str(message.error())})
                continue
            await self.process(message, source)
