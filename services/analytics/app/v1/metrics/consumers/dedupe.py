"""``MongoDedupeStore``: the platform consumer's dedupe hook, for MongoDB (ANL-1).

``EventConsumer`` (``smartretail_common.events.consumer``) hands every decoded
envelope to ``store.run(group, envelope, handler)``. This store:

1. **Stale guard (ANL-2).** An event older than ``PROCESSED_EVENTS_TTL_DAYS - 1``
   days may have outlived its dedupe record, so applying it could count it a
   second time: it is skipped as ``stale`` (the base commits its offset).
2. **One transaction** (snapshot read, majority write): insert
   ``processed_events {_id: {group, event_id}}`` first — a duplicate raises
   ``DuplicateKeyError`` and the transaction aborts with no effect — then
   insert the envelope into ``raw_events``, then run the handler with the
   session so every rollup ``$inc`` commits with the dedupe record or not at all.

An unreachable Mongo raises ``MongoUnavailable`` (a ``DependencyUnavailable``):
the base waits and retries the same message, never dead-letters it (ANL-D30).
"""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from pymongo.errors import DuplicateKeyError

from app.core.db import Mongo, Session, in_transaction
from app.v1.metrics.enums import Collection
from app.v1.metrics.models import collection
from smartretail_common.events.consumer import Handler, Outcome
from smartretail_common.events.envelope import Envelope


def _utc(moment: datetime) -> datetime:
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def raw_document(envelope: Envelope, received_at: datetime) -> dict[str, Any]:
    """``raw_events``: the full envelope as received (RFC 0008 §3.1.3)."""
    return {
        "event_id": str(envelope.event_id),
        "event_type": envelope.event_type,
        "event_version": envelope.event_version,
        "occurred_at": _utc(envelope.occurred_at),
        "producer": envelope.producer,
        "correlation_id": envelope.correlation_id,
        "causation_id": envelope.causation_id,
        "actor_id": envelope.actor_id,
        "client_id": envelope.client_id,  # stored, never read by a metric
        "traceparent": envelope.traceparent,
        "payload": envelope.payload,
        "received_at": received_at,
    }


def envelope_from_raw(doc: dict[str, Any]) -> Envelope:
    return Envelope.from_record(doc)


class MongoDedupeStore:
    def __init__(
        self,
        mongo: Mongo,
        *,
        processed_ttl_days: int,
        record_raw: bool = True,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.mongo = mongo
        self.processed_ttl_days = processed_ttl_days
        # Only the live group archives envelopes; a backfill reads them.
        self.record_raw = record_raw
        self.clock = clock

    def is_stale(self, envelope: Envelope) -> bool:
        cutoff = self.clock() - timedelta(days=self.processed_ttl_days - 1)
        return _utc(envelope.occurred_at) < cutoff

    async def run(self, group: str, envelope: Envelope, handler: Handler) -> str:
        if self.is_stale(envelope):
            return Outcome.STALE.value

        async def work(session: Session) -> str:
            now = self.clock()
            await collection(self.mongo, Collection.PROCESSED_EVENTS).insert_one(
                {
                    "_id": {"group": group, "event_id": str(envelope.event_id)},
                    "event_type": envelope.event_type,
                    "processed_at": now,
                },
                session=session,
            )
            if self.record_raw:
                # $setOnInsert: the archive is keyed by event_id, whichever group wrote it.
                await collection(self.mongo, Collection.RAW_EVENTS).update_one(
                    {"event_id": str(envelope.event_id)},
                    {"$setOnInsert": raw_document(envelope, now)},
                    upsert=True,
                    session=session,
                )
            await handler(envelope, session)
            return Outcome.HANDLED.value

        try:
            return await in_transaction(self.mongo, work)
        except DuplicateKeyError as exc:
            if Collection.PROCESSED_EVENTS in str(exc):
                return Outcome.DUPLICATE.value
            raise
