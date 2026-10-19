"""The backfill path from ``raw_events`` (Q4 archive 0003, ANL-G9, ANL-D20).

A new projection is replayed over the archived envelopes with the same
transaction shape as the live consumer, under its own dedupe group
``analytics.backfill.<projection>``: a rerun after a crash applies each event
once, and the live consumer's effects are never repeated. Reach is
``RAW_EVENTS_TTL_DAYS``. ``until`` defaults to the start of the run, so events
arriving after the projection goes live are not applied twice.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

from app.core.config import Settings
from app.core.db import Mongo
from app.v1.metrics.consumers import context, rollup_handler
from app.v1.metrics.consumers.dedupe import MongoDedupeStore, envelope_from_raw
from app.v1.metrics.enums import Collection
from app.v1.metrics.models import collection
from app.v1.metrics.rollups import PROJECTIONS
from smartretail_common.events.consumer import Outcome


class UnknownProjection(ValueError):
    pass


@dataclass
class BackfillReport:
    projection: str
    group: str
    applied: int = 0
    duplicate: int = 0


def backfill_group(projection: str) -> str:
    return f"analytics.backfill.{projection}"


async def backfill(
    mongo: Mongo,
    settings: Settings,
    *,
    projection: str,
    since: datetime,
    until: datetime | None = None,
) -> BackfillReport:
    try:
        formula = PROJECTIONS[projection]
    except KeyError:
        raise UnknownProjection(
            f"unknown projection {projection!r}; known: {', '.join(sorted(PROJECTIONS))}"
        ) from None
    until = until or datetime.now(UTC)
    report = BackfillReport(projection=projection, group=backfill_group(projection))
    store = MongoDedupeStore(
        mongo, processed_ttl_days=settings.processed_events_ttl_days, record_raw=False
    )
    handler = rollup_handler(mongo, context(settings), formula)
    cursor = (
        collection(mongo, Collection.RAW_EVENTS)
        .find({"occurred_at": {"$gte": since, "$lt": until}})
        .sort([("occurred_at", 1), ("event_id", 1)])
    )
    async for raw in cursor:
        outcome = await store.run(report.group, envelope_from_raw(raw), handler)
        if outcome == Outcome.HANDLED:
            report.applied += 1
        else:
            report.duplicate += 1
    return report
