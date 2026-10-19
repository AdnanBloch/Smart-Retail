"""``rebuild_daily_rollups`` (RFC 0008 §3.1.1.4, Q8 archive 0000).

For each UTC date in ``[today - RECONCILE_LOOKBACK_DAYS, today]`` — days fully
inside ``raw_events``' TTL — re-run the live formulas (``rollups.apply``) over
that day's envelopes, fold them in memory and compare with the stored
``daily_metrics`` and ``failure_stats`` documents. Drift is repaired by
replacing the document with the recomputed one, inside one transaction per
day, so a rerun or a concurrent run converges on the same value.

Scope: only documents the live formulas own — ``orders.<cur>``, ``stockouts``,
``assistant`` buckets and ``order``/``payment`` failures. ``dlq`` failures have
no envelope in ``raw_events`` and backfill-only metrics (``shipments``) have
no live formula, so both are left alone. ``counters`` and ``product_stats``
are all-time and are protected by the ANL-1 transaction instead.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from bson.decimal128 import Decimal128
from bson.int64 import Int64

from app.core.config import Settings
from app.core.db import Mongo, Session, in_transaction
from app.v1.metrics.consumers.dedupe import envelope_from_raw
from app.v1.metrics.enums import Collection, FailureSource, MetricKey
from app.v1.metrics.models import collection
from app.v1.metrics.rollups import Folded, RollupContext, apply, doc_key, fold

logger = logging.getLogger("analytics.reconcile")

RECONCILED = (Collection.DAILY_METRICS, Collection.FAILURE_STATS)
LIVE_METRICS = {
    "$regex": f"^({MetricKey.ORDERS_PREFIX.replace('.', '[.]')}|{MetricKey.STOCKOUTS}$"
    f"|{MetricKey.ASSISTANT}$)"
}
LIVE_FAILURE_SOURCES = [str(FailureSource.ORDER), str(FailureSource.PAYMENT)]


@dataclass
class ReconcileReport:
    days: list[str] = field(default_factory=list)
    drift: dict[str, int] = field(default_factory=lambda: {str(c): 0 for c in RECONCILED})

    @property
    def total_drift(self) -> int:
        return sum(self.drift.values())


def normalise(value: Any) -> Any:
    """Comparable form: Decimal128 → Decimal, Int64 → int, ``updated_at`` dropped."""
    if isinstance(value, dict):
        return {k: normalise(v) for k, v in value.items() if k != "updated_at"}
    if isinstance(value, list):
        return [normalise(v) for v in value]
    if isinstance(value, Decimal128):
        return value.to_decimal()
    if isinstance(value, Int64):
        return int(value)
    return value


def to_bson(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: to_bson(v) for k, v in value.items()}
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return Int64(value)
    if isinstance(value, Decimal):
        return Decimal128(value)
    return value


def _day_range(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=UTC)
    return start, start + timedelta(days=1)


async def recompute(mongo: Mongo, ctx: RollupContext, day: date, session: Session) -> Folded:
    """The day's live-owned documents, recomputed from ``raw_events``."""
    start, end = _day_range(day)
    folded = Folded()
    cursor = (
        collection(mongo, Collection.RAW_EVENTS)
        .find({"occurred_at": {"$gte": start, "$lt": end}}, session=session)
        .sort([("occurred_at", 1), ("event_id", 1)])
    )
    async for raw in cursor:
        ops = [op for op in apply(envelope_from_raw(raw), ctx) if op.collection in RECONCILED]
        fold(ops, folded)
    return folded


async def _stored(mongo: Mongo, day: str, session: Session) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    queries = {
        Collection.DAILY_METRICS: {"_id.date": day, "_id.metric": LIVE_METRICS},
        Collection.FAILURE_STATS: {"_id.date": day, "_id.source": {"$in": LIVE_FAILURE_SOURCES}},
    }
    for name, query in queries.items():
        async for doc in collection(mongo, name).find(query, session=session):
            out[doc_key(name, doc["_id"])] = doc
    return out


async def reconcile_day(mongo: Mongo, ctx: RollupContext, day: date) -> dict[str, int]:
    """Repairs one day in one transaction; returns drifted documents per collection."""

    async def work(session: Session) -> dict[str, int]:
        drift: dict[str, int] = {str(c): 0 for c in RECONCILED}
        expected = await recompute(mongo, ctx, day, session)
        stored = await _stored(mongo, day.isoformat(), session)
        now = datetime.now(UTC)
        for key, doc in expected.docs.items():
            name = expected.collections[key]
            if normalise(stored.get(key)) != normalise(doc):
                await collection(mongo, name).replace_one(
                    {"_id": doc["_id"]},
                    {**to_bson(doc), "updated_at": now},
                    upsert=True,
                    session=session,
                )
                drift[name] += 1
        for key, doc in stored.items():
            if key not in expected.docs:  # a bucket no envelope supports
                name = (
                    Collection.DAILY_METRICS
                    if key.startswith(Collection.DAILY_METRICS)
                    else Collection.FAILURE_STATS
                )
                await collection(mongo, name).delete_one({"_id": doc["_id"]}, session=session)
                drift[name] += 1
        return drift

    return await in_transaction(mongo, work)


async def rebuild_daily_rollups(
    mongo: Mongo, settings: Settings, *, today: date | None = None
) -> ReconcileReport:
    ctx = RollupContext(reporting_currency=settings.analytics_reporting_currency)
    today = today or datetime.now(UTC).date()
    report = ReconcileReport()
    for offset in range(settings.reconcile_lookback_days, -1, -1):
        day = today - timedelta(days=offset)
        drift = await reconcile_day(mongo, ctx, day)
        report.days.append(day.isoformat())
        for name, n in drift.items():
            report.drift[name] += n
    if report.total_drift:
        # Expected zero: drift is a bug signal, not a routine correction.
        logger.warning("reconciliation repaired drift", extra={"drift": report.drift})
    return report
