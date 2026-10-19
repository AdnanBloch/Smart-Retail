"""``rebuild_daily_rollups`` (drift repair, idempotency), the Celery task, the
``raw_events`` backfill (ANL-G9) and the operator CLI."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from bson.int64 import Int64

from app.core.celery_app import app as celery_app
from app.core.config import get_settings
from app.v1.metrics import cli
from app.v1.metrics.backfill_repo import UnknownProjection, backfill, backfill_group
from app.v1.metrics.consumers.dedupe import envelope_from_raw
from app.v1.metrics.reconcile_repo import rebuild_daily_rollups
from app.v1.metrics.rollups import RollupContext, apply_shipments, fold
from app.v1.metrics.utils import bucket_id, failure_id
from tests import factories as f
from tests.integration.conftest import deliver
from tests.integration.v1.metrics.test_consumer import scripted_day


def today():
    return datetime.now(UTC).date()


async def live_docs(mongo):
    daily = {repr(d["_id"]): d async for d in mongo.db.daily_metrics.find({})}
    fails = {repr(d["_id"]): d async for d in mongo.db.failure_stats.find({})}
    for d in [*daily.values(), *fails.values()]:
        d.pop("updated_at", None)
    return daily, fails


async def test_consistent_data_reconciles_with_zero_drift_twice(consumer, mongo, settings) -> None:
    await deliver(consumer, *scripted_day())
    before = await live_docs(mongo)
    report = await rebuild_daily_rollups(mongo, settings, today=today())
    assert report.total_drift == 0
    assert len(report.days) == settings.reconcile_lookback_days + 1
    assert report.days[-1] == today().isoformat()
    assert await live_docs(mongo) == before
    assert (await rebuild_daily_rollups(mongo, settings, today=today())).total_drift == 0


async def test_drift_is_repaired_and_counted(consumer, mongo, settings) -> None:
    a = f.uid()
    events = [
        f.envelope("order.created", f.order_created([f.item(f.uid(), a, 1, "10")])),
        f.envelope("payment.failed", f.payment_failed("card_declined")),
    ]
    await deliver(consumer, *events)
    day = today().isoformat()
    before = await live_docs(mongo)
    platform = bucket_id(day, "orders.USD", None)
    # Corrupt: a wrong count, a bucket no event supports, a lost failure document.
    await mongo.db.daily_metrics.update_one({"_id": platform}, {"$inc": {"orders_created": 5}})
    await mongo.db.daily_metrics.insert_one({"_id": bucket_id(day, "orders.EUR", None), "x": 1})
    await mongo.db.failure_stats.delete_one({"_id": failure_id(day, "payment", "card_declined")})
    # Not owned by the live formulas: must survive untouched.
    dlq = failure_id(day, "dlq", "smartretail.order.created:SchemaError")
    shipments = bucket_id(day, "shipments", None)
    await mongo.db.failure_stats.insert_one({"_id": dlq, "count": Int64(3)})
    await mongo.db.daily_metrics.insert_one({"_id": shipments, "transitions": Int64(2)})

    report = await rebuild_daily_rollups(mongo, settings, today=today())
    assert report.drift == {"daily_metrics": 2, "failure_stats": 1}

    daily, fails = await live_docs(mongo)
    assert daily[repr(platform)]["orders_created"] == 1
    assert repr(bucket_id(day, "orders.EUR", None)) not in daily
    assert fails[repr(failure_id(day, "payment", "card_declined"))]["count"] == 1
    assert fails[repr(dlq)]["count"] == 3
    assert daily[repr(shipments)]["transitions"] == 2
    for key, doc in before[0].items():
        assert daily[key] == doc
    assert (await rebuild_daily_rollups(mongo, settings, today=today())).total_drift == 0


def test_the_task_is_routed_and_scheduled() -> None:
    conf = celery_app.conf
    assert conf.task_default_queue == "analytics.maintenance"
    assert conf.task_acks_late and conf.task_reject_on_worker_lost
    assert {e["task"] for e in conf.beat_schedule.values()} == {
        "smartretail.analytics.rebuild_daily_rollups"
    }


async def test_the_celery_task_runs_the_reconciliation(consumer, mongo, settings, monkeypatch):
    from app.v1.metrics.tasks.maintenance import rebuild_daily_rollups as task

    await deliver(consumer, *scripted_day())
    await mongo.db.daily_metrics.update_many({}, {"$inc": {"orders_created": 1}})
    monkeypatch.setenv("ANALYTICS_MONGO_URI", settings.analytics_mongo_uri)
    monkeypatch.setenv("ANALYTICS_MONGO_DB", settings.analytics_mongo_db)
    monkeypatch.setenv("JWT_PUBLIC_KEY_PATH", "")
    get_settings.cache_clear()
    try:
        result = await asyncio.to_thread(lambda: task.apply().get())
    finally:
        get_settings.cache_clear()
    assert result["drift"]["daily_metrics"] > 0
    assert len(result["days"]) == settings.reconcile_lookback_days + 1


async def test_backfill_equals_live_application(consumer, mongo, settings) -> None:
    """ANL-G9: the shipments projection backfilled over raw_events equals the
    same projection applied to the same events; a rerun applies nothing."""
    yesterday = datetime.now(UTC) - timedelta(days=1)
    events = [
        f.envelope("shipment.status_changed", f.shipment("shipped"), occurred_at=yesterday),
        f.envelope("shipment.status_changed", f.shipment("delivered"), occurred_at=yesterday),
        f.envelope("shipment.status_changed", f.shipment("delivered")),
        f.envelope("order.created", f.order_created([f.item(f.uid(), f.uid(), 1, "1")])),
    ]
    await deliver(consumer, *events)
    since = datetime.now(UTC) - timedelta(days=3)
    report = await backfill(mongo, settings, projection="shipments", since=since)
    assert (report.applied, report.duplicate) == (4, 0)

    raws = [envelope_from_raw(r) async for r in mongo.db.raw_events.find({})]
    expected = fold(op for env in raws for op in apply_shipments(env, RollupContext()))
    stored = [d async for d in mongo.db.daily_metrics.find({"_id.metric": "shipments"})]
    assert len(stored) == len(expected.docs) == 2
    for doc in stored:
        want = expected.get("daily_metrics", doc["_id"])
        assert want is not None
        assert {k: int(v) for k, v in doc["statuses"].items()} == want["statuses"]
        assert doc["transitions"] == want["transitions"]

    again = await backfill(mongo, settings, projection="shipments", since=since)
    assert (again.applied, again.duplicate) == (0, 4)
    groups = await mongo.db.processed_events.distinct("_id.group")
    assert set(groups) == {"analytics", backfill_group("shipments")}


async def test_a_crashed_backfill_rerun_applies_each_event_once(
    consumer, mongo, settings, monkeypatch
):
    events = [f.envelope("shipment.status_changed", f.shipment("shipped")) for _ in range(3)]
    await deliver(consumer, *events)
    from app.v1.metrics import backfill_repo

    real = backfill_repo.rollup_handler
    calls = []

    def crashing_handler(mongo_, ctx, formula):
        inner = real(mongo_, ctx, formula)

        async def handle(env, session):
            calls.append(1)
            if len(calls) == 2:
                raise RuntimeError("crash mid-backfill")
            await inner(env, session)

        return handle

    monkeypatch.setattr(backfill_repo, "rollup_handler", crashing_handler)
    since = datetime.now(UTC) - timedelta(days=1)
    with pytest.raises(RuntimeError):
        await backfill(mongo, settings, projection="shipments", since=since)
    monkeypatch.setattr(backfill_repo, "rollup_handler", real)
    report = await backfill(mongo, settings, projection="shipments", since=since)
    assert (report.applied, report.duplicate) == (2, 1)
    doc = await mongo.db.daily_metrics.find_one({"_id.metric": "shipments"})
    assert doc["transitions"] == 3


async def test_an_unknown_projection_is_refused(mongo, settings) -> None:
    with pytest.raises(UnknownProjection, match="shipments"):
        await backfill(mongo, settings, projection="nope", since=datetime.now(UTC))


async def test_cli_indexes_and_backfill(consumer, mongo, settings) -> None:
    await deliver(consumer, f.envelope("shipment.status_changed", f.shipment("delivered")))
    result = await cli.run(["indexes"], settings, mongo)
    assert result == {"command": "indexes", "created": []}  # already present: idempotent
    since = (datetime.now(UTC) - timedelta(days=1)).date().isoformat()
    result = await cli.run(
        ["backfill", "--projection", "shipments", "--since", since], settings, mongo
    )
    assert result["applied"] == 1 and result["group"] == "analytics.backfill.shipments"
    future = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    result = await cli.run(
        ["backfill", "--projection", "shipments", "--since", since, "--until", future],
        settings,
        mongo,
    )
    assert (result["applied"], result["duplicate"]) == (0, 1)


async def test_cli_opens_its_own_client(settings, mongo) -> None:
    result = await cli.run(["indexes"], settings)
    assert result["command"] == "indexes"


def test_cli_rejects_a_bad_date() -> None:
    with pytest.raises(SystemExit):
        cli.parser().parse_args(["backfill", "--projection", "shipments", "--since", "yesterday"])
    args = cli.parser().parse_args(
        ["backfill", "--projection", "shipments", "--since", "2026-10-10T05:00:00"]
    )
    assert args.since == datetime(2026, 10, 10, 5, tzinfo=UTC)


def test_decimal_revenue_survives_reconciliation_types() -> None:
    from app.v1.metrics.reconcile_repo import normalise, to_bson

    doc = {"a": Decimal("1.50"), "b": 3, "c": True, "d": {"e": [Int64(2)]}, "updated_at": 1}
    assert normalise(to_bson(doc)) == {"a": Decimal("1.5"), "b": 3, "c": True, "d": {"e": [2]}}
