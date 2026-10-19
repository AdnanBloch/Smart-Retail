"""``analytics_db`` indexes and TTLs (RFC 0008 §3.1.3): idempotent creation and
a TTL change applied with ``collMod``, never a drop."""

from app.core.db import require_replica_set
from app.v1.metrics.enums import Collection
from app.v1.metrics.models import DAY_SECONDS, ensure_indexes


async def test_indexes_and_ttls_exist_and_creation_is_idempotent(mongo, settings) -> None:
    raw = await mongo.db.raw_events.index_information()
    assert raw["event_id"]["unique"] is True
    assert raw["occurred_at_ttl"]["expireAfterSeconds"] == 7 * DAY_SECONDS
    assert "type_occurred_at" in raw
    processed = await mongo.db.processed_events.index_information()
    assert processed["processed_at_ttl"]["expireAfterSeconds"] == 30 * DAY_SECONDS
    daily = await mongo.db.daily_metrics.index_information()
    assert {"date_metric", "merchant_metric_date"} <= set(daily)
    stats = await mongo.db.product_stats.index_information()
    assert {"merchant_quantity", "merchant_revenue"} <= set(stats)
    assert set(Collection) <= set(await mongo.db.list_collection_names())
    assert await ensure_indexes(mongo, settings) == []


async def test_a_ttl_change_is_a_collmod(mongo, settings) -> None:
    shorter = settings.model_copy(update={"raw_events_ttl_days": 3, "reconcile_lookback_days": 2})
    assert await ensure_indexes(mongo, shorter) == ["occurred_at_ttl"]
    raw = await mongo.db.raw_events.index_information()
    assert raw["occurred_at_ttl"]["expireAfterSeconds"] == 3 * DAY_SECONDS


async def test_the_test_server_is_a_replica_set(mongo) -> None:
    await require_replica_set(mongo)
    assert await mongo.ping() is True
