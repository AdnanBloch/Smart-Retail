"""``analytics_db``: collections, indexes and TTLs (RFC 0008 §3.1.3).

MongoDB has no Alembic. Indexes are declared here and created by the
idempotent ``ensure_indexes()`` at start-up and by ``python -m
app.v1.metrics.cli indexes``; a TTL change is a ``collMod``, never a drop.
"""

from dataclasses import dataclass, field
from typing import Any

from pymongo import ASCENDING, DESCENDING
from pymongo.asynchronous.collection import AsyncCollection

from app.core.config import Settings
from app.core.db import Document, Mongo, Session, mongo_errors, require_replica_set
from app.v1.metrics.enums import Collection
from app.v1.metrics.rollups import Op

DAY_SECONDS = 86_400


@dataclass(frozen=True)
class IndexSpec:
    collection: str
    name: str
    keys: list[tuple[str, int]]
    options: dict[str, Any] = field(default_factory=dict)


def index_specs(settings: Settings) -> list[IndexSpec]:
    return [
        IndexSpec(
            Collection.DAILY_METRICS,
            "date_metric",
            [("_id.date", ASCENDING), ("_id.metric", ASCENDING)],
        ),
        IndexSpec(
            Collection.DAILY_METRICS,
            "merchant_metric_date",
            [("_id.merchant_id", ASCENDING), ("_id.metric", ASCENDING), ("_id.date", ASCENDING)],
        ),
        IndexSpec(
            Collection.PRODUCT_STATS,
            "merchant_quantity",
            [("merchant_id", ASCENDING), ("quantity_sold", DESCENDING)],
        ),
        IndexSpec(
            Collection.PRODUCT_STATS,
            "merchant_revenue",
            [("merchant_id", ASCENDING), ("revenue", DESCENDING)],
        ),
        IndexSpec(Collection.FAILURE_STATS, "date", [("_id.date", ASCENDING)]),
        IndexSpec(Collection.RAW_EVENTS, "event_id", [("event_id", ASCENDING)], {"unique": True}),
        IndexSpec(
            Collection.RAW_EVENTS,
            "occurred_at_ttl",
            [("occurred_at", ASCENDING)],
            {"expireAfterSeconds": settings.raw_events_ttl_days * DAY_SECONDS},
        ),
        IndexSpec(
            Collection.RAW_EVENTS,
            "type_occurred_at",
            [("event_type", ASCENDING), ("occurred_at", ASCENDING)],
        ),
        IndexSpec(
            Collection.PROCESSED_EVENTS,
            "processed_at_ttl",
            [("processed_at", ASCENDING)],
            {"expireAfterSeconds": settings.processed_events_ttl_days * DAY_SECONDS},
        ),
    ]


def collection(mongo: Mongo, name: str) -> AsyncCollection[Document]:
    return mongo.db[name]


async def ensure_indexes(mongo: Mongo, settings: Settings) -> list[str]:
    """Creates every collection and index; adjusts a changed TTL with ``collMod``.
    Safe to run any number of times. Returns the index names it touched."""
    touched: list[str] = []
    async with mongo_errors():
        existing_collections = set(await mongo.db.list_collection_names())
        for name in Collection:
            # Collections must exist before a transaction writes to them on
            # older servers, and an explicit create documents the store.
            if name not in existing_collections:
                await mongo.db.create_collection(name)
        for spec in index_specs(settings):
            coll = collection(mongo, spec.collection)
            info = await coll.index_information()
            ttl = spec.options.get("expireAfterSeconds")
            current = info.get(spec.name)
            if current is not None and ttl is not None and current.get("expireAfterSeconds") != ttl:
                await mongo.db.command(
                    "collMod",
                    spec.collection,
                    index={"name": spec.name, "expireAfterSeconds": ttl},
                )
                touched.append(spec.name)
                continue
            if current is None:
                await coll.create_index(spec.keys, name=spec.name, **spec.options)
                touched.append(spec.name)
    return touched


async def prepare_store(mongo: Mongo, settings: Settings) -> None:
    """Start-up for every process: refuse a stand-alone server (ANL-1), then
    create the indexes before any read or subscription (RFC 0008 §6.3)."""
    await require_replica_set(mongo)
    await ensure_indexes(mongo, settings)


async def execute(mongo: Mongo, ops: list[Op], session: Session) -> None:
    """Runs rollup operations inside the caller's transaction."""
    for op in ops:
        await collection(mongo, op.collection).update_one(
            op.filter, op.update, upsert=op.upsert, session=session
        )
