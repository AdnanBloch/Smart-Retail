"""The analytics consumer: group ``analytics``, every catalogue topic with a schema.

Each handler is one formula from ``metrics.rollups`` executed in the
``MongoDedupeStore`` transaction; the base (``EventConsumer``) supplies decode,
retry, DLQ and the manual offset commit (RFC 0001 §3.1.1.1). The process
entry point is ``python -m app.v1.metrics.consumers`` (``__main__``).
"""

from app.core.config import Settings
from app.core.db import Mongo, Session
from app.v1.metrics.consumers.dedupe import MongoDedupeStore
from app.v1.metrics.models import execute
from app.v1.metrics.rollups import Projection, RollupContext, apply
from smartretail_common.events.consumer import EventConsumer, Handler
from smartretail_common.events.envelope import Envelope
from smartretail_common.events.producer import Publisher
from smartretail_common.events.registry import catalogue, has_schema
from smartretail_common.events.serde import AvroCodec


def subscribed_event_types() -> list[str]:
    """Every catalogue event with a registered schema (one without has no producer)."""
    return [spec.event_type for spec in catalogue().events if has_schema(spec.event_type)]


def rollup_handler(mongo: Mongo, ctx: RollupContext, projection: Projection = apply) -> Handler:
    async def handle(envelope: Envelope, session: Session) -> None:
        await execute(mongo, projection(envelope, ctx), session)

    return handle


def context(settings: Settings) -> RollupContext:
    return RollupContext(reporting_currency=settings.analytics_reporting_currency)


def build(settings: Settings, mongo: Mongo, codec: AvroCodec, dlq: Publisher) -> EventConsumer:
    handler = rollup_handler(mongo, context(settings))
    return EventConsumer(
        group=settings.analytics_consumer_group,
        handlers=dict.fromkeys(subscribed_event_types(), handler),
        codec=codec,
        dlq=dlq,
        store=MongoDedupeStore(mongo, processed_ttl_days=settings.processed_events_ttl_days),
    )
