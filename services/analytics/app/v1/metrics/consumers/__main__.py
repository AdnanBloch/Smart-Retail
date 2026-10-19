"""``python -m app.v1.metrics.consumers``: the consumer and the DLQ observer in one process.

Both loops run under ``run_until_signal``: either failing stops the other,
so the container restarts as a whole. The store is checked (replica set,
ANL-1) and indexed before anything subscribes (RFC 0008 §6.3).
"""

import asyncio

from app.core.config import Settings, get_settings
from app.core.db import create_mongo
from app.core.logger import init_logger
from app.v1.metrics.consumers import build
from app.v1.metrics.consumers.dlq_observer import DlqObserver
from app.v1.metrics.models import prepare_store
from smartretail_common.events.consumer import (
    Runner,
    consumer_runner,
    kafka_source,
    run_until_signal,
)
from smartretail_common.events.producer import kafka_producer
from smartretail_common.events.schema_registry import HttpSchemaRegistry
from smartretail_common.events.serde import AvroCodec


def observer_runner(observer: DlqObserver, settings: Settings) -> Runner:  # pragma: no cover
    async def run(stop: asyncio.Event) -> None:
        # confluent-kafka treats a subscription starting with ^ as a regex.
        source = kafka_source(
            settings.kafka_bootstrap_servers, observer.group, [settings.analytics_dlq_pattern]
        )
        try:
            await observer.run(source, stop)
        finally:
            source.close()

    return run


async def serve(settings: Settings) -> None:  # pragma: no cover - process wiring
    mongo = create_mongo(settings)
    try:
        await prepare_store(mongo, settings)
        codec = AvroCodec(HttpSchemaRegistry(settings.schema_registry_url))
        consumer = build(
            settings,
            mongo,
            codec,
            kafka_producer(settings.kafka_bootstrap_servers, "analytics-dlq"),
        )
        observer = DlqObserver(mongo, group=settings.analytics_dlq_group)
        await run_until_signal(
            consumer_runner(consumer, settings.kafka_bootstrap_servers),
            observer_runner(observer, settings),
        )
    finally:
        await mongo.close()


def main() -> None:  # pragma: no cover - process wiring, exercised by the e2e suite
    settings = get_settings()
    init_logger(settings)
    asyncio.run(serve(settings))


if __name__ == "__main__":  # pragma: no cover
    main()
