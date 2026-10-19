"""The inventory consumer process: ``python -m app.core.consumer`` (RFC 0006 §3.1.2.4).

One process, consumer group ``inventory``, both topics; each handler lives
with the entity whose state it changes (ARC-D27) and this module, used by
two entities, only wires them to the platform's ``EventConsumer`` (dedupe,
retry, DLQ and manual offset commit are the base's, RFC 0001 §3.1.1.1).
"""

import asyncio

from app.core.config import Settings, get_settings
from app.core.db import create_database
from app.core.logger import init_logger
from app.v1.reservations.consumers import order_cancelled
from app.v1.stock.consumers import catalog_product_published
from smartretail_common.events.consumer import EventConsumer, serve
from smartretail_common.events.producer import Publisher
from smartretail_common.events.serde import AvroCodec

GROUP = "inventory"


def build(settings: Settings, codec: AvroCodec, dlq: Publisher) -> EventConsumer:
    db = create_database(settings)
    return EventConsumer(
        group=GROUP,
        handlers={
            "catalog.product.published": catalog_product_published.handler(settings),
            "order.cancelled": order_cancelled.handler(settings),
        },
        sessionmaker=db.sessionmaker,
        codec=codec,
        dlq=dlq,
    )


def main() -> None:  # pragma: no cover - process wiring, exercised by the e2e suite
    settings = get_settings()
    init_logger(settings)
    asyncio.run(
        serve(
            lambda codec, dlq: build(settings, codec, dlq),
            bootstrap_servers=settings.kafka_bootstrap_servers,
            schema_registry_url=settings.schema_registry_url,
        )
    )


if __name__ == "__main__":  # pragma: no cover
    main()
