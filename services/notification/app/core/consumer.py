"""The notification consumer process: ``python -m app.core.consumer`` (RFC 0007 §3.1.2.4).

One process, consumer group ``notification``, every consumed topic (Q2).
Dedupe, retry, DLQ, outage waits (NTF-14) and manual offset commit are the
base's (RFC 0001 §3.1.1.1); the handlers live with the entity whose rows they
write (NTF-D25), and the after-commit fan-out and enqueue are
``notifications/consumers/after_commit.py``.

Fan-out is mandatory, not a fallback: the consumer runs as its own process,
so the replica holding a stream is never assumed to be the one consuming
(NTF-D31).
"""

import asyncio
from typing import TYPE_CHECKING

from app.core.config import Settings, get_settings
from app.core.db import create_database
from app.core.logger import init_logger
from app.v1.notifications.consumers import payment, record
from app.v1.notifications.consumers.after_commit import Enqueue, after_commit, celery_enqueue
from smartretail_common.events.consumer import EventConsumer, Handler, serve
from smartretail_common.events.producer import Publisher
from smartretail_common.events.serde import AvroCodec
from smartretail_common.redis import create_redis

if TYPE_CHECKING:
    from redis.asyncio import Redis

GROUP = "notification"


def build(
    settings: Settings,
    codec: AvroCodec,
    dlq: Publisher,
    *,
    redis: "Redis | None" = None,
    enqueue: Enqueue = celery_enqueue,
) -> EventConsumer:
    db = create_database(settings)
    redis = redis if redis is not None else create_redis(settings.redis_url)
    handlers: dict[str, Handler] = dict.fromkeys(record.EVENT_TYPES, record.handler())
    handlers.update(dict.fromkeys(payment.EVENT_TYPES, payment.handler()))
    return EventConsumer(
        group=GROUP,
        handlers=handlers,
        sessionmaker=db.sessionmaker,
        codec=codec,
        dlq=dlq,
        max_attempts=settings.consumer_max_attempts,
        after_commit=after_commit(db.sessionmaker, redis, enqueue),
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
