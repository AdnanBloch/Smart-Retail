"""Consumer fakes and the built ``EventConsumer``, shared by the consumer tests."""

import uuid
from dataclasses import dataclass, field
from typing import Any

import pytest
from sqlalchemy import func, select

from app.core.consumer import build
from app.v1.notifications.models import Notification
from smartretail_common.events import registry
from smartretail_common.events.consumer import EventConsumer, Outcome
from smartretail_common.events.schema_registry import InMemorySchemaRegistry
from smartretail_common.events.serde import AvroCodec
from smartretail_common.redis import create_redis

CONSUMED = (
    "identity.user.registered",
    "order.confirmed",
    "order.failed",
    "order.cancelled",
    "shipment.status_changed",
    "inventory.stockout",
    "payment.succeeded",
    "payment.failed",
)


@dataclass
class Msg:
    _topic: str
    _value: bytes

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 0

    def key(self) -> bytes:
        return b"k"

    def value(self) -> bytes:
        return self._value

    def headers(self) -> list[tuple[str, bytes]]:
        return []

    def error(self) -> None:
        return None


@dataclass
class Source:
    committed: list[Msg] = field(default_factory=list)

    def poll(self, timeout: float) -> None:
        return None

    def commit(self, message: Msg) -> None:
        self.committed.append(message)

    def close(self) -> None:
        return None


@dataclass
class Dlq:
    produced: list[dict[str, Any]] = field(default_factory=list)

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.produced.append({"topic": topic, **kwargs})

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


@dataclass
class Enqueued:
    ids: list[uuid.UUID] = field(default_factory=list)
    fail: bool = False

    async def __call__(self, notification_id: uuid.UUID) -> None:
        if self.fail:
            raise ConnectionError("rabbitmq down")
        self.ids.append(notification_id)


@pytest.fixture
def enqueued() -> Enqueued:
    return Enqueued()


@pytest.fixture
def dlq() -> Dlq:
    return Dlq()


@pytest.fixture
async def consumer(app, settings, enqueued, dlq):  # `app` truncates tables afterwards
    schemas = InMemorySchemaRegistry()
    for event_type in CONSUMED:
        spec = registry.catalogue().spec(event_type)
        schemas.register_now(spec.subject, registry.message_schema_str(event_type))
    redis = create_redis(settings.redis_url)
    built = build(settings, AvroCodec(schemas), dlq, redis=redis, enqueue=enqueued)
    built.sleep = _no_sleep
    yield built
    await redis.aclose()


async def _no_sleep(_: float) -> None:
    return None


async def deliver(consumer: EventConsumer, env) -> Outcome:
    msg = Msg(registry.catalogue().topic(env.event_type), await consumer.codec.encode(env))
    source = Source()
    outcome = await consumer.process(msg, source)
    assert source.committed == [msg]  # the offset is committed in every outcome
    return outcome


async def rows(app, *where) -> list[Notification]:
    async with app.state.db.sessionmaker() as session:
        return list(
            await session.scalars(select(Notification).where(*where).order_by(Notification.seq))
        )


async def count(app, model, *where) -> int:
    async with app.state.db.sessionmaker() as session:
        return int(await session.scalar(select(func.count()).select_from(model).where(*where)) or 0)
