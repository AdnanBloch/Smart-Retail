"""Integration fixtures: a real single-node MongoDB replica set (``make test-mongo``),
one throwaway database per test."""

import os
import uuid
from dataclasses import dataclass, field
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from app.core.config import Settings
from app.core.db import Mongo, create_mongo
from app.v1.metrics.consumers import build
from app.v1.metrics.models import prepare_store
from main import create_application
from smartretail_common.auth import JwtVerifier
from smartretail_common.events import registry
from smartretail_common.events.consumer import EventConsumer, Outcome
from smartretail_common.events.envelope import Envelope
from smartretail_common.events.schema_registry import InMemorySchemaRegistry
from smartretail_common.events.serde import AvroCodec

DEFAULT_TEST_MONGO_URI = "mongodb://127.0.0.1:57017/?replicaSet=rs0&directConnection=true"
# Nothing listens here: the "Mongo is down" address.
DEAD_MONGO_URI = "mongodb://127.0.0.1:1/?directConnection=true"


@pytest.fixture(scope="session")
def mongo_uri() -> str:
    uri = os.environ.get("TEST_MONGO_URI", DEFAULT_TEST_MONGO_URI)
    try:
        with MongoClient(uri, serverSelectionTimeoutMS=1500) as client:
            client.admin.command("ping")
    except PyMongoError:
        pytest.skip("test MongoDB not reachable; run `make test-mongo`")
    return uri


@pytest.fixture
def settings(mongo_uri) -> Settings:
    return Settings(
        analytics_mongo_uri=mongo_uri,
        analytics_mongo_db=f"analytics_test_{uuid.uuid4().hex[:12]}",
        jwt_public_key_path=None,
    )


@pytest.fixture
async def mongo(settings):
    store = create_mongo(settings)
    await prepare_store(store, settings)
    yield store
    await store.client.drop_database(settings.analytics_mongo_db)
    await store.close()


@pytest.fixture
async def dead_mongo(settings):
    store = create_mongo(
        settings.model_copy(update={"analytics_mongo_timeout_ms": 200}), uri=DEAD_MONGO_URI
    )
    yield store
    await store.close()


@pytest.fixture
async def app(settings, mongo, keypair):
    verifier = JwtVerifier(
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        static_keys={None: keypair.public_key},
    )
    return create_application(settings, verifier=verifier, mongo=mongo)


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://analytics") as c:
        yield c


# --- consumer plumbing -----------------------------------------------------------------


@dataclass
class Msg:
    _topic: str
    _value: bytes | None
    _headers: list[tuple[str, bytes]] = field(default_factory=list)
    _offset: int = 0

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes:
        return b"k"

    def value(self) -> bytes | None:
        return self._value

    def headers(self) -> list[tuple[str, bytes]]:
        return self._headers

    def error(self) -> None:
        return None


@dataclass
class Source:
    committed: list[Any] = field(default_factory=list)
    queue: list[Any] = field(default_factory=list)
    stop: Any = None

    def poll(self, timeout: float) -> Any:
        if self.queue:
            return self.queue.pop(0)
        if self.stop is not None:
            self.stop.set()
        return None

    def commit(self, message: Any) -> None:
        self.committed.append(message)

    def close(self) -> None:
        return None


@dataclass
class RecordingDlq:
    produced: list[Msg] = field(default_factory=list)

    def produce(self, topic: str, *, value: Any, key: Any, headers: Any, on_delivery: Any) -> None:
        self.produced.append(Msg(topic, value, list(headers), len(self.produced)))

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


async def no_sleep(_: float) -> None:
    return None


def codec() -> AvroCodec:
    schemas = InMemorySchemaRegistry()
    for spec in registry.catalogue().events:
        if registry.has_schema(spec.event_type):
            schemas.register_now(spec.subject, registry.message_schema_str(spec.event_type))
    return AvroCodec(schemas)


@pytest.fixture
def dlq() -> RecordingDlq:
    return RecordingDlq()


@pytest.fixture
def consumer(settings, mongo, dlq) -> EventConsumer:
    built = build(settings, mongo, codec(), dlq)
    built.sleep = no_sleep
    return built


async def message(consumer: EventConsumer, env: Envelope) -> Msg:
    topic = registry.catalogue().topic(env.event_type)
    return Msg(topic, await consumer.codec.encode(env))


async def deliver(consumer: EventConsumer, *envelopes: Envelope) -> list[Outcome]:
    outcomes = []
    for env in envelopes:
        msg = await message(consumer, env)
        source = Source()
        outcomes.append(await consumer.process(msg, source))
        assert source.committed == [msg]  # the offset is committed in every outcome
    return outcomes


async def snapshot(mongo: Mongo) -> dict[str, list[dict[str, Any]]]:
    out = {}
    for name in sorted(await mongo.db.list_collection_names()):
        out[name] = [d async for d in mongo.db[name].find({})]
        out[name].sort(key=repr)
    return out
