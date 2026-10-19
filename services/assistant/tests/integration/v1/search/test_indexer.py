"""The indexer end to end: Avro message → ``EventConsumer`` (dedupe, transaction,
offset commit) → the product-events handlers → assistant_db (§3.1.1.4)."""

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import func, select, text

from app.v1.search.consumers.worker import GROUP, TOPICS, build, check_index_model
from app.v1.search.enums import IndexStatus
from app.v1.search.models import EmbeddingCacheEntry, IndexState, ProductChunk
from app.v1.search.reindex import REINDEX_LOCK_KEY, ReindexScope, reindex_running, run_reindex
from smartretail_common.events import registry
from smartretail_common.events.consumer import EventConsumer, Outcome
from smartretail_common.events.dedupe import ProcessedEvent
from smartretail_common.events.envelope import Envelope
from smartretail_common.events.schema_registry import InMemorySchemaRegistry
from smartretail_common.events.serde import AvroCodec
from tests.integration.conftest import product_payload

PUBLISHED, UPDATED, UNPUBLISHED = TOPICS


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


class NoDlq:
    def produce(self, *args: Any, **kwargs: Any) -> None:
        raise AssertionError("nothing should be dead-lettered")

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


class Stop(Exception):
    pass


@pytest.fixture
async def consumer(app, settings, embeddings) -> EventConsumer:
    schemas = InMemorySchemaRegistry()
    for event_type in TOPICS:
        spec = registry.catalogue().spec(event_type)
        schemas.register_now(spec.subject, registry.message_schema_str(event_type))
    built = build(settings, AvroCodec(schemas), NoDlq(), embeddings)
    yield built


async def deliver(consumer: EventConsumer, event_type: str, payload: dict, event_id=None):
    envelope = Envelope(
        event_id=event_id or uuid.uuid4(),
        event_type=event_type,
        event_version=1,
        occurred_at=datetime.now(UTC),
        producer="catalog",
        correlation_id="corr-1",
        payload=payload,
    )
    msg = Msg(registry.catalogue().topic(event_type), await consumer.codec.encode(envelope))
    source = Source()
    outcome = await consumer.process(msg, source)
    assert source.committed == [msg]
    return outcome, envelope.event_id


def unpublished(product_id: str, merchant_id: str, version: int) -> dict:
    return {
        "product_id": product_id,
        "merchant_id": merchant_id,
        "version": version,
        "previous_status": "published",
        "new_status": "inactive",
        "reason": "merchant",
        "variant_ids": [],
        "unpublished_at": "2026-10-15T00:00:00Z",
    }


async def state_of(app, product_id: str) -> IndexState | None:
    async with app.state.db.sessionmaker() as s:
        return await s.get(IndexState, uuid.UUID(product_id))


async def chunk_count(app, product_id: str) -> int:
    async with app.state.db.sessionmaker() as s:
        return int(
            await s.scalar(
                select(func.count())
                .select_from(ProductChunk)
                .where(ProductChunk.product_id == uuid.UUID(product_id))
            )
        )


DESCRIPTION = "Lightweight breathable running shoes. Mesh upper keeps feet cool on long runs."


async def test_publish_indexes_and_replay_costs_nothing(app, consumer, embeddings, callers):
    pid, mid = str(uuid.uuid4()), callers.merchant_a_id
    payload = product_payload(
        pid, mid, name="Acme Runner", description=DESCRIPTION, attributes={"brand": "Acme"}
    )
    outcome, event_id = await deliver(consumer, PUBLISHED, payload)
    assert outcome == Outcome.HANDLED
    state = await state_of(app, pid)
    assert state.status == IndexStatus.INDEXED and state.catalog_version == 1
    assert state.embedding_model == "hashing-v1" and state.merchant_id == uuid.UUID(mid)
    assert state.source_document["search_document"]["name"] == "Acme Runner"
    count = await chunk_count(app, pid)
    assert count == state.chunk_count >= 3
    assert embeddings.calls == 1 and embeddings.texts == count

    # Same event redelivered: dedupe, no effect.
    assert (await deliver(consumer, PUBLISHED, payload, event_id))[0] == Outcome.DUPLICATE
    # Same content, new event (a replay of the compacted topic): zero embedding calls.
    assert (await deliver(consumer, UPDATED, payload))[0] == Outcome.HANDLED
    assert embeddings.calls == 1
    async with app.state.db.sessionmaker() as s:
        hits = await s.scalar(select(func.sum(EmbeddingCacheEntry.hit_count)))
        claims = await s.scalar(
            select(func.count())
            .select_from(ProcessedEvent)
            .where(ProcessedEvent.consumer_group == GROUP)
        )
    assert hits == count and claims == 2


async def test_edit_reembeds_only_changed_chunks_and_prunes(app, consumer, embeddings, callers):
    pid, mid = str(uuid.uuid4()), callers.merchant_a_id
    await deliver(
        consumer,
        PUBLISHED,
        product_payload(pid, mid, name="Kettle", description="Boils water fast."),
    )
    before = embeddings.texts
    async with app.state.db.sessionmaker() as s:
        old = set(
            (
                await s.scalars(
                    select(ProductChunk.content).where(ProductChunk.product_id == uuid.UUID(pid))
                )
            ).all()
        )
    edited = product_payload(
        pid, mid, name="Kettle", version=2, description="Boils water quietly in two minutes."
    )
    await deliver(consumer, UPDATED, edited)
    assert embeddings.texts - before == 1  # only the description chunk
    async with app.state.db.sessionmaker() as s:
        new = set(
            (
                await s.scalars(
                    select(ProductChunk.content).where(ProductChunk.product_id == uuid.UUID(pid))
                )
            ).all()
        )
    assert not any("Boils water fast." in c for c in new)
    assert len(old & new) == len(new) - 1
    assert (await state_of(app, pid)).catalog_version == 2


async def test_older_version_is_skipped(app, consumer, callers):
    pid, mid = str(uuid.uuid4()), callers.merchant_a_id
    await deliver(consumer, UPDATED, product_payload(pid, mid, name="New name", version=3))
    await deliver(consumer, PUBLISHED, product_payload(pid, mid, name="Old name", version=2))
    async with app.state.db.sessionmaker() as s:
        contents = (
            await s.scalars(
                select(ProductChunk.content).where(ProductChunk.product_id == uuid.UUID(pid))
            )
        ).all()
    assert all("New name" in c for c in contents)
    assert (await state_of(app, pid)).catalog_version == 3


async def test_unpublish_and_non_published_update_remove(app, consumer, callers):
    pid, mid = str(uuid.uuid4()), callers.merchant_a_id
    await deliver(consumer, PUBLISHED, product_payload(pid, mid, name="Lamp"))
    await deliver(consumer, UNPUBLISHED, unpublished(pid, mid, 2))
    state = await state_of(app, pid)
    assert state.status == IndexStatus.REMOVED and await chunk_count(app, pid) == 0
    # A late, older publish is stale and does not resurrect the product.
    await deliver(consumer, PUBLISHED, product_payload(pid, mid, name="Lamp", version=1))
    assert await chunk_count(app, pid) == 0
    # Republished with a newer version: back in.
    await deliver(consumer, PUBLISHED, product_payload(pid, mid, name="Lamp", version=3))
    assert (await state_of(app, pid)).status == IndexStatus.INDEXED
    # An update to a non-published status removes (AST-5).
    await deliver(
        consumer, UPDATED, product_payload(pid, mid, name="Lamp", version=4, status="archived")
    )
    assert (await state_of(app, pid)).status == IndexStatus.REMOVED
    assert await chunk_count(app, pid) == 0
    # Unpublish of a product never indexed records a tombstone.
    other = str(uuid.uuid4())
    await deliver(consumer, UNPUBLISHED, unpublished(other, mid, 5))
    assert (await state_of(app, other)).status == IndexStatus.REMOVED
    # An older unpublish after a newer publish is stale.
    await deliver(consumer, PUBLISHED, product_payload(other, mid, name="Desk", version=6))
    await deliver(consumer, UNPUBLISHED, unpublished(other, mid, 5))
    assert (await state_of(app, other)).status == IndexStatus.INDEXED


async def test_embedding_outage_waits_instead_of_dead_lettering(app, consumer, embeddings, callers):
    pid = str(uuid.uuid4())
    embeddings.down = True
    waits: list[float] = []

    async def sleep(delay: float) -> None:
        waits.append(delay)
        if len(waits) == 2:
            embeddings.down = False

    consumer.sleep = sleep
    outcome, _ = await deliver(
        consumer, PUBLISHED, product_payload(pid, callers.merchant_a_id, name="Fan")
    )
    assert outcome == Outcome.HANDLED and waits == [0.5, 1.0]
    assert (await state_of(app, pid)).status == IndexStatus.INDEXED


async def test_reindex_rebuilds_from_source_documents(app, consumer, embeddings, settings, callers):
    ids = [str(uuid.uuid4()) for _ in range(3)]
    for i, pid in enumerate(ids):
        await deliver(
            consumer,
            PUBLISHED,
            product_payload(
                pid,
                callers.merchant_a_id if i else callers.merchant_b_id,
                name=f"Item {i}",
                description=f"Description number {i}.",
            ),
        )
    calls = embeddings.calls
    async with app.state.db.sessionmaker() as s, s.begin():
        await s.execute(ProductChunk.__table__.delete())
    report = await run_reindex(app.state.db, embeddings, settings, ReindexScope(), batch_size=2)
    assert report.indexed == 3 and report.failed == 0
    assert embeddings.calls == calls  # everything came from the cache
    assert all([await chunk_count(app, pid) > 0 for pid in ids])
    scoped = await run_reindex(
        app.state.db,
        embeddings,
        settings,
        ReindexScope(merchant_id=uuid.UUID(callers.merchant_b_id)),
    )
    assert scoped.indexed == 1
    by_id = await run_reindex(
        app.state.db, embeddings, settings, ReindexScope(product_ids=(uuid.UUID(ids[1]),))
    )
    assert by_id.indexed == 1
    scope = ReindexScope(product_ids=(uuid.UUID(ids[1]),), merchant_id=uuid.UUID(ids[0]))
    assert ReindexScope.from_dict(scope.as_dict()) == scope


async def test_full_reindex_is_exclusive(app, embeddings, settings):
    async with app.state.db.engine.connect() as holder:
        await holder.execute(text("SELECT pg_advisory_lock(:k)"), {"k": REINDEX_LOCK_KEY})
        async with app.state.db.sessionmaker() as s:
            assert await reindex_running(s)
        report = await run_reindex(app.state.db, embeddings, settings, ReindexScope())
        assert report.running_elsewhere
        await holder.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": REINDEX_LOCK_KEY})
    async with app.state.db.sessionmaker() as s:
        assert not await reindex_running(s)


async def test_reindex_failure_marks_failed_and_model_change_marks_pending(
    app, consumer, embeddings, settings, callers
):
    good, bad = str(uuid.uuid4()), str(uuid.uuid4())
    await deliver(consumer, PUBLISHED, product_payload(good, callers.merchant_a_id, name="Good"))
    await deliver(consumer, PUBLISHED, product_payload(bad, callers.merchant_a_id, name="Bad"))
    async with app.state.db.sessionmaker() as s, s.begin():
        state = await s.get(IndexState, uuid.UUID(bad))
        state.source_document = {"product_id": "not-a-uuid", "status": "published"}
    report = await run_reindex(app.state.db, embeddings, settings, ReindexScope())
    assert report.failed == 1 and report.indexed == 1 and bad in report.errors
    failed = await state_of(app, bad)
    assert failed.status == IndexStatus.FAILED and failed.error
    other = settings.model_copy(update={"embedding_model": "another-model"})
    assert await check_index_model(other) >= 1
    assert (await state_of(app, good)).status == IndexStatus.PENDING
