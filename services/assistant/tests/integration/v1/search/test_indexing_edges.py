"""Indexing, search-service and reindex edge cases, and the races on one
product's ``index_state`` row (§3.1.1.4, AST-3, AST-5, AST-7, AST-D36).

The races drive real Postgres transactions concurrently: two first-time
indexes of one product, an event landing while a reindex is between reading
the stored document and writing chunks, and an unpublish landing after a
reindex batch was listed.
"""

import asyncio
import logging
import uuid

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError

import app.v1.search.reindex as reindex_module
from app.core.errors import EmbeddingUnavailable
from app.v1.search.enums import IndexOutcome, IndexStatus, SearchMode
from app.v1.search.indexing_repo import IndexingRepo
from app.v1.search.models import EmbeddingCacheEntry, IndexState, ProductChunk, QueryLog
from app.v1.search.reindex import ReindexScope, run_reindex
from app.v1.search.repo import SearchFilters
from app.v1.search.service import SearchService
from tests.integration.conftest import index, product_payload, seed_catalogue


async def state_of(app, product_id: str) -> IndexState | None:
    async with app.state.db.sessionmaker() as s:
        return await s.get(IndexState, uuid.UUID(product_id))


async def chunks_of(app, product_id: str) -> list[ProductChunk]:
    async with app.state.db.sessionmaker() as s:
        return list(
            (
                await s.scalars(
                    select(ProductChunk).where(ProductChunk.product_id == uuid.UUID(product_id))
                )
            ).all()
        )


async def assert_consistent(app, product_id: str) -> list[ProductChunk]:
    """No duplicate chunk per content hash, and ``chunk_count`` matches the rows."""
    chunks = await chunks_of(app, product_id)
    hashes = [c.content_hash for c in chunks]
    assert len(hashes) == len(set(hashes))
    assert (await state_of(app, product_id)).chunk_count == len(chunks)
    return chunks


def mentions(chunks: list[ProductChunk], text: str) -> bool:
    return any(text in c.content for c in chunks)


# --- indexing edge cases ------------------------------------------------------------


async def test_an_unparseable_price_is_indexed_without_a_price(app, callers):
    pid = str(uuid.uuid4())
    payload = product_payload(pid, callers.merchant_a_id, name="Odd Price", price="call us")
    result = await index(app, payload)
    assert result.outcome == IndexOutcome.INDEXED and result.chunks > 0
    chunks = await chunks_of(app, pid)
    assert chunks and all(c.price is None for c in chunks)


async def test_the_chunk_bound_truncates_and_warns(app, settings, embeddings, callers, caplog):
    pid = str(uuid.uuid4())
    long_description = " ".join(f"word{i}." for i in range(8000))
    payload = product_payload(pid, callers.merchant_a_id, name="Epic", description=long_description)
    with caplog.at_level(logging.WARNING, logger="assistant.indexer"):
        result = await index(app, payload)
    assert result.truncated and result.chunks == settings.chunk_max_per_product
    assert any(r.message == "chunk bound hit; description truncated" for r in caplog.records)
    assert len(await assert_consistent(app, pid)) == settings.chunk_max_per_product


async def test_mark_removed_is_false_for_unknown_and_already_removed(app, embeddings, settings):
    pid = str(uuid.uuid4())
    await index(app, product_payload(pid, str(uuid.uuid4()), name="Gone Soon"))

    async def mark(product_id: str) -> bool:
        async with app.state.db.sessionmaker() as s, s.begin():
            repo = IndexingRepo(s, embeddings, settings)
            return await repo.mark_removed(uuid.UUID(product_id), "catalog: not visible")

    assert await mark(str(uuid.uuid4())) is False
    assert await mark(pid) is True
    state = await state_of(app, pid)
    assert state.status == IndexStatus.REMOVED and state.error == "catalog: not visible"
    assert await chunks_of(app, pid) == []
    assert await mark(pid) is False


# --- the search service outside the routes ---------------------------------------------


async def test_service_search_without_logging_and_similar_without_hydration(
    app, upstreams, callers, embeddings, settings
):
    cat = await seed_catalogue(app, upstreams, callers)
    async with app.state.db.sessionmaker() as s:
        service = SearchService(s, embeddings, app.state.hydrator, settings)
        response = await service.search(
            "running shoes",
            filters=SearchFilters(),
            mode=SearchMode.HYBRID,
            limit=5,
            user_id=None,
            log=False,
        )
        assert response.results[0].hit.product_id == uuid.UUID(cat.shoes)
        assert str(response.results[0].price) == "89.00"

        calls = upstreams.catalog_calls
        similar = await service.similar(uuid.UUID(cat.shoes), 5, hydrate=False)
        assert similar and uuid.UUID(cat.shoes) not in [r.hit.product_id for r in similar]
        assert all(not r.live.hydrated and r.price is None for r in similar)
        assert upstreams.catalog_calls == calls  # no live reads asked for
        assert await service.similar(uuid.uuid4(), 5) is None
    async with app.state.db.sessionmaker() as s:
        assert await s.scalar(select(func.count()).select_from(QueryLog)) == 0


# --- reindex edge cases -------------------------------------------------------------------


async def test_reindex_failure_from_pending_and_again_from_failed(app, embeddings, settings):
    """A product the model check marked ``pending`` fails straight to ``failed``;
    rerunning over a ``failed`` product counts it again without an illegal move."""
    pid = str(uuid.uuid4())
    await index(app, product_payload(pid, str(uuid.uuid4()), name="Broken"))
    async with app.state.db.sessionmaker() as s, s.begin():
        state = await s.get(IndexState, uuid.UUID(pid))
        state.embedding_model = "an-older-model"  # the reindex marks it pending first
        state.source_document = {"product_id": "not-a-uuid", "status": "published"}
    scope = ReindexScope(product_ids=(uuid.UUID(pid),))

    first = await run_reindex(app.state.db, embeddings, settings, scope)
    assert (first.failed, first.indexed) == (1, 0) and pid in first.errors
    assert (await state_of(app, pid)).status == IndexStatus.FAILED

    again = await run_reindex(app.state.db, embeddings, settings, scope)
    assert again.failed == 1
    state = await state_of(app, pid)
    assert state.status == IndexStatus.FAILED and state.error


async def test_reindex_of_an_unchanged_product_embeds_nothing(app, embeddings, settings):
    pid = str(uuid.uuid4())
    await index(app, product_payload(pid, str(uuid.uuid4()), name="Stable", description="Same."))
    before = (await chunks_of(app, pid), embeddings.calls)
    report = await run_reindex(
        app.state.db, embeddings, settings, ReindexScope(product_ids=(uuid.UUID(pid),))
    )
    assert report.indexed == 1 and embeddings.calls == before[1]
    assert {c.content_hash for c in await assert_consistent(app, pid)} == {
        c.content_hash for c in before[0]
    }


# --- races on one product ----------------------------------------------------------------


async def test_concurrent_reindexes_of_one_product_leave_one_set_of_chunks(
    app, embeddings, settings
):
    pid = str(uuid.uuid4())
    await index(app, product_payload(pid, str(uuid.uuid4()), name="Racer", description="Fast."))
    async with app.state.db.sessionmaker() as s, s.begin():
        await s.execute(ProductChunk.__table__.delete())
    scope = ReindexScope(product_ids=(uuid.UUID(pid),))
    reports = await asyncio.gather(
        *(run_reindex(app.state.db, embeddings, settings, scope) for _ in range(4))
    )
    assert [r.indexed for r in reports] == [1] * 4
    assert all(r.failed == 0 for r in reports)
    chunks = await assert_consistent(app, pid)
    assert chunks and (await state_of(app, pid)).status == IndexStatus.INDEXED


async def test_two_first_time_indexes_of_one_product_converge(app, callers):
    """No ``index_state`` row exists to lock yet: the loser of the race fails on
    the primary key (or a deadlock) and rolls back whole; redelivered, as the
    consumer does, the newer version wins and no chunk is duplicated."""
    pid = str(uuid.uuid4())
    v1 = product_payload(pid, callers.merchant_a_id, name="Twin", description="Old words.")
    v2 = product_payload(
        pid, callers.merchant_a_id, name="Twin", description="New words.", version=2
    )
    results = await asyncio.gather(index(app, v1), index(app, v2), return_exceptions=True)
    for payload, result in zip((v1, v2), results, strict=True):
        if isinstance(result, BaseException):
            assert isinstance(result, DBAPIError), result
            await index(app, payload)  # the redelivery
    state = await state_of(app, pid)
    assert state.catalog_version == 2 and state.source_document["version"] == 2
    chunks = await assert_consistent(app, pid)
    assert mentions(chunks, "New words.") and not mentions(chunks, "Old words.")


async def test_an_event_landing_mid_reindex_is_not_overwritten(
    app, embeddings, settings, callers, monkeypatch
):
    """The reindex reads the stored v1 document; v2 arrives before it writes.
    Whatever the interleaving, the product ends at v2 (AST-3)."""
    pid = str(uuid.uuid4())
    merchant = callers.merchant_a_id
    await index(app, product_payload(pid, merchant, name="Moving", description="Version one."))
    v2 = product_payload(pid, merchant, name="Moving", description="Version two.", version=2)
    pending: list[asyncio.Task] = []

    class EventMidReindex(IndexingRepo):
        async def index_payload(self, payload):
            if not pending:
                pending.append(asyncio.create_task(index(app, v2)))
                # Let the event run: it commits now, or waits on our row lock.
                await asyncio.sleep(0.3)
            return await super().index_payload(payload)

    monkeypatch.setattr(reindex_module, "IndexingRepo", EventMidReindex)
    await run_reindex(
        app.state.db, embeddings, settings, ReindexScope(product_ids=(uuid.UUID(pid),))
    )
    await pending[0]

    state = await state_of(app, pid)
    assert state.catalog_version == 2 and state.source_document["version"] == 2
    chunks = await assert_consistent(app, pid)
    assert mentions(chunks, "Version two.") and not mentions(chunks, "Version one.")


class HookedEmbeddings:
    """Delegates to the test provider; runs ``hook`` once, on the first embed."""

    def __init__(self, inner, hook) -> None:
        self.inner, self.hook = inner, hook
        self.model, self.dimension = inner.model, inner.dimension

    async def embed(self, texts, *, input_type):
        if self.hook is not None:
            hook, self.hook = self.hook, None
            await hook()
        return await self.inner.embed(texts, input_type=input_type)


async def test_an_unpublish_after_the_batch_was_listed_is_not_resurrected(
    app, embeddings, settings, callers
):
    """The batch lists both products; while the first is re-embedded, the
    second is unpublished. The reindex must skip it, not index it again from
    its stored (published) document (AST-5, fail closed)."""
    first, second = sorted((str(uuid.uuid4()), str(uuid.uuid4())))
    for pid, name in ((first, "Alpha"), (second, "Beta")):
        await index(app, product_payload(pid, callers.merchant_a_id, name=name))
    async with app.state.db.sessionmaker() as s, s.begin():
        await s.execute(EmbeddingCacheEntry.__table__.delete())  # force an embed call

    async def unpublish_second() -> None:
        async with app.state.db.sessionmaker() as s, s.begin():
            result = await IndexingRepo(s, embeddings, settings).remove(
                {"product_id": second, "merchant_id": callers.merchant_a_id, "version": 2}
            )
        assert result.outcome == IndexOutcome.REMOVED

    scope = ReindexScope(product_ids=(uuid.UUID(first), uuid.UUID(second)))
    report = await run_reindex(
        app.state.db, HookedEmbeddings(embeddings, unpublish_second), settings, scope
    )
    assert (report.indexed, report.skipped, report.failed) == (1, 1, 0)
    state = await state_of(app, second)
    assert state.status == IndexStatus.REMOVED and state.chunk_count == 0
    assert await chunks_of(app, second) == []
    assert (await state_of(app, first)).status == IndexStatus.INDEXED


async def test_reindex_never_lists_removed_products(app, embeddings, settings):
    pid = str(uuid.uuid4())
    await index(app, product_payload(pid, str(uuid.uuid4()), name="Ghost"))
    async with app.state.db.sessionmaker() as s, s.begin():
        await IndexingRepo(s, embeddings, settings).mark_removed(uuid.UUID(pid), "test")
    report = await run_reindex(
        app.state.db, embeddings, settings, ReindexScope(product_ids=(uuid.UUID(pid),))
    )
    assert (report.indexed, report.skipped, report.failed) == (0, 0, 0)
    assert (await state_of(app, pid)).status == IndexStatus.REMOVED


async def test_an_embedding_outage_stops_the_reindex_and_a_rerun_resumes(app, embeddings, settings):
    """Provider down is not a product failure: nothing is marked ``failed``,
    the run stops with the error, and the next run completes (idempotent)."""
    pid = str(uuid.uuid4())
    await index(app, product_payload(pid, str(uuid.uuid4()), name="Patient"))
    async with app.state.db.sessionmaker() as s, s.begin():
        await s.execute(EmbeddingCacheEntry.__table__.delete())
    scope = ReindexScope(product_ids=(uuid.UUID(pid),))
    embeddings.down = True
    with pytest.raises(EmbeddingUnavailable):
        await run_reindex(app.state.db, embeddings, settings, scope)
    assert (await state_of(app, pid)).status == IndexStatus.INDEXED
    embeddings.down = False
    report = await run_reindex(app.state.db, embeddings, settings, scope)
    assert (report.indexed, report.failed) == (1, 0)
    await assert_consistent(app, pid)
