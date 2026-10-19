"""Per-event index and remove logic, reindex and the model check (§3.1.1.4).

Runs inside the caller's transaction: the consumer base's (which also holds
the ``processed_events`` claim, S13), a reindex batch's, or hydration's
fail-closed removal. ``index_state`` is locked ``FOR UPDATE`` first, so two
events for one product serialise, and its transitions follow the AST-7 table.

* An event whose ``version`` is below ``catalog_version`` is stale and
  skipped (AST-3).
* A payload whose status is not ``published`` removes the product (AST-5).
* Chunk text is hashed; only cache misses reach the embedding provider (D6).
* Chunks whose hash disappeared are pruned; the rest are upserted.
* The payload is kept as ``source_document`` for the reindex (AST-D36).
"""

import logging
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.v1.search.cache_repo import EmbeddingCacheRepo
from app.v1.search.chunker import Chunk, chunk_product, content_hash
from app.v1.search.clients.embeddings import EmbeddingProvider
from app.v1.search.enums import IndexOutcome, IndexStatus
from app.v1.search.models import IndexState, ProductChunk
from app.v1.search.renderer import category_path, document_from_payload, render
from app.v1.search.utils import path_to_indexed, path_to_removed

logger = logging.getLogger("assistant.indexer")

PUBLISHED = "published"


@dataclass(frozen=True)
class IndexResult:
    outcome: IndexOutcome
    chunks: int = 0
    cache_hits: int = 0
    embedded: int = 0
    truncated: bool = False


def _uuid(value: Any) -> uuid.UUID | None:
    if value in (None, ""):
        return None
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))


def _money(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value)).quantize(Decimal("0.01")) if value is not None else None
    except InvalidOperation:
        return None


class IndexingRepo:
    def __init__(
        self, session: AsyncSession, embeddings: EmbeddingProvider, settings: Settings
    ) -> None:
        self.session = session
        self.embeddings = embeddings
        self.settings = settings
        self.model = settings.active_embedding_model

    async def _state(self, product_id: uuid.UUID) -> IndexState | None:
        return await self.session.scalar(
            select(IndexState).where(IndexState.product_id == product_id).with_for_update()
        )

    async def index_payload(self, payload: dict[str, Any]) -> IndexResult:
        """``catalog.product.published`` / ``.updated`` (or a stored source document)."""
        product_id = _uuid(payload["product_id"])
        assert product_id is not None
        version = int(payload["version"])
        state = await self._state(product_id)
        if state is not None and version < state.catalog_version:
            logger.info("stale event skipped", extra={"product_id": str(product_id)})
            return IndexResult(IndexOutcome.SKIPPED_STALE)
        if payload.get("status") != PUBLISHED:
            return await self._remove(product_id, version, payload, state)

        document = document_from_payload(payload)
        plan = chunk_product(
            render(document),
            target=self.settings.chunk_target_tokens,
            max_tokens=self.settings.chunk_max_tokens,
            overlap=self.settings.chunk_overlap_tokens,
            max_chunks=self.settings.chunk_max_per_product,
        )
        if plan.truncated:
            logger.warning(
                "chunk bound hit; description truncated",
                extra={"product_id": str(product_id), "bound": self.settings.chunk_max_per_product},
            )
        chunks: dict[str, Chunk] = {}
        for chunk in plan.chunks:  # identical text in one product is one row
            chunks.setdefault(content_hash(self.model, chunk.content), chunk)
        cached = await EmbeddingCacheRepo(
            self.session, self.embeddings, batch_size=self.settings.embedding_batch_size
        ).get_or_embed({h: c.content for h, c in chunks.items()}, input_type="document")

        await self.session.execute(
            delete(ProductChunk).where(
                ProductChunk.product_id == product_id,
                ProductChunk.content_hash.not_in(list(chunks)),
            )
        )
        merchant_id = _uuid(payload.get("merchant_id"))
        category_id = _uuid(payload.get("category_id")) or _uuid(
            (document.get("category") or {}).get("id")
        )
        media = document.get("media") or []
        meta = {
            "name": render(document).name,
            "sku": document.get("sku"),
            "category_path": category_path(document),
            "media_url": media[0].get("url") if media and isinstance(media[0], dict) else None,
        }
        rows = [
            {
                "product_id": product_id,
                "merchant_id": merchant_id,
                "category_id": category_id,
                "chunk_index": chunk.chunk_index,
                "chunk_type": chunk.chunk_type,
                "content": chunk.content,
                "content_hash": digest,
                "embedding": cached.vectors[digest],
                "token_count": chunk.token_count,
                "product_status": PUBLISHED,
                "price": _money(payload.get("base_price") or document.get("base_price")),
                "currency": payload.get("currency") or document.get("currency"),
                "meta": meta,
            }
            for digest, chunk in chunks.items()
        ]
        insert = pg_insert(ProductChunk).values(rows)
        await self.session.execute(
            insert.on_conflict_do_update(
                index_elements=["product_id", "content_hash"],
                set_={
                    "chunk_index": insert.excluded.chunk_index,
                    "chunk_type": insert.excluded.chunk_type,
                    "merchant_id": insert.excluded.merchant_id,
                    "category_id": insert.excluded.category_id,
                    "product_status": insert.excluded.product_status,
                    "price": insert.excluded.price,
                    "currency": insert.excluded.currency,
                    "metadata": insert.excluded.metadata,
                    "indexed_at": func.now(),
                },
            )
        )
        path_to_indexed(state.status if state is not None else None)
        await self._save_state(
            state,
            product_id,
            merchant_id=merchant_id,
            version=version,
            status=IndexStatus.INDEXED,
            chunk_count=len(rows),
            source_document=payload,
            indexed=True,
        )
        return IndexResult(
            IndexOutcome.INDEXED,
            chunks=len(rows),
            cache_hits=cached.hits,
            embedded=cached.misses,
            truncated=plan.truncated,
        )

    async def remove(self, payload: dict[str, Any]) -> IndexResult:
        """``catalog.product.unpublished``: every chunk deleted, status ``removed``."""
        product_id = _uuid(payload["product_id"])
        assert product_id is not None
        version = int(payload["version"])
        state = await self._state(product_id)
        if state is not None and version < state.catalog_version:
            return IndexResult(IndexOutcome.SKIPPED_STALE)
        return await self._remove(product_id, version, payload, state)

    async def _remove(
        self,
        product_id: uuid.UUID,
        version: int,
        payload: dict[str, Any],
        state: IndexState | None,
    ) -> IndexResult:
        path_to_removed(state.status if state is not None else None)
        await self.session.execute(
            delete(ProductChunk).where(ProductChunk.product_id == product_id)
        )
        await self._save_state(
            state,
            product_id,
            merchant_id=_uuid(payload.get("merchant_id")),
            version=version,
            status=IndexStatus.REMOVED,
            chunk_count=0,
            source_document=None,
            indexed=False,
        )
        return IndexResult(IndexOutcome.REMOVED)

    async def mark_removed(self, product_id: uuid.UUID, reason: str) -> bool:
        """Fail closed: catalog no longer shows the product (hydration 404)."""
        state = await self._state(product_id)
        if state is None or state.status == IndexStatus.REMOVED:
            return False
        path_to_removed(state.status)
        await self.session.execute(
            delete(ProductChunk).where(ProductChunk.product_id == product_id)
        )
        state.status = IndexStatus.REMOVED
        state.chunk_count = 0
        state.error = reason
        logger.warning(
            "removed from index", extra={"product_id": str(product_id), "reason": reason}
        )
        return True

    async def _save_state(
        self,
        state: IndexState | None,
        product_id: uuid.UUID,
        *,
        merchant_id: uuid.UUID | None,
        version: int,
        status: IndexStatus,
        chunk_count: int,
        source_document: dict[str, Any] | None,
        indexed: bool,
    ) -> None:
        if state is None:
            state = IndexState(product_id=product_id, catalog_version=version)
            self.session.add(state)
        state.catalog_version = max(version, state.catalog_version or 0)
        if merchant_id is not None:
            state.merchant_id = merchant_id
        state.chunk_count = chunk_count
        state.embedding_model = self.model
        state.status = status
        state.error = None
        if source_document is not None:
            state.source_document = source_document
        if indexed:
            state.indexed_at = func.now()
        await self.session.flush()


async def mark_stale_models(session: AsyncSession, model: str) -> int:
    """``check_index_model``: products indexed with another model become ``pending``."""
    result = await session.execute(
        update(IndexState)
        .where(IndexState.status == IndexStatus.INDEXED, IndexState.embedding_model != model)
        .values(status=IndexStatus.PENDING)
    )
    count = int(getattr(result, "rowcount", 0) or 0)
    if count:
        logger.warning(
            "embedding model changed; products marked pending",
            extra={"model": model, "count": count},
        )
    return count
