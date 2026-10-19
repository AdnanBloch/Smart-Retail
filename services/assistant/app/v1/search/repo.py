"""SearchRepo: dense, keyword, RRF fusion, grouping, similar (D4, D7, D8, AST-D9, AST-D10).

Every leg filters in SQL before ranking — ``product_status = 'published'``
always, plus the optional category, merchant and price-snapshot filters — so
a filtered query still returns up to k (D8). Results are grouped to products
by their best chunk (D4). When the query cannot be embedded, hybrid and dense
degrade to the keyword leg with ``degraded = "keyword_only"`` (Q9 (0004)).
"""

import logging
import re
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import numpy as np
from sqlalchemy import ColumnElement, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.v1.search.cache_repo import EmbeddingCacheRepo
from app.v1.search.chunker import content_hash
from app.v1.search.clients.embeddings import EmbeddingError, EmbeddingProvider
from app.v1.search.enums import SearchMode
from app.v1.search.models import ProductChunk
from app.v1.search.utils import rrf
from smartretail_common.errors import DependencyUnavailable

logger = logging.getLogger("assistant.search")

KEYWORD_ONLY = "keyword_only"
_WORD = re.compile(r"[a-z0-9]+")


@dataclass(frozen=True)
class SearchFilters:
    category_id: uuid.UUID | None = None
    merchant_id: uuid.UUID | None = None
    min_price: Decimal | None = None
    max_price: Decimal | None = None

    def as_dict(self) -> dict[str, Any]:
        return {k: str(v) for k, v in self.__dict__.items() if v is not None}


@dataclass(frozen=True)
class ChunkRow:
    chunk_id: uuid.UUID
    product_id: uuid.UUID
    merchant_id: uuid.UUID
    category_id: uuid.UUID | None
    chunk_type: str
    content: str
    meta: dict[str, Any]
    score: float


@dataclass
class Hit:
    product_id: uuid.UUID
    merchant_id: uuid.UUID
    category_id: uuid.UUID | None
    name: str
    score: float
    chunk_id: uuid.UUID
    chunk_type: str
    content: str
    match_reason: str
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RawSearch:
    hits: list[Hit]
    mode: SearchMode
    degraded: str | None
    embedding_cached: bool


def _conditions(filters: SearchFilters) -> list[ColumnElement[bool]]:
    conds: list[ColumnElement[bool]] = [ProductChunk.product_status == "published"]
    if filters.category_id is not None:
        conds.append(ProductChunk.category_id == filters.category_id)
    if filters.merchant_id is not None:
        conds.append(ProductChunk.merchant_id == filters.merchant_id)
    if filters.min_price is not None:
        conds.append(ProductChunk.price >= filters.min_price)
    if filters.max_price is not None:
        conds.append(ProductChunk.price <= filters.max_price)
    return conds


def keyword_query(query: str) -> str | None:
    """An OR tsquery of the query's alphanumeric words — injection-free by construction."""
    words = list(dict.fromkeys(_WORD.findall(query.lower())))[:32]
    return " | ".join(words) if words else None


def group_by_product(
    rows: list[ChunkRow], scores: dict[uuid.UUID, float], legs: dict[uuid.UUID, set[str]]
) -> list[Hit]:
    best: dict[uuid.UUID, Hit] = {}
    for row in rows:
        score = scores.get(row.chunk_id, 0.0)
        current = best.get(row.product_id)
        reasons = legs.get(row.product_id, set())
        reason = "+".join(sorted(reasons, key=lambda r: r != "semantic")) or "semantic"
        if current is None or score > current.score:
            best[row.product_id] = Hit(
                product_id=row.product_id,
                merchant_id=row.merchant_id,
                category_id=row.category_id,
                name=str(row.meta.get("name") or ""),
                score=score,
                chunk_id=row.chunk_id,
                chunk_type=row.chunk_type,
                content=row.content,
                match_reason=reason,
                meta=row.meta,
            )
    return sorted(best.values(), key=lambda h: (-h.score, str(h.product_id)))


class SearchRepo:
    def __init__(
        self, session: AsyncSession, embeddings: EmbeddingProvider, settings: Settings
    ) -> None:
        self.session = session
        self.embeddings = embeddings
        self.settings = settings

    def _columns(self) -> tuple[Any, ...]:
        return (
            ProductChunk.id,
            ProductChunk.product_id,
            ProductChunk.merchant_id,
            ProductChunk.category_id,
            ProductChunk.chunk_type,
            ProductChunk.content,
            ProductChunk.meta,
        )

    @staticmethod
    def _rows(result: Any) -> list[ChunkRow]:
        return [
            ChunkRow(r[0], r[1], r[2], r[3], str(r[4]), r[5], r[6] or {}, float(r[7]))
            for r in result.all()
        ]

    async def query_vector(self, query: str) -> tuple[list[float] | None, bool]:
        """The query's embedding (cached by hash), or ``None`` when the provider fails."""
        digest = content_hash(self.embeddings.model, "query\x00" + query)
        cache = EmbeddingCacheRepo(self.session, self.embeddings)
        try:
            result = await cache.get_or_embed({digest: query}, input_type="query")
        except (DependencyUnavailable, EmbeddingError) as exc:
            logger.warning("query embedding failed; keyword only", extra={"error": str(exc)})
            return None, False
        return result.vectors[digest], result.hits == 1

    async def dense(
        self,
        vector: list[float],
        filters: SearchFilters,
        k: int,
        *,
        exclude_product: uuid.UUID | None = None,
    ) -> list[ChunkRow]:
        await self.session.execute(
            text(f"SET LOCAL hnsw.ef_search = {int(self.settings.hnsw_ef_search)}")
        )
        distance = ProductChunk.embedding.cosine_distance(vector)
        conds = _conditions(filters)
        if exclude_product is not None:
            conds.append(ProductChunk.product_id != exclude_product)
        result = await self.session.execute(
            select(*self._columns(), (1 - distance).label("score"))
            .where(*conds)
            .order_by(distance)
            .limit(k)
        )
        return self._rows(result)

    async def keyword(self, query: str, filters: SearchFilters, k: int) -> list[ChunkRow]:
        expression = keyword_query(query)
        if expression is None:
            return []
        tsquery = func.to_tsquery("english", expression)
        rank = func.ts_rank_cd(ProductChunk.search_vector, tsquery)
        result = await self.session.execute(
            select(*self._columns(), rank.label("score"))
            .where(ProductChunk.search_vector.op("@@")(tsquery), *_conditions(filters))
            .order_by(rank.desc(), ProductChunk.id)
            .limit(k)
        )
        return self._rows(result)

    async def search(
        self, query: str, filters: SearchFilters, mode: SearchMode, limit: int
    ) -> RawSearch:
        k = self.settings.search_candidates_per_mode
        vector, cached = (None, False)
        degraded = None
        if mode in (SearchMode.HYBRID, SearchMode.DENSE):
            vector, cached = await self.query_vector(query)
            if vector is None:
                degraded = KEYWORD_ONLY
        dense_rows = await self.dense(vector, filters, k) if vector is not None else []
        keyword_rows = (
            await self.keyword(query, filters, k)
            if mode == SearchMode.KEYWORD or degraded or mode == SearchMode.HYBRID
            else []
        )
        legs: dict[uuid.UUID, set[str]] = {}
        for row in dense_rows:
            legs.setdefault(row.product_id, set()).add("semantic")
        for row in keyword_rows:
            legs.setdefault(row.product_id, set()).add("keyword")

        if mode == SearchMode.HYBRID and not degraded:
            scores = rrf(
                [
                    ([r.chunk_id for r in dense_rows], self.settings.rrf_weight_dense),
                    ([r.chunk_id for r in keyword_rows], self.settings.rrf_weight_keyword),
                ],
                k=self.settings.rrf_k,
            )
        else:
            source = dense_rows if mode == SearchMode.DENSE and not degraded else keyword_rows
            scores = {r.chunk_id: r.score for r in source}
        hits = group_by_product(dense_rows + keyword_rows, scores, legs)[:limit]
        return RawSearch(hits, mode, degraded, cached)

    async def similar(
        self,
        product_id: uuid.UUID,
        limit: int,
        *,
        merchant_id: uuid.UUID | None = None,
        category_id: uuid.UUID | None = None,
    ) -> list[Hit] | None:
        """Products nearest to the centroid of ``product_id``'s chunks; ``None`` if
        the product has no chunks (not indexed)."""
        embeddings = (
            await self.session.scalars(
                select(ProductChunk.embedding).where(
                    ProductChunk.product_id == product_id,
                    ProductChunk.product_status == "published",
                )
            )
        ).all()
        if not embeddings:
            return None
        centroid = np.mean(np.array([np.asarray(e, dtype=np.float64) for e in embeddings]), axis=0)
        norm = float(np.linalg.norm(centroid)) or 1.0
        vector = [float(x) for x in centroid / norm]
        rows = await self.dense(
            vector,
            SearchFilters(category_id=category_id, merchant_id=merchant_id),
            self.settings.search_candidates_per_mode,
            exclude_product=product_id,
        )
        scores = {r.chunk_id: r.score for r in rows}
        legs = {r.product_id: {"semantic"} for r in rows}
        return group_by_product(rows, scores, legs)[:limit]
