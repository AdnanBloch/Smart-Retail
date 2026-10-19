"""The search use case: retrieve → hydrate → fail closed → log (§3.1.2.1 sequence).

Used by ``/search/*`` and by the chat agent's retrieve node, so a product
the chat cites passed the same visibility and hydration rules as one search
shows.
"""

import time
import uuid
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.v1.search.clients.embeddings import EmbeddingProvider
from app.v1.search.enums import SearchMode
from app.v1.search.hydration import Hydrator, Live
from app.v1.search.indexing_repo import IndexingRepo
from app.v1.search.models import QueryLog
from app.v1.search.repo import Hit, SearchFilters, SearchRepo
from smartretail_common.context import get_request_id


@dataclass
class Result:
    hit: Hit
    live: Live

    @property
    def price(self) -> Decimal | None:
        return self.live.price

    def as_source(self) -> dict[str, object]:
        """The ``sources`` SSE / grounding-record shape (E5, E7)."""
        return {
            "id": str(self.hit.product_id),
            "name": self.live.name or self.hit.name,
            "price": str(self.live.price) if self.live.price is not None else None,
            "currency": self.live.currency,
            "in_stock": self.live.in_stock,
            "hydrated": self.live.hydrated,
            "score": round(self.hit.score, 6),
            "chunk_id": str(self.hit.chunk_id),
            "matched_text": self.hit.content,
        }


@dataclass
class SearchResponse:
    results: list[Result]
    mode: SearchMode
    degraded: str | None


class SearchService:
    def __init__(
        self,
        session: AsyncSession,
        embeddings: EmbeddingProvider,
        hydrator: Hydrator,
        settings: Settings,
    ) -> None:
        self.session = session
        self.repo = SearchRepo(session, embeddings, settings)
        self.indexing = IndexingRepo(session, embeddings, settings)
        self.hydrator = hydrator
        self.settings = settings

    async def hydrate(self, hits: list[Hit]) -> tuple[list[Result], bool]:
        """Live facts per hit; removed products are dropped and de-indexed.
        Returns the results and whether every one was hydrated."""
        live = await self.hydrator.hydrate([h.product_id for h in hits])
        results: list[Result] = []
        for hit in hits:
            facts = live[hit.product_id]
            if facts.removed:
                await self.indexing.mark_removed(hit.product_id, "catalog: not visible")
                continue
            results.append(Result(hit, facts))
        return results, all(r.live.hydrated for r in results)

    async def search(
        self,
        query: str,
        *,
        filters: SearchFilters,
        mode: SearchMode,
        limit: int,
        user_id: str | None,
        log: bool = True,
    ) -> SearchResponse:
        started = time.monotonic()
        raw = await self.repo.search(query, filters, mode, limit)
        results, _ = await self.hydrate(raw.hits)
        if log:
            self.session.add(
                QueryLog(
                    query_text=query,
                    filters=filters.as_dict(),
                    mode=mode,
                    result_product_ids=[r.hit.product_id for r in results],
                    result_count=len(results),
                    latency_ms=int((time.monotonic() - started) * 1000),
                    embedding_cached=raw.embedding_cached,
                    degraded=raw.degraded,
                    user_id=uuid.UUID(user_id) if user_id else None,
                    correlation_id=get_request_id(),
                )
            )
        await self.session.commit()
        return SearchResponse(results, mode, raw.degraded)

    async def similar(
        self,
        product_id: uuid.UUID,
        limit: int,
        *,
        merchant_id: uuid.UUID | None = None,
        category_id: uuid.UUID | None = None,
        hydrate: bool = True,
    ) -> list[Result] | None:
        hits = await self.repo.similar(
            product_id, limit, merchant_id=merchant_id, category_id=category_id
        )
        if hits is None:
            await self.session.commit()
            return None
        if not hydrate:
            await self.session.commit()
            return [Result(h, Live()) for h in hits]
        results, _ = await self.hydrate(hits)
        await self.session.commit()
        return results
