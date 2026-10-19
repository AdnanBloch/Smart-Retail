"""The content-hash embedding cache (D6, AST-D7).

Keyed by ``sha256(model, normalised text)``: an unchanged chunk is never
re-embedded, and a re-index of an unchanged product makes zero provider calls
(G4 (0004)). Query embeddings share the table under a ``query`` purpose in the
hash input (Q7 (0004)). Misses are embedded in batches of at most
``EMBEDDING_BATCH_SIZE``.
"""

from dataclasses import dataclass

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.tokens import count_tokens
from app.v1.search.clients.embeddings import EmbeddingProvider, InputType
from app.v1.search.models import EmbeddingCacheEntry


@dataclass(frozen=True)
class CacheResult:
    vectors: dict[str, list[float]]
    hits: int
    misses: int


class EmbeddingCacheRepo:
    def __init__(
        self, session: AsyncSession, provider: EmbeddingProvider, *, batch_size: int = 96
    ) -> None:
        self.session = session
        self.provider = provider
        self.batch_size = batch_size

    async def lookup(self, hashes: list[str]) -> dict[str, list[float]]:
        if not hashes:
            return {}
        rows = await self.session.execute(
            select(EmbeddingCacheEntry.content_hash, EmbeddingCacheEntry.embedding).where(
                EmbeddingCacheEntry.content_hash.in_(hashes)
            )
        )
        found = {h: [float(x) for x in e] for h, e in rows.all()}
        if found:
            await self.session.execute(
                update(EmbeddingCacheEntry)
                .where(EmbeddingCacheEntry.content_hash.in_(list(found)))
                .values(hit_count=EmbeddingCacheEntry.hit_count + 1, last_used_at=func.now())
            )
        return found

    async def get_or_embed(self, texts: dict[str, str], *, input_type: InputType) -> CacheResult:
        """``texts`` maps content hash → text. Returns a vector for every hash."""
        found = await self.lookup(list(texts))
        missing = [h for h in texts if h not in found]
        for start in range(0, len(missing), self.batch_size):
            batch = missing[start : start + self.batch_size]
            vectors = await self.provider.embed([texts[h] for h in batch], input_type=input_type)
            await self.session.execute(
                pg_insert(EmbeddingCacheEntry)
                .values(
                    [
                        {
                            "content_hash": h,
                            "model": self.provider.model,
                            "embedding": v,
                            "token_count": count_tokens(texts[h]),
                        }
                        for h, v in zip(batch, vectors, strict=True)
                    ]
                )
                .on_conflict_do_nothing(index_elements=["content_hash"])
            )
            found.update(zip(batch, vectors, strict=True))
        return CacheResult(found, hits=len(texts) - len(missing), misses=len(missing))
