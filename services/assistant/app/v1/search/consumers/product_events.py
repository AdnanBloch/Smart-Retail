"""``catalog.product.published`` / ``.updated`` / ``.unpublished`` → the index (§3.1.2.4).

Thin handlers over :class:`IndexingRepo`, run by ``indexer-worker`` on the
consumer base: the ``processed_events`` claim and these effects share one
transaction (S13). An embedding-provider outage raises
``EmbeddingUnavailable`` (a ``DependencyUnavailable``), so the base waits
and the event stays in Kafka instead of being dead-lettered (RFC 0001 §3.1.1.5).
"""

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.v1.search.clients.embeddings import EmbeddingProvider
from app.v1.search.indexing_repo import IndexingRepo
from smartretail_common.events.consumer import Handler
from smartretail_common.events.envelope import Envelope

logger = logging.getLogger("assistant.indexer")


def index_handler(settings: Settings, embeddings: EmbeddingProvider) -> Handler:
    async def handle(event: Envelope, session: AsyncSession) -> None:
        result = await IndexingRepo(session, embeddings, settings).index_payload(event.payload)
        logger.info(
            "product indexed",
            extra={
                "event_id": str(event.event_id),
                "event_type": event.event_type,
                "product_id": event.payload.get("product_id"),
                "outcome": str(result.outcome),
                "chunks": result.chunks,
                "cache_hits": result.cache_hits,
                "embedded": result.embedded,
            },
        )

    return handle


def unpublish_handler(settings: Settings, embeddings: EmbeddingProvider) -> Handler:
    async def handle(event: Envelope, session: AsyncSession) -> None:
        result = await IndexingRepo(session, embeddings, settings).remove(event.payload)
        logger.info(
            "product removed",
            extra={
                "event_id": str(event.event_id),
                "product_id": event.payload.get("product_id"),
                "outcome": str(result.outcome),
            },
        )

    return handle
