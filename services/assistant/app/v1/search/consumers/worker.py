"""The indexer-worker process: ``python -m app.v1.search.consumers.worker`` (RFC 0001 §3.1.1.5).

Consumer group ``assistant-indexer`` (AST-15) on the three catalog product
topics, on the platform's ``EventConsumer`` (dedupe, retry, DLQ, manual
offset commit). At start-up the configured embedding model is checked
against ``index_state`` (``check_index_model``).
"""

import asyncio

import httpx

# Register every ORM model: a worker reaches only some through its own imports.
import app.models  # noqa: F401
from app.core.config import Settings, get_settings
from app.core.db import create_database
from app.core.logger import init_logger
from app.v1.search.clients.embeddings import EmbeddingProvider, build_embeddings
from app.v1.search.consumers.product_events import index_handler, unpublish_handler
from app.v1.search.indexing_repo import mark_stale_models
from smartretail_common.events.consumer import EventConsumer, serve
from smartretail_common.events.producer import Publisher
from smartretail_common.events.serde import AvroCodec

GROUP = "assistant-indexer"
TOPICS = ("catalog.product.published", "catalog.product.updated", "catalog.product.unpublished")


def build(
    settings: Settings, codec: AvroCodec, dlq: Publisher, embeddings: EmbeddingProvider
) -> EventConsumer:
    db = create_database(settings)
    index = index_handler(settings, embeddings)
    return EventConsumer(
        group=GROUP,
        handlers={
            "catalog.product.published": index,
            "catalog.product.updated": index,
            "catalog.product.unpublished": unpublish_handler(settings, embeddings),
        },
        sessionmaker=db.sessionmaker,
        codec=codec,
        dlq=dlq,
    )


async def check_index_model(settings: Settings) -> int:
    db = create_database(settings)
    try:
        async with db.sessionmaker() as session, session.begin():
            return await mark_stale_models(session, settings.active_embedding_model)
    finally:
        await db.dispose()


def main() -> None:  # pragma: no cover - process wiring, exercised by the e2e suite
    settings = get_settings()
    init_logger(settings)
    asyncio.run(check_index_model(settings))
    http = httpx.AsyncClient()
    embeddings = build_embeddings(settings, http)
    asyncio.run(
        serve(
            lambda codec, dlq: build(settings, codec, dlq, embeddings),
            bootstrap_servers=settings.kafka_bootstrap_servers,
            schema_registry_url=settings.schema_registry_url,
        )
    )


if __name__ == "__main__":  # pragma: no cover
    main()
