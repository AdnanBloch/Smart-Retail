"""Every ORM model, registered on one metadata.

Alembic's ``env.py`` imports from here, and so does every process that opens
a session (API, indexer-worker, Celery), so relationships and foreign keys
always resolve.
"""

from app.v1.chat.models import Conversation, Message
from app.v1.generations.models import BulkGenerationJob, Generation, TokenBudget
from app.v1.prompts.models import PromptTemplate
from app.v1.search.models import EmbeddingCacheEntry, IndexState, ProductChunk, QueryLog
from smartretail_common.db import Base
from smartretail_common.events.dedupe import ProcessedEvent
from smartretail_common.outbox import OutboxEvent

__all__ = [
    "Base",
    "BulkGenerationJob",
    "Conversation",
    "EmbeddingCacheEntry",
    "Generation",
    "IndexState",
    "Message",
    "OutboxEvent",
    "ProcessedEvent",
    "ProductChunk",
    "PromptTemplate",
    "QueryLog",
    "TokenBudget",
]
