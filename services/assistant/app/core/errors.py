"""assistant problem types (RFC 0009 §3.1.2.1). RFC 9457 bodies via smartretail_common."""

from datetime import UTC, datetime, timedelta
from typing import Any

from smartretail_common.errors import (
    Conflict,
    DependencyUnavailable,
    DomainError,
    Forbidden,
    NotFound,
    Unprocessable,
)


class ProductNotIndexed(NotFound):
    type_slug = "product-not-indexed"
    title = "The product is not in the search index"


class ProductNotFound(NotFound):
    type_slug = "product-not-found"
    title = "Product not found"


class NotProductOwner(Forbidden):
    type_slug = "not-product-owner"
    title = "The product belongs to another merchant"


class ConversationNotFound(NotFound):
    type_slug = "conversation-not-found"
    title = "Conversation not found"


class MessageNotFound(NotFound):
    type_slug = "message-not-found"
    title = "Message not found"


class GenerationNotFound(NotFound):
    type_slug = "generation-not-found"
    title = "Generation not found"


class BulkJobNotFound(NotFound):
    type_slug = "bulk-job-not-found"
    title = "Bulk generation job not found"


class BulkJobTerminal(Conflict):
    type_slug = "bulk-job-terminal"
    title = "The job has already finished"


class IdempotencyKeyReused(Conflict):
    type_slug = "idempotency-key-reused"
    title = "The Idempotency-Key was used with a different request"


class ReindexRunning(Conflict):
    type_slug = "reindex-running"
    title = "A full reindex is already running"


class MessageBlocked(Unprocessable):
    type_slug = "message-blocked"
    title = "The message was blocked by the input guard"


class StreamCapReached(DependencyUnavailable):
    type_slug = "stream-cap-reached"
    title = "Too many concurrent streams; retry shortly"


class CatalogUnavailable(DependencyUnavailable):
    type_slug = "catalog-unavailable"
    title = "The catalog is unavailable"


class EmbeddingUnavailable(DependencyUnavailable):
    type_slug = "embedding-provider-unavailable"
    title = "The embedding provider is unavailable"


class LLMUnavailable(DependencyUnavailable):
    type_slug = "llm-provider-unavailable"
    title = "The language model provider is unavailable"


def seconds_until_utc_midnight(now: datetime | None = None) -> int:
    now = now or datetime.now(UTC)
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(1, int((midnight - now).total_seconds()))


class BudgetExceeded(DomainError):
    """429 with ``Retry-After`` to the next UTC midnight (E10)."""

    status = 429
    type_slug = "token-budget-exceeded"
    title = "Daily token budget exhausted"

    def __init__(self, scope_kind: str, *, now: datetime | None = None) -> None:
        retry = seconds_until_utc_midnight(now)
        reset = (now or datetime.now(UTC)) + timedelta(seconds=retry)
        extras: dict[str, Any] = {"scope": scope_kind, "resets_at": reset.isoformat()}
        super().__init__(
            f"The {scope_kind} daily token budget is exhausted",
            extras=extras,
            headers={"Retry-After": str(retry)},
        )
