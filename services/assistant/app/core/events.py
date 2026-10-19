"""``assistant.interaction`` (RFC 0009 §3.1.2.3, AST-6, E13).

Written with ``write_outbox`` in the transaction that writes the message or
generation row; the payload is validated against the registered Avro schema
at write time. Ids and numbers only — never prompt text or PII.
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from smartretail_common.outbox import write_outbox

EVENT_TYPE = "assistant.interaction"


def interaction_payload(
    *,
    interaction_id: uuid.UUID,
    kind: str,
    finish_reason: str,
    answered: bool,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cost_usd: Decimal,
    latency_ms: int,
    time_to_first_token_ms: int | None,
    conversation_id: uuid.UUID | None = None,
    generation_id: uuid.UUID | None = None,
    user_id: str | None = None,
    merchant_id: str | None = None,
    intent: str | None = None,
    content_type: str | None = None,
    source_product_ids: list[str] | None = None,
    occurred_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "interaction_id": str(interaction_id),
        "kind": kind,
        "conversation_id": str(conversation_id) if conversation_id else None,
        "generation_id": str(generation_id) if generation_id else None,
        "user_id": user_id,
        "merchant_id": merchant_id,
        "intent": intent,
        "content_type": content_type,
        "answered": answered,
        "finish_reason": finish_reason,
        "source_product_ids": source_product_ids or [],
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": str(cost_usd),
        "latency_ms": latency_ms,
        "time_to_first_token_ms": time_to_first_token_ms,
        "occurred_at": (occurred_at or datetime.now(UTC)).isoformat(),
    }


def emit_interaction(
    session: AsyncSession,
    *,
    aggregate_type: str,
    aggregate_id: uuid.UUID,
    payload: dict[str, Any],
    correlation_id: str | None,
) -> uuid.UUID:
    return write_outbox(
        session,
        event_type=EVENT_TYPE,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        payload=payload,
        correlation_id=correlation_id,
    )
