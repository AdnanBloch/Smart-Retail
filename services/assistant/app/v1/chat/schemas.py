"""``/api/v1/assistant/chat*``, ``/conversations*``, ``/messages/{id}/feedback`` bodies."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import Field

from app.core.schemas import OutModel, StrictModel
from app.v1.search.schemas import FiltersIn


class ChatIn(StrictModel):
    message: Annotated[str, Field(min_length=1, max_length=20_000)]
    conversation_id: uuid.UUID | None = None
    session_id: Annotated[str, Field(min_length=8, max_length=128)] | None = None
    filters: FiltersIn | None = None


class ChatSyncOut(OutModel):
    conversation_id: uuid.UUID
    message_id: uuid.UUID
    session_id: str | None = None
    content: str
    sources: list[dict[str, Any]]
    usage: dict[str, Any]
    finish_reason: str


class ConversationOut(OutModel):
    id: uuid.UUID
    title: str | None
    message_count: int
    total_tokens: int
    last_message_at: datetime | None
    created_at: datetime


class MessageOut(OutModel):
    id: uuid.UUID
    role: str
    content: str
    intent: str | None
    sources: list[dict[str, Any]]
    prompt_tokens: int
    completion_tokens: int
    model: str | None
    cost_usd: Decimal
    finish_reason: str | None
    feedback_helpful: bool | None
    created_at: datetime


class ConversationDetail(ConversationOut):
    messages: list[MessageOut]


class FeedbackIn(StrictModel):
    helpful: bool
    comment: Annotated[str, Field(max_length=2000)] | None = None
