"""``conversations`` and ``messages`` (RFC 0009 §3.1.3)."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.enums import FinishReason
from app.v1.chat.enums import Intent, MessageRole
from smartretail_common.db import Base, CreatedAt, Timestamps, UUIDPrimaryKey
from smartretail_common.db_types import DomainEnumText


class Conversation(UUIDPrimaryKey, Timestamps, Base):
    """Owned by one ``user_id``, or by one anonymous ``session_id`` (AST-13)."""

    __tablename__ = "conversations"
    __table_args__ = (
        Index("ix_conversations_user_id_created_at", "user_id", text("created_at DESC")),
        Index("ix_conversations_session_id", "session_id"),
        Index("ix_conversations_last_message_at", "last_message_at"),
    )

    user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    session_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    message_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    total_tokens: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    last_message_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Message(UUIDPrimaryKey, CreatedAt, Base):
    __tablename__ = "messages"
    __table_args__ = (
        Index("ix_messages_conversation_id_created_at", "conversation_id", "created_at"),
        Index("ix_messages_correlation_id", "correlation_id"),
    )

    conversation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[MessageRole | str] = mapped_column(
        DomainEnumText(MessageRole), nullable=False, comment="DomainEnum MessageRole"
    )
    content: Mapped[str] = mapped_column(Text, nullable=False)
    intent: Mapped[Intent | str | None] = mapped_column(
        DomainEnumText(Intent), nullable=True, comment="DomainEnum Intent"
    )
    sources: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    prompt_tokens: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    completion_tokens: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    model: Mapped[str | None] = mapped_column(Text, nullable=True)
    cost_usd: Mapped[Decimal] = mapped_column(
        Numeric(10, 6), nullable=False, server_default=text("0")
    )
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    time_to_first_token_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    finish_reason: Mapped[FinishReason | str | None] = mapped_column(
        DomainEnumText(FinishReason), nullable=True, comment="DomainEnum FinishReason"
    )
    feedback_helpful: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    feedback_comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
