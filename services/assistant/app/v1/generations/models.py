"""``generations``, ``token_budgets``, ``bulk_generation_jobs`` (RFC 0009 §3.1.3)."""

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.enums import ContentType, FinishReason
from app.v1.generations.enums import BulkJobStatus
from smartretail_common.db import Base, CreatedAt, Timestamps, UUIDPrimaryKey
from smartretail_common.db_types import DomainEnumText


class BulkGenerationJob(UUIDPrimaryKey, Timestamps, Base):
    """``inputs`` snapshots the request and each product's facts at submission,
    so the worker needs no merchant token (AST-D37)."""

    __tablename__ = "bulk_generation_jobs"
    __table_args__ = (
        UniqueConstraint("merchant_id", "idempotency_key"),
        Index("ix_bulk_generation_jobs_merchant_id", "merchant_id"),
    )

    merchant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    content_type: Mapped[ContentType | str] = mapped_column(
        DomainEnumText(ContentType), nullable=False, comment="DomainEnum ContentType"
    )
    product_ids: Mapped[list[uuid.UUID]] = mapped_column(ARRAY(UUID(as_uuid=True)), nullable=False)
    status: Mapped[BulkJobStatus | None] = mapped_column(
        DomainEnumText(BulkJobStatus, on_unknown="fallback", fallback=BulkJobStatus.QUEUED),
        nullable=False,
        comment="DomainEnum BulkJobStatus",
    )
    completed: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    failed: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    request_hash: Mapped[str] = mapped_column(Text, nullable=False)
    inputs: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    item_errors: Mapped[dict[str, str]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Generation(UUIDPrimaryKey, CreatedAt, Base):
    __tablename__ = "generations"
    __table_args__ = (
        Index("ix_generations_merchant_id_created_at", "merchant_id", "created_at"),
        Index("ix_generations_product_id", "product_id"),
        Index("ix_generations_bulk_job_id", "bulk_job_id"),
        # A redelivered bulk task never generates the same item twice.
        Index(
            "uq_generations_bulk_job_id_product_id",
            "bulk_job_id",
            "product_id",
            unique=True,
            postgresql_where=text("bulk_job_id IS NOT NULL"),
        ),
    )

    merchant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    product_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    content_type: Mapped[ContentType | str] = mapped_column(
        DomainEnumText(ContentType), nullable=False, comment="DomainEnum ContentType"
    )
    parameters: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    content: Mapped[str] = mapped_column(Text, nullable=False)
    prompt_tokens: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    completion_tokens: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    cost_usd: Mapped[Decimal] = mapped_column(
        Numeric(10, 6), nullable=False, server_default=text("0")
    )
    model: Mapped[str] = mapped_column(Text, nullable=False)
    finish_reason: Mapped[FinishReason | str | None] = mapped_column(
        DomainEnumText(FinishReason), nullable=True, comment="DomainEnum FinishReason"
    )
    applied: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    bulk_job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("bulk_generation_jobs.id"), nullable=True
    )


class TokenBudget(Base):
    """Per scope per UTC day: ``user:<uuid>``, ``ip:<sha256>`` or ``global`` (AST-9)."""

    __tablename__ = "token_budgets"

    scope: Mapped[str] = mapped_column(Text, primary_key=True)
    date: Mapped[date] = mapped_column(Date, primary_key=True)
    tokens_used: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    requests: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    cost_usd: Mapped[Decimal] = mapped_column(
        Numeric(10, 6), nullable=False, server_default=text("0")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
