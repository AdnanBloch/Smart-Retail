"""``product_chunks``, ``embedding_cache``, ``index_state``, ``query_log`` (RFC 0009 §3.1.3)."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    CHAR,
    Boolean,
    Computed,
    DateTime,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TSVECTOR, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.config import EMBEDDING_DIMENSION
from app.v1.search.enums import ChunkType, IndexStatus, SearchMode
from smartretail_common.db import Base, Timestamps, UUIDPrimaryKey
from smartretail_common.db_types import DomainEnumText


class ProductChunk(UUIDPrimaryKey, Base):
    """Derived and rebuildable (S5): inserted and pruned, never updated in content."""

    __tablename__ = "product_chunks"
    __table_args__ = (
        UniqueConstraint("product_id", "content_hash"),
        Index("ix_product_chunks_product_id", "product_id"),
        Index("ix_product_chunks_merchant_id", "merchant_id"),
        Index("ix_product_chunks_category_id", "category_id"),
        Index("ix_product_chunks_product_status", "product_status"),
        Index("ix_product_chunks_status_category", "product_status", "category_id"),
        Index("ix_product_chunks_search_vector", "search_vector", postgresql_using="gin"),
        Index(
            "ix_product_chunks_embedding",
            "embedding",
            postgresql_using="hnsw",
            postgresql_with={"m": 16, "ef_construction": 64},
            postgresql_ops={"embedding": "vector_cosine_ops"},
        ),
    )
    __mapper_args__ = {"eager_defaults": True}

    product_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    merchant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    category_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    chunk_type: Mapped[ChunkType | str] = mapped_column(
        DomainEnumText(ChunkType), nullable=False, comment="DomainEnum ChunkType"
    )
    content: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    embedding: Mapped[Any] = mapped_column(Vector(EMBEDDING_DIMENSION), nullable=False)
    token_count: Mapped[int] = mapped_column(Integer, nullable=False)
    search_vector: Mapped[Any] = mapped_column(
        TSVECTOR, Computed("to_tsvector('english', content)", persisted=True)
    )
    product_status: Mapped[str] = mapped_column(Text, nullable=False)
    price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    currency: Mapped[str | None] = mapped_column(CHAR(3), nullable=True)
    # ``metadata`` is reserved on declarative classes; the column keeps its name.
    meta: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    indexed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class EmbeddingCacheEntry(Base):
    __tablename__ = "embedding_cache"
    __table_args__ = (Index("ix_embedding_cache_last_used_at", "last_used_at"),)

    content_hash: Mapped[str] = mapped_column(Text, primary_key=True)
    model: Mapped[str] = mapped_column(Text, nullable=False)
    embedding: Mapped[Any] = mapped_column(Vector(EMBEDDING_DIMENSION), nullable=False)
    token_count: Mapped[int] = mapped_column(Integer, nullable=False)
    hit_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_used_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class IndexState(Timestamps, Base):
    """The product-index aggregate root (§3.1.1.2). ``source_document`` keeps the
    last event payload so ``python -m app.v1.search.reindex`` can rebuild without
    catalog (AST-D36)."""

    __tablename__ = "index_state"
    __table_args__ = (
        Index("ix_index_state_status", "status"),
        Index("ix_index_state_merchant_id", "merchant_id"),
    )

    product_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    merchant_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    catalog_version: Mapped[int] = mapped_column(Integer, nullable=False)
    chunk_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    embedding_model: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[IndexStatus | None] = mapped_column(
        DomainEnumText(IndexStatus, on_unknown="fallback", fallback=IndexStatus.PENDING),
        nullable=False,
        comment="DomainEnum IndexStatus",
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_document: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    indexed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class QueryLog(UUIDPrimaryKey, Base):
    __tablename__ = "query_log"
    __table_args__ = (
        Index("ix_query_log_created_at", "created_at"),
        Index("ix_query_log_user_id", "user_id"),
        Index("ix_query_log_correlation_id", "correlation_id"),
    )
    __mapper_args__ = {"eager_defaults": True}

    query_text: Mapped[str] = mapped_column(Text, nullable=False)
    filters: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    mode: Mapped[SearchMode | str] = mapped_column(
        DomainEnumText(SearchMode), nullable=False, comment="DomainEnum SearchMode"
    )
    result_product_ids: Mapped[list[uuid.UUID]] = mapped_column(
        ARRAY(UUID(as_uuid=True)), nullable=False, server_default=text("'{}'")
    )
    result_count: Mapped[int] = mapped_column(Integer, nullable=False)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    embedding_cached: Mapped[bool] = mapped_column(Boolean, nullable=False)
    degraded: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
