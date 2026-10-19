"""retrieval_tables: product_chunks, embedding_cache, index_state, query_log, processed_events.

``product_chunks.embedding`` is ``vector(1024)`` (AST-D31) with an HNSW cosine
index (m = 16, ef_construction = 64); ``search_vector`` is a generated
``tsvector`` with a GIN index, so it can never disagree with ``content``.

Revision ID: 0002_retrieval_tables
Revises: 0001_create_vector_extension
Create Date: 2026-10-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects import postgresql

from smartretail_common.migrations import create_processed_events, drop_processed_events

revision: str = "0002_retrieval_tables"
down_revision: str | None = "0001_create_vector_extension"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

DIM = 1024


def upgrade() -> None:
    op.create_table(
        "product_chunks",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("product_id", sa.UUID(), nullable=False),
        sa.Column("merchant_id", sa.UUID(), nullable=False),
        sa.Column("category_id", sa.UUID(), nullable=True),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("chunk_type", sa.Text(), nullable=False, comment="DomainEnum ChunkType"),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("embedding", Vector(DIM), nullable=False),
        sa.Column("token_count", sa.Integer(), nullable=False),
        sa.Column(
            "search_vector",
            postgresql.TSVECTOR(),
            sa.Computed("to_tsvector('english', content)", persisted=True),
            nullable=True,
        ),
        sa.Column("product_status", sa.Text(), nullable=False),
        sa.Column("price", sa.Numeric(12, 2), nullable=True),
        sa.Column("currency", sa.CHAR(3), nullable=True),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "indexed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_product_chunks")),
        sa.UniqueConstraint(
            "product_id", "content_hash", name=op.f("uq_product_chunks_product_id_content_hash")
        ),
    )
    op.create_index("ix_product_chunks_product_id", "product_chunks", ["product_id"])
    op.create_index("ix_product_chunks_merchant_id", "product_chunks", ["merchant_id"])
    op.create_index("ix_product_chunks_category_id", "product_chunks", ["category_id"])
    op.create_index("ix_product_chunks_product_status", "product_chunks", ["product_status"])
    op.create_index(
        "ix_product_chunks_status_category", "product_chunks", ["product_status", "category_id"]
    )
    op.create_index(
        "ix_product_chunks_search_vector",
        "product_chunks",
        ["search_vector"],
        postgresql_using="gin",
    )
    op.execute(
        "CREATE INDEX ix_product_chunks_embedding ON product_chunks "
        "USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)"
    )

    op.create_table(
        "embedding_cache",
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("embedding", Vector(DIM), nullable=False),
        sa.Column("token_count", sa.Integer(), nullable=False),
        sa.Column("hit_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "last_used_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("content_hash", name=op.f("pk_embedding_cache")),
    )
    op.create_index("ix_embedding_cache_last_used_at", "embedding_cache", ["last_used_at"])

    op.create_table(
        "index_state",
        sa.Column("product_id", sa.UUID(), nullable=False),
        sa.Column("merchant_id", sa.UUID(), nullable=True),
        sa.Column("catalog_version", sa.Integer(), nullable=False),
        sa.Column("chunk_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("embedding_model", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, comment="DomainEnum IndexStatus"),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("source_document", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("indexed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("product_id", name=op.f("pk_index_state")),
    )
    op.create_index("ix_index_state_status", "index_state", ["status"])
    op.create_index("ix_index_state_merchant_id", "index_state", ["merchant_id"])

    op.create_table(
        "query_log",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("query_text", sa.Text(), nullable=False),
        sa.Column(
            "filters",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("mode", sa.Text(), nullable=False, comment="DomainEnum SearchMode"),
        sa.Column(
            "result_product_ids",
            postgresql.ARRAY(sa.UUID()),
            server_default=sa.text("'{}'"),
            nullable=False,
        ),
        sa.Column("result_count", sa.Integer(), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=False),
        sa.Column("embedding_cached", sa.Boolean(), nullable=False),
        sa.Column("degraded", sa.Text(), nullable=True),
        sa.Column("user_id", sa.UUID(), nullable=True),
        sa.Column("correlation_id", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_query_log")),
    )
    op.create_index("ix_query_log_created_at", "query_log", ["created_at"])
    op.create_index("ix_query_log_user_id", "query_log", ["user_id"])
    op.create_index("ix_query_log_correlation_id", "query_log", ["correlation_id"])

    create_processed_events(op)


def downgrade() -> None:
    drop_processed_events(op)
    op.drop_table("query_log")
    op.drop_table("index_state")
    op.drop_table("embedding_cache")
    op.drop_table("product_chunks")
