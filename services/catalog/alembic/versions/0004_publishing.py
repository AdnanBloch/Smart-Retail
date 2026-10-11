"""publishing: publish_workflow_id, search_document, product_publish_failures,
media storage and processing columns, outbox (RFC 0004 §3.1.3 "0002_publishing")

URL media rows that exist before this revision are `ready`: nothing of theirs
is stored by us, so there is nothing to process.

Revision ID: 0004_publishing
Revises: 0003_enum_comments_products
Create Date: 2026-10-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004_publishing"
down_revision: str | None = "0003_enum_comments_products"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("event_id", sa.UUID(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("aggregate_type", sa.Text(), nullable=False),
        sa.Column("aggregate_id", sa.UUID(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("correlation_id", sa.Text(), nullable=True),
        sa.Column("causation_id", sa.Text(), nullable=True),
        sa.Column("client_id", sa.Text(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox")),
        sa.UniqueConstraint("event_id", name=op.f("uq_outbox_event_id")),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["id"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "product_publish_failures",
        sa.Column("product_id", sa.UUID(), nullable=False),
        sa.Column("workflow_id", sa.Text(), nullable=False),
        sa.Column(
            "failed_step",
            sa.Text(),
            nullable=False,
            comment="DomainEnum PublishStep — see services/catalog/app/products/enums.py",
        ),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_product_publish_failures")),
    )
    op.create_index(
        "ix_product_publish_failures_created_at",
        "product_publish_failures",
        ["created_at"],
        unique=False,
    )
    op.create_index(
        "ix_product_publish_failures_product_id",
        "product_publish_failures",
        ["product_id"],
        unique=False,
    )
    op.add_column("product_media", sa.Column("storage_key", sa.Text(), nullable=True))
    op.add_column("product_media", sa.Column("content_type", sa.Text(), nullable=True))
    op.add_column("product_media", sa.Column("size_bytes", sa.BigInteger(), nullable=True))
    op.add_column(
        "product_media",
        sa.Column(
            "processing_status",
            sa.Text(),
            server_default=sa.text("'ready'"),
            nullable=False,
            comment="DomainEnum MediaProcessingStatus — see services/catalog/app/media/enums.py",
        ),
    )
    op.add_column(
        "product_media",
        sa.Column("derived", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column("products", sa.Column("publish_workflow_id", sa.Text(), nullable=True))
    op.add_column(
        "products",
        sa.Column("search_document", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("products", "search_document")
    op.drop_column("products", "publish_workflow_id")
    op.drop_column("product_media", "derived")
    op.drop_column("product_media", "processing_status")
    op.drop_column("product_media", "size_bytes")
    op.drop_column("product_media", "content_type")
    op.drop_column("product_media", "storage_key")
    op.drop_index("ix_product_publish_failures_product_id", table_name="product_publish_failures")
    op.drop_index("ix_product_publish_failures_created_at", table_name="product_publish_failures")
    op.drop_table("product_publish_failures")
    op.drop_index(
        "ix_outbox_unpublished",
        table_name="outbox",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_table("outbox")
