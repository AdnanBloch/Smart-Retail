"""initial catalog schema: categories, products, variants, media (URL form), audit log

Revision ID: 0001_initial
Revises:
Create Date: 2026-10-03
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = postgresql.UUID(as_uuid=True)


def _timestamps(updated: bool = True) -> list[sa.Column]:
    cols = [
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        )
    ]
    if updated:
        cols.append(
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            )
        )
    return cols


def _uuid_pk() -> sa.Column:
    return sa.Column("id", UUID, server_default=sa.text("gen_random_uuid()"), nullable=False)


def upgrade() -> None:
    op.create_table(
        "categories",
        _uuid_pk(),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("parent_id", UUID, nullable=True),
        *_timestamps(),
        sa.ForeignKeyConstraint(
            ["parent_id"], ["categories.id"], name="fk_categories_parent_id_categories"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_categories"),
        sa.UniqueConstraint("slug", name="uq_categories_slug"),
    )

    op.create_table(
        "products",
        _uuid_pk(),
        sa.Column(
            "merchant_id", UUID, nullable=False, comment="identity user id; a claim, not a FK"
        ),
        sa.Column("sku", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("category_id", UUID, nullable=True),
        sa.Column("base_price", sa.Numeric(12, 2), nullable=False),
        sa.Column("currency", sa.CHAR(3), server_default=sa.text("'USD'"), nullable=False),
        sa.Column(
            "status",
            sa.Text(),
            server_default=sa.text("'draft'"),
            nullable=False,
            comment=(
                "DomainEnum ProductStatus — see services/catalog/app/domain/enums.py; "
                "transitions in app/domain/state_machine.py"
            ),
        ),
        sa.Column(
            "attributes",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        *_timestamps(),
        sa.CheckConstraint("base_price >= 0", name="ck_products_base_price_non_negative"),
        sa.ForeignKeyConstraint(
            ["category_id"],
            ["categories.id"],
            name="fk_products_category_id_categories",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_products"),
        sa.UniqueConstraint("merchant_id", "sku", name="uq_products_merchant_id_sku"),
    )
    op.create_index("ix_products_merchant_id", "products", ["merchant_id"])
    op.create_index("ix_products_category_id", "products", ["category_id"])
    op.create_index("ix_products_status", "products", ["status"])
    op.create_index(
        "ix_products_status_created_at", "products", ["status", sa.text("created_at DESC")]
    )
    op.create_index("ix_products_attributes", "products", ["attributes"], postgresql_using="gin")

    op.create_table(
        "product_variants",
        _uuid_pk(),
        sa.Column("product_id", UUID, nullable=False),
        sa.Column("sku", sa.Text(), nullable=False),
        sa.Column("options", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("price_override", sa.Numeric(12, 2), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        *_timestamps(),
        sa.CheckConstraint(
            "price_override >= 0", name="ck_product_variants_price_override_non_negative"
        ),
        sa.ForeignKeyConstraint(
            ["product_id"],
            ["products.id"],
            name="fk_product_variants_product_id_products",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_product_variants"),
        sa.UniqueConstraint("product_id", "sku", name="uq_product_variants_product_id_sku"),
    )
    op.create_index("ix_product_variants_product_id", "product_variants", ["product_id"])

    op.create_table(
        "product_media",
        _uuid_pk(),
        sa.Column("product_id", UUID, nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("alt_text", sa.Text(), nullable=True),
        sa.Column("position", sa.Integer(), server_default=sa.text("0"), nullable=False),
        *_timestamps(updated=False),
        sa.ForeignKeyConstraint(
            ["product_id"],
            ["products.id"],
            name="fk_product_media_product_id_products",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_product_media"),
    )
    op.create_index("ix_product_media_product_id", "product_media", ["product_id"])

    op.create_table(
        "product_audit_log",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("product_id", UUID, nullable=False),
        sa.Column("actor_id", UUID, nullable=False),
        sa.Column(
            "action",
            sa.Text(),
            nullable=False,
            comment="DomainEnum ProductAuditAction — see services/catalog/app/domain/enums.py",
        ),
        sa.Column("changes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        *_timestamps(updated=False),
        sa.PrimaryKeyConstraint("id", name="pk_product_audit_log"),
    )
    op.create_index("ix_product_audit_log_product_id", "product_audit_log", ["product_id"])
    op.create_index("ix_product_audit_log_created_at", "product_audit_log", ["created_at"])


def downgrade() -> None:
    op.drop_table("product_audit_log")
    op.drop_table("product_media")
    op.drop_table("product_variants")
    op.drop_table("products")
    op.drop_table("categories")
