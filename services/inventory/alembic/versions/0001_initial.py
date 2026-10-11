"""initial inventory schema: stock_items, reservations, stock_movements, outbox

Revision ID: 0001_initial
Revises:
Create Date: 2026-10-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: str | None = None
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
        "stock_items",
        sa.Column("variant_id", sa.UUID(), nullable=False),
        sa.Column("product_id", sa.UUID(), nullable=False),
        sa.Column("merchant_id", sa.UUID(), nullable=False),
        sa.Column("sku", sa.Text(), nullable=False),
        sa.Column("on_hand", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("reserved", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "available",
            sa.Integer(),
            sa.Computed("on_hand - reserved", persisted=True),
            nullable=False,
        ),
        sa.Column("low_stock_threshold", sa.Integer(), server_default=sa.text("5"), nullable=False),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "on_hand >= reserved AND reserved >= 0", name=op.f("ck_stock_items_quantities")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stock_items")),
        sa.UniqueConstraint("variant_id", name=op.f("uq_stock_items_variant_id")),
    )
    op.create_index("ix_stock_items_merchant_id", "stock_items", ["merchant_id"], unique=False)
    op.create_index("ix_stock_items_product_id", "stock_items", ["product_id"], unique=False)
    op.create_table(
        "stock_movements",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("variant_id", sa.UUID(), nullable=False),
        sa.Column("delta_on_hand", sa.Integer(), nullable=False),
        sa.Column("delta_reserved", sa.Integer(), nullable=False),
        sa.Column(
            "reason",
            sa.Text(),
            nullable=False,
            comment="DomainEnum StockMovementReason — see services/inventory/app/stock/enums.py",
        ),
        sa.Column("reference_type", sa.Text(), nullable=True),
        sa.Column("reference_id", sa.UUID(), nullable=True),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column("correlation_id", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stock_movements")),
    )
    op.create_index(
        "ix_stock_movements_created_at", "stock_movements", ["created_at"], unique=False
    )
    op.create_index(
        "ix_stock_movements_reference",
        "stock_movements",
        ["reference_type", "reference_id"],
        unique=False,
    )
    op.create_index(
        "ix_stock_movements_variant_id", "stock_movements", ["variant_id"], unique=False
    )
    op.create_table(
        "reservations",
        sa.Column("order_id", sa.UUID(), nullable=False),
        sa.Column("variant_id", sa.UUID(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            comment=(
                "DomainEnum ReservationStatus — see services/inventory/app/reservations/enums.py; "
                "transitions in app/reservations/state_machine.py"
            ),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("quantity > 0", name=op.f("ck_reservations_quantity_positive")),
        sa.ForeignKeyConstraint(
            ["variant_id"],
            ["stock_items.variant_id"],
            name=op.f("fk_reservations_variant_id_stock_items"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_reservations")),
        sa.UniqueConstraint("order_id", "variant_id", name="uq_reservations_order_id_variant_id"),
    )
    op.create_index(
        "ix_reservations_expires_at_held",
        "reservations",
        ["expires_at"],
        unique=False,
        postgresql_where=sa.text("status = 'held'"),
    )
    op.create_index("ix_reservations_variant_id", "reservations", ["variant_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_reservations_variant_id", table_name="reservations")
    op.drop_index(
        "ix_reservations_expires_at_held",
        table_name="reservations",
        postgresql_where=sa.text("status = 'held'"),
    )
    op.drop_table("reservations")
    op.drop_index("ix_stock_movements_variant_id", table_name="stock_movements")
    op.drop_index("ix_stock_movements_reference", table_name="stock_movements")
    op.drop_index("ix_stock_movements_created_at", table_name="stock_movements")
    op.drop_table("stock_movements")
    op.drop_index("ix_stock_items_product_id", table_name="stock_items")
    op.drop_index("ix_stock_items_merchant_id", table_name="stock_items")
    op.drop_table("stock_items")
    op.drop_index(
        "ix_outbox_unpublished",
        table_name="outbox",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_table("outbox")
