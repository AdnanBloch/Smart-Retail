"""payment_attempts: the provider fake's dedupe state, shared by every worker (ORD-D36).

The fake used to keep it in process memory, so a second ``order-worker``
replica, or a restarted one, could charge an order twice. Mentor review
2026-10-20 §7.

Revision ID: 0006_payment_attempts
Revises: 0005_enum_comment_paths_v1
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006_payment_attempts"
down_revision: str | None = "0005_enum_comment_paths_v1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "payment_attempts",
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column(
            "kind",
            sa.Text(),
            nullable=False,
            comment="DomainEnum PaymentAttemptKind — see services/order/app/v1/orders/enums.py",
        ),
        sa.Column("order_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("payment_method", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=True),
        sa.Column("reference", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("attempts >= 1", name=op.f("ck_payment_attempts_attempts_positive")),
        sa.PrimaryKeyConstraint("idempotency_key", name=op.f("pk_payment_attempts")),
    )
    op.create_index("ix_payment_attempts_order_id", "payment_attempts", ["order_id"])


def downgrade() -> None:
    op.drop_index("ix_payment_attempts_order_id", table_name="payment_attempts")
    op.drop_table("payment_attempts")
