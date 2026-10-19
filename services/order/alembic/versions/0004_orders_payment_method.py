"""orders.payment_method: what resume_unstarted_orders needs to start a workflow later.

ORD-Q5 settled: the token is an opaque provider identifier, never card data,
so it may be stored; a real provider would store its payment-intent id here.
Null for orders placed before this column, which the resume job skips.

Revision ID: 0004_orders_payment_method
Revises: 0003_outbox_relay_grant
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_orders_payment_method"
down_revision: str | None = "0003_outbox_relay_grant"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("orders", sa.Column("payment_method", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("orders", "payment_method")
