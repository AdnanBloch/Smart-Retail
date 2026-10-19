"""Point the idempotency_keys.state comment at app/orders/enums.py.

The ``Idempotency-Key`` model and its states moved from ``app/core/`` into the
``orders`` entity (ARC-D27). Comment only; no data or type change.

Revision ID: 0002_idempotency_state_comment
Revises: 0001_initial
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002_idempotency_state_comment"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "idempotency_keys",
        "state",
        comment="DomainEnum IdempotencyState — see services/order/app/orders/enums.py",
        existing_comment="DomainEnum IdempotencyState — see services/order/app/core/enums.py",
    )


def downgrade() -> None:
    op.alter_column(
        "idempotency_keys",
        "state",
        comment="DomainEnum IdempotencyState — see services/order/app/core/enums.py",
        existing_comment="DomainEnum IdempotencyState — see services/order/app/orders/enums.py",
    )
