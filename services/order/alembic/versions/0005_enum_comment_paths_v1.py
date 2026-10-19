"""Point enum column comments at the version-first layout (app/v1/<entity>/).

Entity folders moved under ``app/v1/`` (ARC-D23, mentor review 2026-10-13), so
every comment that names an enum or state-machine file is repointed. Comment
only; no data or type change.

Revision ID: 0005_enum_comment_paths_v1
Revises: 0004_orders_payment_method
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0005_enum_comment_paths_v1"
down_revision: str | None = "0004_orders_payment_method"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# (table, column, enum, entity, has a state machine)
COMMENTS = [
    ("orders", "status", "OrderStatus", "orders", True),
    ("orders", "payment_status", "PaymentStatus", "orders", True),
    ("orders", "shipment_status", "ShipmentStatus", "orders", True),
    ("idempotency_keys", "state", "IdempotencyState", "orders", False),
]


def _comment(enum: str, entity: str, machine: bool, base: str) -> str:
    text = f"DomainEnum {enum} — see services/order/{base}/{entity}/enums.py"
    return f"{text}; transitions in {base}/{entity}/state_machine.py" if machine else text


def upgrade() -> None:
    for table, column, enum, entity, machine in COMMENTS:
        op.alter_column(
            table,
            column,
            comment=_comment(enum, entity, machine, "app/v1"),
            existing_comment=_comment(enum, entity, machine, "app"),
        )


def downgrade() -> None:
    for table, column, enum, entity, machine in COMMENTS:
        op.alter_column(
            table,
            column,
            comment=_comment(enum, entity, machine, "app"),
            existing_comment=_comment(enum, entity, machine, "app/v1"),
        )
