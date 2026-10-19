"""COMMENT ON COLUMN for every enum-valued column (S4): text, no CHECK, a pointer
to the vocabulary in code.

Revision ID: 0003_enum_comments
Revises: 0002_stream_cursors
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_enum_comments"
down_revision: str | None = "0002_stream_cursors"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ENUMS = "services/notification/app/notifications/enums.py"
COMMENTS = [
    ("notifications", "type", "NotificationType"),
    ("notifications", "channel", "NotificationChannel"),
    ("notifications", "delivery_scope", "NotificationDeliveryScope"),
    ("delivery_attempts", "channel", "NotificationChannel"),
    ("delivery_attempts", "status", "DeliveryStatus"),
]


def upgrade() -> None:
    for table, column, enum in COMMENTS:
        op.alter_column(
            table,
            column,
            existing_type=sa.Text(),
            existing_nullable=False,
            comment=f"DomainEnum {enum} — see {ENUMS}",
        )


def downgrade() -> None:
    for table, column, enum in COMMENTS:
        op.alter_column(
            table,
            column,
            existing_type=sa.Text(),
            existing_nullable=False,
            comment=None,
            existing_comment=f"DomainEnum {enum} — see {ENUMS}",
        )
