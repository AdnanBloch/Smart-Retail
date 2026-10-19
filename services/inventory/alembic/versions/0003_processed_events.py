"""processed_events: inventory becomes a Kafka consumer (RFC 0000 §3.1.3.2, A11).

Consumer group ``inventory`` records each handled ``event_id`` here in the
same transaction as the effect (RFC 0006 §3.1.2.4).

Revision ID: 0003_processed_events
Revises: 0002_outbox_relay_grant
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_processed_events"
down_revision: str | None = "0002_outbox_relay_grant"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "processed_events",
        sa.Column("consumer_group", sa.Text(), nullable=False),
        sa.Column("event_id", sa.UUID(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("consumer_group", "event_id", name=op.f("pk_processed_events")),
    )
    op.create_index(
        "ix_processed_events_processed_at", "processed_events", ["processed_at"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_processed_events_processed_at", table_name="processed_events")
    op.drop_table("processed_events")
