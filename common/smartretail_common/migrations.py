"""DDL of the two shared tables, for services' Alembic migrations (RFC 0000 §3.1.3.2).

``outbox`` (S12) and ``processed_events`` (S13) are identical in every database
that carries them, so their DDL is written once here; a migration calls these
with its ``op``. Kept in step with the ORM models in ``outbox.py`` and
``events/dedupe.py`` — each service's schema test compares migrations to models.
"""

from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from smartretail_common.outbox import RELAY_GRANT_SQL, RELAY_REVOKE_SQL


def create_outbox(op: Any, *, grant_relay: bool = True) -> None:
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
    if grant_relay:
        op.execute(RELAY_GRANT_SQL)


def drop_outbox(op: Any) -> None:
    op.execute(RELAY_REVOKE_SQL)
    op.drop_index("ix_outbox_unpublished", table_name="outbox")
    op.drop_table("outbox")


def create_processed_events(op: Any) -> None:
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


def drop_processed_events(op: Any) -> None:
    op.drop_index("ix_processed_events_processed_at", table_name="processed_events")
    op.drop_table("processed_events")
