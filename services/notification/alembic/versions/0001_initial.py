"""notification_db: notifications, delivery_attempts, push_subscriptions, processed_events.

RFC 0007 §3.1.3. ``notifications.seq`` is a bigint identity (S1 exception,
C18); at-most-one per (event, user, channel) is a unique expression index
on the envelope's ``event_id`` (NTF-1). No outbox: notification produces
nothing (§3.1.2.3).

Revision ID: 0001_initial
Revises:
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from smartretail_common.migrations import create_processed_events, drop_processed_events

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "notifications",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("seq", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("correlation_id", sa.Text(), nullable=True),
        sa.Column("origin_client_id", sa.Text(), nullable=True),
        sa.Column(
            "delivery_scope", sa.Text(), server_default=sa.text("'all_devices'"), nullable=False
        ),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivered_client_id", sa.Text(), nullable=True),
        sa.Column("promoted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_notifications")),
    )
    op.create_index("ix_notifications_user_id_seq", "notifications", ["user_id", "seq"])
    op.create_index(
        "ix_notifications_user_id_created_at",
        "notifications",
        ["user_id", sa.text("created_at DESC")],
    )
    op.create_index("ix_notifications_correlation_id", "notifications", ["correlation_id"])
    op.create_index(
        "ix_notifications_promotable",
        "notifications",
        ["created_at"],
        postgresql_where=sa.text(
            "delivered_at IS NULL AND promoted_at IS NULL AND delivery_scope = 'origin_first'"
        ),
    )
    op.create_index(
        "uq_notifications_event_user_channel",
        "notifications",
        [sa.text("(payload ->> 'event_id')"), "user_id", "channel"],
        unique=True,
    )

    op.create_table(
        "delivery_attempts",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("notification_id", sa.UUID(), nullable=False),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["notification_id"],
            ["notifications.id"],
            name=op.f("fk_delivery_attempts_notification_id_notifications"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_delivery_attempts")),
    )
    op.create_index(
        "ix_delivery_attempts_notification_id", "delivery_attempts", ["notification_id"]
    )
    op.create_index("ix_delivery_attempts_created_at", "delivery_attempts", ["created_at"])

    op.create_table(
        "push_subscriptions",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("endpoint", sa.Text(), nullable=False),
        sa.Column("p256dh", sa.Text(), nullable=False),
        sa.Column("auth", sa.Text(), nullable=False),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column(
            "last_seen_at",
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
        sa.PrimaryKeyConstraint("id", name=op.f("pk_push_subscriptions")),
        sa.UniqueConstraint(
            "user_id", "endpoint", name=op.f("uq_push_subscriptions_user_id_endpoint")
        ),
    )
    op.create_index("ix_push_subscriptions_user_id", "push_subscriptions", ["user_id"])

    create_processed_events(op)


def downgrade() -> None:
    drop_processed_events(op)
    op.drop_index("ix_push_subscriptions_user_id", table_name="push_subscriptions")
    op.drop_table("push_subscriptions")
    op.drop_index("ix_delivery_attempts_created_at", table_name="delivery_attempts")
    op.drop_index("ix_delivery_attempts_notification_id", table_name="delivery_attempts")
    op.drop_table("delivery_attempts")
    for index in (
        "uq_notifications_event_user_channel",
        "ix_notifications_promotable",
        "ix_notifications_correlation_id",
        "ix_notifications_user_id_created_at",
        "ix_notifications_user_id_seq",
    ):
        op.drop_index(index, table_name="notifications")
    op.drop_table("notifications")
