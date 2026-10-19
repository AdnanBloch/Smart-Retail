"""``notifications`` and ``delivery_attempts`` (RFC 0007 §3.1.3)."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.v1.notifications.enums import (
    DeliveryStatus,
    NotificationChannel,
    NotificationDeliveryScope,
    NotificationType,
)
from smartretail_common.db import Base, CreatedAt, UUIDPrimaryKey
from smartretail_common.db_types import DomainEnumText

ENUMS = "services/notification/app/v1/notifications/enums.py"
TYPE_COMMENT = f"DomainEnum NotificationType — see {ENUMS}"
CHANNEL_COMMENT = f"DomainEnum NotificationChannel — see {ENUMS}"
SCOPE_COMMENT = f"DomainEnum NotificationDeliveryScope — see {ENUMS}"
STATUS_COMMENT = f"DomainEnum DeliveryStatus — see {ENUMS}"


class Notification(UUIDPrimaryKey, CreatedAt, Base):
    """One per ``(event, user, channel)`` (I1). ``title``/``body`` never change after insert.

    The lifecycle is derived from ``read_at``, ``delivered_at`` and
    ``promoted_at`` (NTF-13); there is no status column.
    """

    __tablename__ = "notifications"
    __table_args__ = (
        Index("ix_notifications_user_id_seq", "user_id", "seq"),
        Index("ix_notifications_user_id_created_at", "user_id", text("created_at DESC")),
        Index("ix_notifications_correlation_id", "correlation_id"),
        # The promoter's only query, over a normally near-empty set.
        Index(
            "ix_notifications_promotable",
            "created_at",
            postgresql_where=text(
                "delivered_at IS NULL AND promoted_at IS NULL "
                "AND delivery_scope = 'origin_first'"
            ),
        ),
        # I1 / NTF-1: at most one per (event, user, channel).
        Index(
            "uq_notifications_event_user_channel",
            text("(payload ->> 'event_id')"),
            "user_id",
            "channel",
            unique=True,
        ),
    )

    # The stream cursor (C18): bigint identity, the S1 exception.
    seq: Mapped[int] = mapped_column(BigInteger, Identity(), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    type: Mapped[NotificationType | str] = mapped_column(
        DomainEnumText(NotificationType), nullable=False, comment=TYPE_COMMENT
    )
    channel: Mapped[NotificationChannel | str] = mapped_column(
        DomainEnumText(NotificationChannel), nullable=False, comment=CHANNEL_COMMENT
    )
    title: Mapped[str] = mapped_column(Text, nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    # The whole source envelope (S8, NTF-1); never filtered except by the unique index.
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    origin_client_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    delivery_scope: Mapped[NotificationDeliveryScope] = mapped_column(
        DomainEnumText(
            NotificationDeliveryScope,
            on_unknown="fallback",
            fallback=NotificationDeliveryScope.ALL_DEVICES,
        ),
        nullable=False,
        server_default=text("'all_devices'"),
        comment=SCOPE_COMMENT,
    )
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delivered_client_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    promoted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DeliveryAttempt(UUIDPrimaryKey, Base):
    """Append-only: a retry inserts attempt *n + 1* (NTF-D20). ``in_app`` has none (NTF-4)."""

    __tablename__ = "delivery_attempts"
    __table_args__ = (
        Index("ix_delivery_attempts_notification_id", "notification_id"),
        Index("ix_delivery_attempts_created_at", "created_at"),
    )
    __mapper_args__ = {"eager_defaults": True}

    notification_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("notifications.id", ondelete="CASCADE"),
        nullable=False,
    )
    channel: Mapped[NotificationChannel | str] = mapped_column(
        DomainEnumText(NotificationChannel), nullable=False, comment=CHANNEL_COMMENT
    )
    status: Mapped[DeliveryStatus] = mapped_column(
        DomainEnumText(DeliveryStatus, on_unknown="fallback", fallback=DeliveryStatus.PENDING),
        nullable=False,
        comment=STATUS_COMMENT,
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
