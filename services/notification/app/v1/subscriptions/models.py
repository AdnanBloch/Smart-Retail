"""``push_subscriptions`` (RFC 0007 §3.1.3): specified, unused (NTF-D22).

The table exists so the offline channel is an adapter, not a migration; no
code reads or writes it until a Web Push adapter is registered.
"""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from smartretail_common.db import Base, CreatedAt, UUIDPrimaryKey


class PushSubscription(UUIDPrimaryKey, CreatedAt, Base):
    __tablename__ = "push_subscriptions"
    __table_args__ = (
        UniqueConstraint("user_id", "endpoint"),
        Index("ix_push_subscriptions_user_id", "user_id"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    endpoint: Mapped[str] = mapped_column(Text, nullable=False)
    p256dh: Mapped[str] = mapped_column(Text, nullable=False)
    auth: Mapped[str] = mapped_column(Text, nullable=False)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
