"""``stream_cursors`` (RFC 0007 §3.1.3): the per-``(user, device)`` replay floor (C22)."""

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Index, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from smartretail_common.db import Base, Timestamps, UUIDPrimaryKey


class StreamCursor(UUIDPrimaryKey, Timestamps, Base):
    __tablename__ = "stream_cursors"
    __table_args__ = (
        UniqueConstraint("user_id", "client_id"),
        Index("ix_stream_cursors_last_seen_at", "last_seen_at"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    # The client-minted token (≤ 64 chars), always scoped under user_id (I7).
    client_id: Mapped[str] = mapped_column(Text, nullable=False)
    # Mirrors notifications.seq; only ever raised (I5).
    cursor_seq: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
