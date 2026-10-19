"""``categories`` table (RFC 0004 §3.1.3)."""

import uuid

from sqlalchemy import ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from smartretail_common.db import Base, Timestamps, UUIDPrimaryKey


class Category(UUIDPrimaryKey, Timestamps, Base):
    __tablename__ = "categories"

    name: Mapped[str] = mapped_column(Text, nullable=False)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    parent_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("categories.id"), nullable=True
    )
