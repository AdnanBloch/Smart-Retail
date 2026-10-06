"""``product_media`` table (RFC 0004 §3.1.3)."""

import uuid

from sqlalchemy import ForeignKey, Index, Integer, Text, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from smartretail_common.db import Base, CreatedAt, UUIDPrimaryKey


class ProductMedia(UUIDPrimaryKey, CreatedAt, Base):
    """URL-form media. Storage and processing columns arrive in Milestone 2."""

    __tablename__ = "product_media"
    __table_args__ = (Index("ix_product_media_product_id", "product_id"),)

    product_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("products.id", ondelete="CASCADE"), nullable=False
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    alt_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    position: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0"), default=0
    )
