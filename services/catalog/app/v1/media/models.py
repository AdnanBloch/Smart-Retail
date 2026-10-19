"""``product_media`` table (RFC 0004 §3.1.3)."""

import uuid
from typing import Any

from sqlalchemy import BigInteger, ForeignKey, Index, Integer, Text, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.v1.media.enums import MediaProcessingStatus
from smartretail_common.db import Base, CreatedAt, UUIDPrimaryKey
from smartretail_common.db_types import DomainEnumText


class ProductMedia(UUIDPrimaryKey, CreatedAt, Base):
    """An image of a product: either an external URL (``ready``, nothing stored by
    us) or an upload to object storage (``pending`` until the publishing workflow's
    ``process_media`` step derives its sizes)."""

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
    # Null for URL media: "not stored by us" (S10).
    storage_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    processing_status: Mapped[MediaProcessingStatus | str] = mapped_column(
        DomainEnumText(
            MediaProcessingStatus, on_unknown="fallback", fallback=MediaProcessingStatus.PENDING
        ),
        nullable=False,
        server_default=text("'ready'"),
        default=MediaProcessingStatus.READY,
        comment="DomainEnum MediaProcessingStatus — see services/catalog/app/v1/media/enums.py",
    )
    # size name → storage key, written by process_media.
    derived: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
