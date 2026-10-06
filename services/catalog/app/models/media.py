import uuid
from typing import Annotated

from pydantic import AnyUrl, Field, UrlConstraints
from sqlalchemy import ForeignKey, Index, Integer, Text, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.product import MediaOut
from app.models.response import StrictModel, Version
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


# --- request / response models ------------------------------------------

HttpsUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["https"], max_length=2048)]


class MediaCreate(StrictModel):
    url: HttpsUrl
    alt_text: Annotated[str, Field(max_length=500)] | None = None
    position: Annotated[int, Field(ge=0, le=10_000)] = 0
    version: Version


class MediaWritten(MediaOut):
    version: int = Field(description="The product's new version")
