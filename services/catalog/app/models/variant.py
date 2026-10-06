import uuid
from decimal import Decimal
from typing import Any

from pydantic import Field, model_validator
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Numeric,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.product import VariantOut
from app.models.response import Money, OpenMap, OutModel, Sku, StrictModel, Version
from smartretail_common.db import Base, Timestamps, UUIDPrimaryKey


class ProductVariant(UUIDPrimaryKey, Timestamps, Base):
    """No stock column, deliberately: stock belongs to inventory (R3)."""

    __tablename__ = "product_variants"
    __table_args__ = (
        UniqueConstraint("product_id", "sku", name="uq_product_variants_product_id_sku"),
        CheckConstraint("price_override >= 0", name="price_override_non_negative"),
        Index("ix_product_variants_product_id", "product_id"),
    )

    product_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("products.id", ondelete="CASCADE"), nullable=False
    )
    sku: Mapped[str] = mapped_column(Text, nullable=False)
    options: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    price_override: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true"), default=True
    )


# --- request / response models ------------------------------------------


class VariantCreate(StrictModel):
    sku: Sku
    options: OpenMap
    price_override: Money | None = None
    version: Version


class VariantPatch(StrictModel):
    sku: Sku | None = None
    options: OpenMap | None = None
    price_override: Money | None = None
    is_active: bool | None = None
    version: Version

    @model_validator(mode="after")
    def _required_fields_not_null(self) -> "VariantPatch":
        for name in ("sku", "options", "is_active"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} cannot be null")
        return self


class VariantWritten(VariantOut):
    version: int = Field(description="The product's new version")


class VariantDeactivated(OutModel):
    id: uuid.UUID
    is_active: bool
    version: int
