import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Annotated, Any, Literal

from pydantic import Field, model_validator
from sqlalchemy import (
    CHAR,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.category import CategoryRef
from app.models.enums import ProductAuditAction, ProductStatus
from app.models.response import Currency, Money, OpenMap, OutModel, Sku, StrictModel, Version
from smartretail_common.db import Base, Timestamps, UUIDPrimaryKey
from smartretail_common.db_types import DomainEnumText
from smartretail_common.enums import widen_for_response

if TYPE_CHECKING:
    from app.models.category import Category
    from app.models.media import ProductMedia
    from app.models.variant import ProductVariant


class Product(UUIDPrimaryKey, Timestamps, Base):
    __tablename__ = "products"
    __table_args__ = (
        UniqueConstraint("merchant_id", "sku", name="uq_products_merchant_id_sku"),
        CheckConstraint("base_price >= 0", name="base_price_non_negative"),
        Index("ix_products_merchant_id", "merchant_id"),
        Index("ix_products_category_id", "category_id"),
        Index("ix_products_status", "status"),
        Index("ix_products_status_created_at", "status", text("created_at DESC")),
        Index("ix_products_attributes", "attributes", postgresql_using="gin"),
    )

    # A claim from identity, not a foreign key (RFC 0000 S15).
    merchant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, comment="identity user id; a claim, not a FK"
    )
    sku: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    category_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("categories.id", ondelete="RESTRICT"), nullable=True
    )
    base_price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), nullable=False, server_default=text("'USD'"))
    status: Mapped[ProductStatus | str] = mapped_column(
        DomainEnumText(ProductStatus),
        nullable=False,
        server_default=text("'draft'"),
        default=ProductStatus.DRAFT,
        comment=(
            "DomainEnum ProductStatus — see services/catalog/app/models/enums.py; "
            "transitions in app/utils/state_machine.py"
        ),
    )
    attributes: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb"), default=dict
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    version: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("1"), default=1
    )

    category: Mapped["Category | None"] = relationship(lazy="raise")
    variants: Mapped[list["ProductVariant"]] = relationship(
        lazy="raise", order_by="ProductVariant.created_at", passive_deletes=True
    )
    media: Mapped[list["ProductMedia"]] = relationship(
        lazy="raise",
        order_by="(ProductMedia.position, ProductMedia.created_at)",
        passive_deletes=True,
    )


# --- request / response models ------------------------------------------

Name = Annotated[str, Field(min_length=1, max_length=300)]
Description = Annotated[str, Field(max_length=20_000)]
StatusOut = Annotated[ProductStatus | str, widen_for_response(ProductStatus)]

SortKey = Literal["created_at", "-created_at", "price", "-price", "name", "-name"]


class ProductCreate(StrictModel):
    sku: Sku
    name: Name
    description: Description | None = None
    category_id: uuid.UUID | None = None
    base_price: Money
    currency: Currency = "USD"
    attributes: OpenMap = Field(default_factory=dict)
    # Required for admins; ignored for merchants, whose id comes from the token.
    merchant_id: uuid.UUID | None = None


class ProductPatch(StrictModel):
    sku: Sku | None = None
    name: Name | None = None
    description: Description | None = None
    category_id: uuid.UUID | None = None
    base_price: Money | None = None
    currency: Currency | None = None
    attributes: OpenMap | None = None
    version: Version

    @model_validator(mode="after")
    def _required_fields_not_null(self) -> "ProductPatch":
        for name in ("sku", "name", "base_price", "currency", "attributes"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} cannot be null")
        return self


class StatusChange(StrictModel):
    # `publishing` is never accepted from a client.
    status: Literal["draft", "inactive", "published", "archived"]
    version: Version


class VariantLinks(OutModel):
    availability: str


class VariantOut(OutModel):
    id: uuid.UUID
    product_id: uuid.UUID
    sku: str
    options: dict[str, Any]
    price_override: Decimal | None
    is_active: bool
    created_at: datetime
    updated_at: datetime
    links: VariantLinks | None = Field(default=None, serialization_alias="_links")


class MediaOut(OutModel):
    id: uuid.UUID
    product_id: uuid.UUID
    url: str
    alt_text: str | None
    position: int
    created_at: datetime


class ProductSummary(OutModel):
    id: uuid.UUID
    merchant_id: uuid.UUID
    sku: str
    name: str
    base_price: Decimal
    currency: str
    status: StatusOut
    category_id: uuid.UUID | None
    primary_media_url: str | None = None
    published_at: datetime | None


class ProductDetail(OutModel):
    id: uuid.UUID
    merchant_id: uuid.UUID
    sku: str
    name: str
    description: str | None
    category_id: uuid.UUID | None
    category: CategoryRef | None
    base_price: Decimal
    currency: str
    status: StatusOut
    attributes: dict[str, Any]
    published_at: datetime | None
    version: int
    created_at: datetime
    updated_at: datetime
    variants: list[VariantOut]
    media: list[MediaOut]


class ArchivedOut(OutModel):
    id: uuid.UUID
    status: StatusOut
    version: int


class AuditEntry(OutModel):
    id: int
    actor_id: uuid.UUID
    action: Annotated[ProductAuditAction | str, widen_for_response(ProductAuditAction)]
    changes: dict[str, Any]
    created_at: datetime
