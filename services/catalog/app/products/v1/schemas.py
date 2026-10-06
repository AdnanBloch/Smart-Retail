"""``/api/v1/products`` request and response models."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import Field, model_validator

from app.categories.v1.schemas import CategoryRef
from app.core.schemas import Currency, Money, OpenMap, OutModel, Sku, StrictModel, Version
from app.media.v1.schemas import MediaOut
from app.products.enums import ProductAuditAction, ProductStatus
from app.products.listing_repo import Listing
from app.products.models import Product
from app.variants.v1.schemas import VariantOut
from smartretail_common.enums import widen_for_response

Name = Annotated[str, Field(min_length=1, max_length=300)]
Description = Annotated[str, Field(max_length=20_000)]
StatusOut = Annotated[ProductStatus | str, widen_for_response(ProductStatus)]


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

    @classmethod
    def from_listing(cls, listing: Listing) -> "ProductSummary":
        summary = cls.model_validate(listing.product)
        summary.primary_media_url = listing.primary_media_url
        return summary


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

    @classmethod
    def from_product(cls, product: Product) -> "ProductDetail":
        """``product`` must have its category, variants and media loaded."""
        return cls(
            id=product.id,
            merchant_id=product.merchant_id,
            sku=product.sku,
            name=product.name,
            description=product.description,
            category_id=product.category_id,
            category=CategoryRef.model_validate(product.category) if product.category else None,
            base_price=product.base_price,
            currency=product.currency,
            status=product.status,
            attributes=product.attributes,
            published_at=product.published_at,
            version=product.version,
            created_at=product.created_at,
            updated_at=product.updated_at,
            variants=[VariantOut.from_variant(v) for v in product.variants],
            media=[MediaOut.model_validate(m) for m in product.media],
        )


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
