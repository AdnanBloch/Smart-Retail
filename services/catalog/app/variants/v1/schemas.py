"""``/api/v1/variants`` request and response models."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import Field, model_validator

from app.core.schemas import Money, OpenMap, OutModel, Sku, StrictModel, Version
from app.variants.models import ProductVariant
from app.variants.repo import VariantWrite

# Where a client reads a variant's stock; inventory owns it (R3).
AVAILABILITY_LINK = "/api/v1/inventory/{variant_id}"


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

    @classmethod
    def from_variant(cls, variant: ProductVariant) -> "VariantOut":
        out = cls.model_validate(variant)
        out.links = VariantLinks(availability=AVAILABILITY_LINK.format(variant_id=variant.id))
        return out


class VariantWritten(VariantOut):
    version: int = Field(description="The product's new version")

    @classmethod
    def from_write(cls, write: VariantWrite) -> "VariantWritten":
        fields = VariantOut.from_variant(write.variant).model_dump()
        return cls.model_validate({**fields, "version": write.product_version})


class VariantDeactivated(OutModel):
    id: uuid.UUID
    is_active: bool
    version: int

    @classmethod
    def from_write(cls, write: VariantWrite) -> "VariantDeactivated":
        return cls(id=write.variant.id, is_active=False, version=write.product_version)
