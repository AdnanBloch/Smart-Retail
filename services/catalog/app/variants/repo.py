"""Variants (RFC 0004 §3.1.2). Writes bump the root product's version (CAT-1) and
are audited against the product."""

import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core import utils
from app.core.errors import DuplicateSku, VariantNotFound
from app.products import audit_repo
from app.products.enums import ProductAuditAction
from app.products.repo import after_published_change, bump, lock_for_change
from app.variants.models import ProductVariant
from smartretail_common.auth import Principal


@dataclass(frozen=True)
class VariantWrite:
    """A variant after a write, with the product version that write produced."""

    variant: ProductVariant
    product_version: int


class VariantRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def _variant(self, variant_id: uuid.UUID) -> ProductVariant:
        variant = await self.session.get(ProductVariant, variant_id)
        if variant is None:
            raise VariantNotFound()
        return variant

    async def add(
        self,
        principal: Principal,
        product_id: uuid.UUID,
        *,
        sku: str,
        options: dict[str, Any],
        price_override: Decimal | None,
        version: int,
    ) -> VariantWrite:
        product = await lock_for_change(self.session, principal, product_id, version)
        variant = ProductVariant(
            id=uuid.uuid4(),
            product_id=product.id,
            sku=sku,
            options=options,
            price_override=price_override,
            is_active=True,
        )
        self.session.add(variant)
        bump(product)
        await self._flush()
        audit_repo.record(
            self.session,
            product_id=product.id,
            actor_id=principal.subject,
            action=ProductAuditAction.VARIANT_ADDED,
            changes={
                "variant_id": variant.id,
                "after": {"sku": sku, "options": options, "price_override": price_override},
            },
        )
        await after_published_change(self.session, product.id, ["variants"])
        await self.session.commit()
        return VariantWrite(variant, product.version)

    async def update(
        self, principal: Principal, variant_id: uuid.UUID, changes: dict[str, Any], version: int
    ) -> VariantWrite:
        """Applies ``changes`` (the fields the caller sent) if ``version`` is current."""
        variant = await self._variant(variant_id)
        product = await lock_for_change(self.session, principal, variant.product_id, version)
        diff = utils.diff({f: getattr(variant, f) for f in changes}, changes)
        if diff:
            for field in diff:
                setattr(variant, field, changes[field])
            bump(product)
            await self._flush()
            audit_repo.record(
                self.session,
                product_id=product.id,
                actor_id=principal.subject,
                action=ProductAuditAction.VARIANT_UPDATED,
                changes={"variant_id": variant.id, **diff},
            )
            await after_published_change(self.session, product.id, ["variants"])
        await self.session.commit()
        await self.session.refresh(variant)
        return VariantWrite(variant, product.version)

    async def deactivate(
        self, principal: Principal, variant_id: uuid.UUID, version: int
    ) -> VariantWrite:
        """Never a hard delete (CAT-2): orders keep referencing the variant."""
        variant = await self._variant(variant_id)
        product = await lock_for_change(self.session, principal, variant.product_id, version)
        if variant.is_active:
            variant.is_active = False
            bump(product)
            audit_repo.record(
                self.session,
                product_id=product.id,
                actor_id=principal.subject,
                action=ProductAuditAction.VARIANT_REMOVED,
                changes={"variant_id": variant.id, "is_active": {"from": True, "to": False}},
            )
            # Deactivating the last active variant of a published product is a 409.
            await after_published_change(self.session, product.id, ["variants"])
        await self.session.commit()
        return VariantWrite(variant, product.version)

    async def _flush(self) -> None:
        try:
            await self.session.flush()
        except IntegrityError as exc:
            await self.session.rollback()
            if "uq_product_variants_product_id_sku" in str(exc.orig):
                raise DuplicateSku("This product already has a variant with that SKU") from exc
            raise
