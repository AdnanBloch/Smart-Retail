"""Variants and URL media (RFC 0004 §3.1.2). Writes bump the root product's
version (CAT-1) and are audited against the product."""

import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.exception_handlers.exception_handlers import DuplicateSku, MediaNotFound, VariantNotFound
from app.models import Product, ProductMedia, ProductVariant
from app.models.enums import ProductAuditAction
from app.models.media import MediaCreate, MediaWritten
from app.models.product import VariantLinks
from app.models.variant import (
    VariantCreate,
    VariantDeactivated,
    VariantPatch,
    VariantWritten,
)
from app.services import audit_repo
from app.services.product_repo import AVAILABILITY_LINK, bump, lock_product
from app.utils import access, helpers
from smartretail_common.auth import Principal


class VariantRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def _guarded_product(
        self, principal: Principal, product_id: uuid.UUID, version: int
    ) -> Product:
        product = await lock_product(self.session, product_id)
        access.ensure_can_manage(principal, product)
        access.ensure_mutable(product)
        access.ensure_version(product, version)
        return product

    async def _variant(self, variant_id: uuid.UUID) -> ProductVariant:
        variant = await self.session.get(ProductVariant, variant_id)
        if variant is None:
            raise VariantNotFound()
        return variant

    def _written(self, variant: ProductVariant, product: Product) -> VariantWritten:
        out = VariantWritten.model_validate({**variant.__dict__, "version": product.version})
        out.links = VariantLinks(availability=AVAILABILITY_LINK.format(variant_id=variant.id))
        return out

    async def add(
        self, principal: Principal, product_id: uuid.UUID, data: VariantCreate
    ) -> VariantWritten:
        product = await self._guarded_product(principal, product_id, data.version)
        variant = ProductVariant(
            id=uuid.uuid4(),
            product_id=product.id,
            sku=data.sku,
            options=data.options,
            price_override=data.price_override,
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
            changes={"variant_id": variant.id, "after": data.model_dump(exclude={"version"})},
        )
        await self.session.commit()
        return self._written(variant, product)

    async def update(
        self, principal: Principal, variant_id: uuid.UUID, data: VariantPatch
    ) -> VariantWritten:
        variant = await self._variant(variant_id)
        product = await self._guarded_product(principal, variant.product_id, data.version)
        requested = data.model_dump(exclude_unset=True, exclude={"version"})
        changes = helpers.diff({f: getattr(variant, f) for f in requested}, requested)
        if changes:
            for field in changes:
                setattr(variant, field, requested[field])
            bump(product)
            await self._flush()
            audit_repo.record(
                self.session,
                product_id=product.id,
                actor_id=principal.subject,
                action=ProductAuditAction.VARIANT_UPDATED,
                changes={"variant_id": variant.id, **changes},
            )
        await self.session.commit()
        await self.session.refresh(variant)
        return self._written(variant, product)

    async def deactivate(
        self, principal: Principal, variant_id: uuid.UUID, version: int
    ) -> VariantDeactivated:
        """Never a hard delete (CAT-2): orders keep referencing the variant."""
        variant = await self._variant(variant_id)
        product = await self._guarded_product(principal, variant.product_id, version)
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
        await self.session.commit()
        return VariantDeactivated(id=variant.id, is_active=False, version=product.version)

    async def add_media(
        self, principal: Principal, product_id: uuid.UUID, data: MediaCreate
    ) -> MediaWritten:
        product = await self._guarded_product(principal, product_id, data.version)
        media = ProductMedia(
            id=uuid.uuid4(),
            product_id=product.id,
            url=str(data.url),
            alt_text=data.alt_text,
            position=data.position,
        )
        self.session.add(media)
        bump(product)
        await self.session.flush()
        audit_repo.record(
            self.session,
            product_id=product.id,
            actor_id=principal.subject,
            action=ProductAuditAction.MEDIA_ADDED,
            changes={"media_id": media.id, "url": media.url, "position": media.position},
        )
        await self.session.commit()
        return MediaWritten.model_validate({**media.__dict__, "version": product.version})

    async def remove_media(self, principal: Principal, media_id: uuid.UUID, version: int) -> None:
        media = await self.session.scalar(select(ProductMedia).where(ProductMedia.id == media_id))
        if media is None:
            raise MediaNotFound()
        product = await self._guarded_product(principal, media.product_id, version)
        await self.session.delete(media)
        bump(product)
        audit_repo.record(
            self.session,
            product_id=product.id,
            actor_id=principal.subject,
            action=ProductAuditAction.MEDIA_REMOVED,
            changes={"media_id": media.id, "url": media.url},
        )
        await self.session.commit()

    async def _flush(self) -> None:
        try:
            await self.session.flush()
        except IntegrityError as exc:
            await self.session.rollback()
            if "uq_product_variants_product_id_sku" in str(exc.orig):
                raise DuplicateSku("This product already has a variant with that SKU") from exc
            raise
