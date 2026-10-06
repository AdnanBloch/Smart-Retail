"""Product lifecycle: create, update, status, archive, history (RFC 0004 §3.1.4).

Every write locks the product row, checks ownership and ``version``, bumps the
version and writes the audit row in the same transaction.
"""

import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.exception_handlers.exception_handlers import (
    DuplicateSku,
    ProductNotFound,
    PublishingNotAvailable,
    unknown_reference,
)
from app.models import Category, Product, ProductAuditLog
from app.models.category import CategoryRef
from app.models.enums import ProductAuditAction, ProductStatus
from app.models.product import (
    ArchivedOut,
    AuditEntry,
    MediaOut,
    ProductCreate,
    ProductDetail,
    ProductPatch,
    StatusChange,
    VariantLinks,
    VariantOut,
)
from app.services import audit_repo
from app.utils import access, helpers
from app.utils.state_machine import transition
from smartretail_common.auth import Principal
from smartretail_common.errors import Unprocessable
from smartretail_common.pagination import Page, PageParams

AVAILABILITY_LINK = "/api/v1/inventory/{variant_id}"
EDITABLE_FIELDS = (
    "sku",
    "name",
    "description",
    "category_id",
    "base_price",
    "currency",
    "attributes",
)


def variant_out(variant: Any) -> VariantOut:
    out = VariantOut.model_validate(variant)
    out.links = VariantLinks(availability=AVAILABILITY_LINK.format(variant_id=variant.id))
    return out


def detail_out(product: Product) -> ProductDetail:
    return ProductDetail(
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
        variants=[variant_out(v) for v in product.variants],
        media=[MediaOut.model_validate(m) for m in product.media],
    )


async def lock_product(session: AsyncSession, product_id: uuid.UUID) -> Product:
    product = await session.scalar(
        select(Product).where(Product.id == product_id).with_for_update()
    )
    if product is None:
        raise ProductNotFound()
    return product


async def load_detail(session: AsyncSession, product_id: uuid.UUID) -> Product:
    product = await session.scalar(
        select(Product)
        .where(Product.id == product_id)
        .options(
            selectinload(Product.variants),
            selectinload(Product.media),
            selectinload(Product.category),
        )
        .execution_options(populate_existing=True)
    )
    if product is None:
        raise ProductNotFound()
    return product


def bump(product: Product) -> None:
    product.version += 1


class ProductRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, product_id: uuid.UUID, principal: Principal | None) -> ProductDetail:
        product = await load_detail(self.session, product_id)
        access.ensure_visible(principal, product)
        return detail_out(product)

    async def create(self, principal: Principal, data: ProductCreate) -> ProductDetail:
        if access.is_admin(principal):
            if data.merchant_id is None:
                raise Unprocessable(
                    "Admins must say which merchant owns the product",
                    violations=[
                        {
                            "field": "merchant_id",
                            "message": "required for admins",
                            "code": "missing",
                        }
                    ],
                )
            merchant_id = data.merchant_id
        else:
            # A merchant's body merchant_id is ignored, not obeyed (I4).
            owner = access.caller_merchant_id(principal)
            assert owner is not None  # guaranteed by the route's role check
            merchant_id = owner
        await self._ensure_category(data.category_id)

        product = Product(
            id=uuid.uuid4(),
            merchant_id=merchant_id,
            status=ProductStatus.DRAFT,
            version=1,
            **data.model_dump(exclude={"merchant_id"}),
        )
        self.session.add(product)
        await self._flush()
        audit_repo.record(
            self.session,
            product_id=product.id,
            actor_id=principal.subject,
            action=ProductAuditAction.CREATED,
            changes={
                "after": {
                    f: getattr(product, f) for f in (*EDITABLE_FIELDS, "merchant_id", "status")
                }
            },
        )
        await self.session.commit()
        return detail_out(await load_detail(self.session, product.id))

    async def update(
        self, principal: Principal, product_id: uuid.UUID, data: ProductPatch
    ) -> ProductDetail:
        product = await lock_product(self.session, product_id)
        access.ensure_can_manage(principal, product)
        access.ensure_mutable(product)
        access.ensure_version(product, data.version)

        requested = data.model_dump(exclude_unset=True, exclude={"version"})
        if requested.get("category_id") is not None:
            await self._ensure_category(requested["category_id"])
        changes = helpers.diff({f: getattr(product, f) for f in requested}, requested)
        if changes:
            for field in changes:
                setattr(product, field, requested[field])
            bump(product)
            await self._flush()
            audit_repo.record(
                self.session,
                product_id=product.id,
                actor_id=principal.subject,
                action=ProductAuditAction.UPDATED,
                changes=changes,
            )
        await self.session.commit()
        return detail_out(await load_detail(self.session, product.id))

    async def change_status(
        self, principal: Principal, product_id: uuid.UUID, data: StatusChange
    ) -> ProductDetail:
        target = ProductStatus(data.status)
        product = await lock_product(self.session, product_id)
        access.ensure_can_manage(principal, product)
        if target == ProductStatus.PUBLISHED:
            raise PublishingNotAvailable()
        access.ensure_version(product, data.version)
        await self._transition(principal, product, target)
        await self.session.commit()
        return detail_out(await load_detail(self.session, product.id))

    async def archive(
        self, principal: Principal, product_id: uuid.UUID, version: int
    ) -> ArchivedOut:
        """Soft delete (S5): products are referenced across boundaries, never removed."""
        product = await lock_product(self.session, product_id)
        access.ensure_can_manage(principal, product)
        access.ensure_version(product, version)
        await self._transition(principal, product, ProductStatus.ARCHIVED)
        await self.session.commit()
        return ArchivedOut(id=product.id, status=product.status, version=product.version)

    async def history(
        self, principal: Principal, product_id: uuid.UUID, page: PageParams
    ) -> Page[AuditEntry]:
        product = await self.session.get(Product, product_id)
        if product is None:
            raise ProductNotFound()
        access.ensure_can_manage(principal, product)
        where = ProductAuditLog.product_id == product_id
        total = await self.session.scalar(
            select(func.count()).select_from(ProductAuditLog).where(where)
        )
        rows = await self.session.scalars(
            select(ProductAuditLog)
            .where(where)
            .order_by(ProductAuditLog.created_at.desc(), ProductAuditLog.id.desc())
            .limit(page.limit)
            .offset(page.offset)
        )
        return Page[AuditEntry](
            items=[AuditEntry.model_validate(r) for r in rows],
            total=total or 0,
            limit=page.limit,
            offset=page.offset,
        )

    async def _transition(
        self, principal: Principal, product: Product, target: ProductStatus
    ) -> None:
        previous = transition(product, target)
        bump(product)
        audit_repo.record(
            self.session,
            product_id=product.id,
            actor_id=principal.subject,
            action=(
                ProductAuditAction.DELETED
                if target == ProductStatus.ARCHIVED
                else ProductAuditAction.STATUS_CHANGED
            ),
            changes={"status": {"from": str(previous), "to": str(target)}},
        )

    async def _ensure_category(self, category_id: uuid.UUID | None) -> None:
        if category_id is not None and await self.session.get(Category, category_id) is None:
            raise unknown_reference("category_id", "Category does not exist")

    async def _flush(self) -> None:
        try:
            await self.session.flush()
        except IntegrityError as exc:
            await self.session.rollback()
            if "uq_products_merchant_id_sku" in str(exc.orig):
                raise DuplicateSku("This merchant already has a product with that SKU") from exc
            raise
