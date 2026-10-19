"""Product lifecycle: create, update, status, archive, history (RFC 0004 §3.1.4).

Every write locks the product row, checks ownership and ``version``, bumps the
version and writes the audit row in the same transaction. A write to a
``published`` product also rebuilds its search document and writes
``catalog.product.updated``; leaving ``published`` writes
``catalog.product.unpublished`` — both in that same transaction (B11).
Publishing itself is ``publishing_repo``'s.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core import utils
from app.core.errors import DuplicateSku, ProductNotFound, WouldBreakPublished, unknown_reference
from app.v1.categories.models import Category
from app.v1.categories.repo import CategoryRepo
from app.v1.products import access, audit_repo, completeness, events, search_document
from app.v1.products.enums import ProductAuditAction, ProductStatus, UnpublishReason
from app.v1.products.models import Product, ProductAuditLog
from app.v1.products.state_machine import transition
from smartretail_common.auth import Principal
from smartretail_common.errors import Unprocessable
from smartretail_common.pagination import PageParams, Slice

EDITABLE_FIELDS = (
    "sku",
    "name",
    "description",
    "category_id",
    "base_price",
    "currency",
    "attributes",
)


async def lock_product(session: AsyncSession, product_id: uuid.UUID) -> Product:
    product = await session.scalar(
        select(Product).where(Product.id == product_id).with_for_update()
    )
    if product is None:
        raise ProductNotFound()
    return product


async def lock_for_change(
    session: AsyncSession, principal: Principal, product_id: uuid.UUID, version: int
) -> Product:
    """Locks a product its caller may change: owner or admin, not archived, same version.

    Used by every write to the product and to its variants and media.
    """
    product = await lock_product(session, product_id)
    access.ensure_can_manage(principal, product)
    access.ensure_mutable(product)
    access.ensure_not_publishing(product)
    access.ensure_version(product, version)
    return product


async def load_detail(session: AsyncSession, product_id: uuid.UUID) -> Product:
    """The product with its category, variants and media loaded."""
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


async def after_published_change(
    session: AsyncSession, product_id: uuid.UUID, changed_fields: list[str]
) -> None:
    """CAT-13: a committed change to a published product keeps it complete,
    rebuilds its search document and writes ``catalog.product.updated``.

    Called after the change is applied and before the caller commits; raising
    here leaves the caller's transaction uncommitted, so the change is undone.
    """
    product = await load_detail(session, product_id)
    if product.status != ProductStatus.PUBLISHED:
        return
    missing = completeness.missing_fields(product, require_ready_media=True)
    if missing:
        raise WouldBreakPublished(missing)
    path: list[str] = []
    if product.category_id is not None:
        found = await CategoryRepo(session).get(product.category_id)
        path = [c.name for c in found.path] + [found.category.name]
    product.search_document = search_document.build(
        product, category_path=path, built_at=datetime.now(UTC)
    )
    events.updated(session, product, changed_fields)


class ProductRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, product_id: uuid.UUID, principal: Principal | None) -> Product:
        product = await load_detail(self.session, product_id)
        access.ensure_visible(principal, product)
        return product

    async def create(
        self, principal: Principal, fields: dict[str, Any], *, merchant_id: uuid.UUID | None
    ) -> Product:
        """``fields`` are the editable columns; ``merchant_id`` is honoured for admins only."""
        if access.is_admin(principal):
            if merchant_id is None:
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
            owner = merchant_id
        else:
            # A merchant's body merchant_id is ignored, not obeyed (I4).
            caller = access.caller_merchant_id(principal)
            assert caller is not None  # guaranteed by the route's role check
            owner = caller
        await self._ensure_category(fields.get("category_id"))

        product = Product(
            id=uuid.uuid4(),
            merchant_id=owner,
            status=ProductStatus.DRAFT,
            version=1,
            **fields,
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
        return await load_detail(self.session, product.id)

    async def update(
        self, principal: Principal, product_id: uuid.UUID, changes: dict[str, Any], version: int
    ) -> Product:
        """Applies ``changes`` (the fields the caller sent) if ``version`` is current."""
        product = await lock_for_change(self.session, principal, product_id, version)

        if changes.get("category_id") is not None:
            await self._ensure_category(changes["category_id"])
        diff = utils.diff({f: getattr(product, f) for f in changes}, changes)
        if diff:
            for field in diff:
                setattr(product, field, changes[field])
            bump(product)
            await self._flush()
            audit_repo.record(
                self.session,
                product_id=product.id,
                actor_id=principal.subject,
                action=ProductAuditAction.UPDATED,
                changes=diff,
            )
            await after_published_change(self.session, product.id, list(diff))
        await self.session.commit()
        return await load_detail(self.session, product.id)

    async def change_status(
        self, principal: Principal, product_id: uuid.UUID, status: str, version: int
    ) -> Product:
        """Every target but ``published``, which is ``PublishingRepo.accept``'s."""
        target = ProductStatus(status)
        product = await lock_product(self.session, product_id)
        access.ensure_can_manage(principal, product)
        access.ensure_not_publishing(product)
        access.ensure_version(product, version)
        await self._transition(principal, product, target)
        await self.session.commit()
        return await load_detail(self.session, product.id)

    async def archive(self, principal: Principal, product_id: uuid.UUID, version: int) -> Product:
        """Soft delete (S5): products are referenced across boundaries, never removed."""
        product = await lock_product(self.session, product_id)
        access.ensure_can_manage(principal, product)
        access.ensure_not_publishing(product)
        access.ensure_version(product, version)
        await self._transition(principal, product, ProductStatus.ARCHIVED)
        await self.session.commit()
        return product

    async def history(
        self, principal: Principal, product_id: uuid.UUID, page: PageParams
    ) -> Slice[ProductAuditLog]:
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
        return Slice(items=list(rows), total=total or 0, params=page)

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
        if previous == ProductStatus.PUBLISHED:
            # T7 unpublish / T11 archive: the product leaves the public catalogue.
            detail = await load_detail(self.session, product.id)
            events.unpublished(
                self.session,
                detail,
                new_status=target,
                reason=(
                    UnpublishReason.ARCHIVED
                    if target == ProductStatus.ARCHIVED
                    else UnpublishReason.MERCHANT_UNPUBLISHED
                ),
                variant_ids=[v.id for v in detail.variants],
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
