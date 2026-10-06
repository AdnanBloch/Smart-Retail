"""Browse and merchant listings (RFC 0004 §3.1.2, visibility per §3.1.2.2)."""

import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from sqlalchemy import ColumnElement, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.media.models import ProductMedia
from app.products import access
from app.products.enums import ProductStatus
from app.products.models import Product
from smartretail_common.auth import Principal
from smartretail_common.errors import Unprocessable
from smartretail_common.pagination import PageParams, Slice

SortKey = Literal["created_at", "-created_at", "price", "-price", "name", "-name"]
SORT_COLUMNS = {"created_at": Product.created_at, "price": Product.base_price, "name": Product.name}


@dataclass(frozen=True)
class ProductFilters:
    category_id: uuid.UUID | None = None
    merchant_id: uuid.UUID | None = None
    q: str | None = None
    min_price: Decimal | None = None
    max_price: Decimal | None = None
    status: ProductStatus | None = None
    sort: SortKey = "-created_at"

    def __post_init__(self) -> None:
        if (
            self.min_price is not None
            and self.max_price is not None
            and self.min_price > self.max_price
        ):
            raise Unprocessable(
                "min_price must not exceed max_price",
                violations=[
                    {"field": "min_price", "message": "greater than max_price", "code": "range"}
                ],
            )


@dataclass(frozen=True)
class Listing:
    """A product as it appears in a listing, with its first media URL."""

    product: Product
    primary_media_url: str | None


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _common_conditions(f: ProductFilters) -> list[ColumnElement[bool]]:
    conditions: list[ColumnElement[bool]] = []
    if f.category_id is not None:
        conditions.append(Product.category_id == f.category_id)
    if f.merchant_id is not None:
        conditions.append(Product.merchant_id == f.merchant_id)
    if f.min_price is not None:
        conditions.append(Product.base_price >= f.min_price)
    if f.max_price is not None:
        conditions.append(Product.base_price <= f.max_price)
    if f.q:
        pattern = f"%{_escape_like(f.q)}%"
        conditions.append(
            or_(
                Product.name.ilike(pattern, escape="\\"),
                Product.sku.ilike(pattern, escape="\\"),
                Product.description.ilike(pattern, escape="\\"),
            )
        )
    return conditions


def _status_condition(principal: Principal | None, f: ProductFilters) -> ColumnElement[bool] | None:
    if access.is_admin(principal):
        return Product.status == f.status if f.status else None
    own = access.caller_merchant_id(principal)
    if own is not None and f.merchant_id == own and f.status is not None:
        return Product.status == f.status
    # Everyone else, and merchants browsing without that combination, see
    # published products only; a status filter is ignored, not rejected.
    return Product.status == ProductStatus.PUBLISHED


class ListingRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def browse(
        self, principal: Principal | None, f: ProductFilters, page: PageParams
    ) -> Slice[Listing]:
        conditions = _common_conditions(f)
        status = _status_condition(principal, f)
        if status is not None:
            conditions.append(status)
        return await self._page(conditions, f.sort, page)

    async def mine(
        self, principal: Principal, f: ProductFilters, page: PageParams
    ) -> Slice[Listing]:
        own = access.caller_merchant_id(principal)
        assert own is not None  # route requires the merchant role
        conditions = [*_common_conditions(f), Product.merchant_id == own]
        if f.status is not None:
            conditions.append(Product.status == f.status)
        return await self._page(conditions, f.sort, page)

    async def _page(
        self, conditions: list[ColumnElement[bool]], sort: SortKey, page: PageParams
    ) -> Slice[Listing]:
        primary_media = (
            select(ProductMedia.url)
            .where(ProductMedia.product_id == Product.id)
            .order_by(ProductMedia.position, ProductMedia.created_at)
            .limit(1)
            .scalar_subquery()
        )
        column = SORT_COLUMNS[sort.lstrip("-")]
        order = column.desc() if sort.startswith("-") else column.asc()
        total = await self.session.scalar(
            select(func.count()).select_from(Product).where(*conditions)
        )
        rows = await self.session.execute(
            select(Product, primary_media.label("primary_media_url"))
            .where(*conditions)
            .order_by(order, Product.id)
            .limit(page.limit)
            .offset(page.offset)
        )
        items = [Listing(product, media_url) for product, media_url in rows]
        return Slice(items=items, total=total or 0, params=page)
