import uuid
from decimal import Decimal

from fastapi import APIRouter, Depends, Query, Response

from app.api.handlers.utils import (
    listing_repo,
    optional_principal,
    product_repo,
    require_catalog_writer,
    require_merchant,
    variant_repo,
)
from app.models.enums import ProductStatus
from app.models.media import MediaCreate, MediaWritten
from app.models.product import (
    ArchivedOut,
    AuditEntry,
    ProductCreate,
    ProductDetail,
    ProductPatch,
    ProductSummary,
    SortKey,
    StatusChange,
)
from app.models.variant import VariantCreate, VariantWritten
from app.services.listing_repo import ListingRepo, ProductFilters
from app.services.product_repo import ProductRepo
from app.services.variant_repo import VariantRepo
from smartretail_common.auth import Principal
from smartretail_common.pagination import Page, PageParams, page_params

router = APIRouter()


def product_filters(
    category_id: uuid.UUID | None = None,
    merchant_id: uuid.UUID | None = None,
    q: str | None = Query(None, min_length=1, max_length=200),
    min_price: Decimal | None = Query(None, ge=0),
    max_price: Decimal | None = Query(None, ge=0),
    status: ProductStatus | None = Query(
        None,
        description="Honoured for admins, and for merchants filtering by their own merchant_id",
    ),
    sort: SortKey = "-created_at",
) -> ProductFilters:
    return ProductFilters(
        category_id=category_id,
        merchant_id=merchant_id,
        q=q,
        min_price=min_price,
        max_price=max_price,
        status=status,
        sort=sort,
    )


async def browse_products(
    filters: ProductFilters = Depends(product_filters),
    page: PageParams = Depends(page_params),
    principal: Principal | None = Depends(optional_principal),
    repo: ListingRepo = Depends(listing_repo),
) -> Page[ProductSummary]:
    return await repo.browse(principal, filters, page)


async def my_products(
    filters: ProductFilters = Depends(product_filters),
    page: PageParams = Depends(page_params),
    principal: Principal = Depends(require_merchant),
    repo: ListingRepo = Depends(listing_repo),
) -> Page[ProductSummary]:
    return await repo.mine(principal, filters, page)


async def create_product(
    body: ProductCreate,
    response: Response,
    principal: Principal = Depends(require_catalog_writer),
    repo: ProductRepo = Depends(product_repo),
) -> ProductDetail:
    product = await repo.create(principal, body)
    response.headers["Location"] = f"/api/v1/products/{product.id}"
    return product


async def get_product(
    product_id: uuid.UUID,
    principal: Principal | None = Depends(optional_principal),
    repo: ProductRepo = Depends(product_repo),
) -> ProductDetail:
    return await repo.get(product_id, principal)


async def update_product(
    product_id: uuid.UUID,
    body: ProductPatch,
    principal: Principal = Depends(require_catalog_writer),
    repo: ProductRepo = Depends(product_repo),
) -> ProductDetail:
    return await repo.update(principal, product_id, body)


async def archive_product(
    product_id: uuid.UUID,
    version: int = Query(..., ge=1),
    principal: Principal = Depends(require_catalog_writer),
    repo: ProductRepo = Depends(product_repo),
) -> ArchivedOut:
    return await repo.archive(principal, product_id, version)


async def change_status(
    product_id: uuid.UUID,
    body: StatusChange,
    principal: Principal = Depends(require_catalog_writer),
    repo: ProductRepo = Depends(product_repo),
) -> ProductDetail:
    return await repo.change_status(principal, product_id, body)


async def product_history(
    product_id: uuid.UUID,
    page: PageParams = Depends(page_params),
    principal: Principal = Depends(require_catalog_writer),
    repo: ProductRepo = Depends(product_repo),
) -> Page[AuditEntry]:
    return await repo.history(principal, product_id, page)


async def add_variant(
    product_id: uuid.UUID,
    body: VariantCreate,
    response: Response,
    principal: Principal = Depends(require_catalog_writer),
    repo: VariantRepo = Depends(variant_repo),
) -> VariantWritten:
    variant = await repo.add(principal, product_id, body)
    response.headers["Location"] = f"/api/v1/variants/{variant.id}"
    return variant


async def add_media(
    product_id: uuid.UUID,
    body: MediaCreate,
    response: Response,
    principal: Principal = Depends(require_catalog_writer),
    repo: VariantRepo = Depends(variant_repo),
) -> MediaWritten:
    media = await repo.add_media(principal, product_id, body)
    response.headers["Location"] = f"/api/v1/media/{media.id}"
    return media
