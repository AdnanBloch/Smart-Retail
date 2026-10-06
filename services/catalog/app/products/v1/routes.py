"""``/api/v1/products``: browse, lifecycle, history, and the product's variants and
media collections (RFC 0004 §3.1.2)."""

import uuid
from decimal import Decimal

from fastapi import APIRouter, Depends, Query, Response, status

from app.core.dependencies import optional_principal, require_catalog_writer, require_merchant
from app.media.dependencies import media_repo
from app.media.repo import MediaRepo
from app.media.v1.schemas import MediaCreate, MediaWritten
from app.products.dependencies import listing_repo, product_repo
from app.products.enums import ProductStatus
from app.products.listing_repo import ListingRepo, ProductFilters, SortKey
from app.products.repo import ProductRepo
from app.products.v1.schemas import (
    ArchivedOut,
    AuditEntry,
    ProductCreate,
    ProductDetail,
    ProductPatch,
    ProductSummary,
    StatusChange,
)
from app.variants.dependencies import variant_repo
from app.variants.repo import VariantRepo
from app.variants.v1.schemas import VariantCreate, VariantWritten
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


@router.get("", response_model=Page[ProductSummary])
async def browse_products(
    filters: ProductFilters = Depends(product_filters),
    page: PageParams = Depends(page_params),
    principal: Principal | None = Depends(optional_principal),
    repo: ListingRepo = Depends(listing_repo),
) -> Page[ProductSummary]:
    listings = await repo.browse(principal, filters, page)
    return listings.to_page(ProductSummary.from_listing)


# Registered before /{product_id} so "me" is not parsed as an id.
@router.get("/me", response_model=Page[ProductSummary])
async def my_products(
    filters: ProductFilters = Depends(product_filters),
    page: PageParams = Depends(page_params),
    principal: Principal = Depends(require_merchant),
    repo: ListingRepo = Depends(listing_repo),
) -> Page[ProductSummary]:
    listings = await repo.mine(principal, filters, page)
    return listings.to_page(ProductSummary.from_listing)


@router.post("", status_code=status.HTTP_201_CREATED, response_model=ProductDetail)
async def create_product(
    body: ProductCreate,
    response: Response,
    principal: Principal = Depends(require_catalog_writer),
    repo: ProductRepo = Depends(product_repo),
) -> ProductDetail:
    product = await repo.create(
        principal, body.model_dump(exclude={"merchant_id"}), merchant_id=body.merchant_id
    )
    response.headers["Location"] = f"/api/v1/products/{product.id}"
    return ProductDetail.from_product(product)


@router.get("/{product_id}", response_model=ProductDetail)
async def get_product(
    product_id: uuid.UUID,
    principal: Principal | None = Depends(optional_principal),
    repo: ProductRepo = Depends(product_repo),
) -> ProductDetail:
    return ProductDetail.from_product(await repo.get(product_id, principal))


@router.patch("/{product_id}", response_model=ProductDetail)
async def update_product(
    product_id: uuid.UUID,
    body: ProductPatch,
    principal: Principal = Depends(require_catalog_writer),
    repo: ProductRepo = Depends(product_repo),
) -> ProductDetail:
    changes = body.model_dump(exclude_unset=True, exclude={"version"})
    product = await repo.update(principal, product_id, changes, body.version)
    return ProductDetail.from_product(product)


@router.delete("/{product_id}", response_model=ArchivedOut)
async def archive_product(
    product_id: uuid.UUID,
    version: int = Query(..., ge=1),
    principal: Principal = Depends(require_catalog_writer),
    repo: ProductRepo = Depends(product_repo),
) -> ArchivedOut:
    return ArchivedOut.model_validate(await repo.archive(principal, product_id, version))


@router.post("/{product_id}/status", response_model=ProductDetail)
async def change_status(
    product_id: uuid.UUID,
    body: StatusChange,
    principal: Principal = Depends(require_catalog_writer),
    repo: ProductRepo = Depends(product_repo),
) -> ProductDetail:
    product = await repo.change_status(principal, product_id, body.status, body.version)
    return ProductDetail.from_product(product)


@router.get("/{product_id}/history", response_model=Page[AuditEntry])
async def product_history(
    product_id: uuid.UUID,
    page: PageParams = Depends(page_params),
    principal: Principal = Depends(require_catalog_writer),
    repo: ProductRepo = Depends(product_repo),
) -> Page[AuditEntry]:
    entries = await repo.history(principal, product_id, page)
    return entries.to_page(AuditEntry.model_validate)


@router.post(
    "/{product_id}/variants", status_code=status.HTTP_201_CREATED, response_model=VariantWritten
)
async def add_variant(
    product_id: uuid.UUID,
    body: VariantCreate,
    response: Response,
    principal: Principal = Depends(require_catalog_writer),
    repo: VariantRepo = Depends(variant_repo),
) -> VariantWritten:
    write = await repo.add(
        principal,
        product_id,
        sku=body.sku,
        options=body.options,
        price_override=body.price_override,
        version=body.version,
    )
    response.headers["Location"] = f"/api/v1/variants/{write.variant.id}"
    return VariantWritten.from_write(write)


@router.post(
    "/{product_id}/media", status_code=status.HTTP_201_CREATED, response_model=MediaWritten
)
async def add_media(
    product_id: uuid.UUID,
    body: MediaCreate,
    response: Response,
    principal: Principal = Depends(require_catalog_writer),
    repo: MediaRepo = Depends(media_repo),
) -> MediaWritten:
    write = await repo.add(
        principal,
        product_id,
        url=str(body.url),
        alt_text=body.alt_text,
        position=body.position,
        version=body.version,
    )
    response.headers["Location"] = f"/api/v1/media/{write.media.id}"
    return MediaWritten.from_write(write)
