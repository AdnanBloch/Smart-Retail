"""``/api/v1/products``: browse, lifecycle, history, and the product's variants and
media collections (RFC 0004 §3.1.2)."""

import uuid
from decimal import Decimal

from fastapi import APIRouter, Depends, File, Form, Query, Response, UploadFile, status
from fastapi.responses import JSONResponse

from app.core.config import Settings
from app.core.dependencies import (
    get_settings,
    optional_principal,
    require_catalog_writer,
    require_merchant,
)
from app.media.dependencies import media_repo, media_storage
from app.media.repo import MediaRepo, Upload
from app.media.storage import MediaStorage
from app.media.v1.schemas import MediaCreate, MediaWritten
from app.products import access
from app.products.dependencies import listing_repo, product_repo, publishing_repo
from app.products.enums import ProductStatus
from app.products.listing_repo import ListingRepo, ProductFilters, SortKey
from app.products.publishing_repo import PublishingRepo
from app.products.repo import ProductRepo
from app.products.v1.schemas import (
    ArchivedOut,
    AuditEntry,
    ProductCreate,
    ProductDetail,
    ProductPatch,
    ProductSummary,
    PublishAcceptedOut,
    PublishStatusOut,
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
    """Also the order validate/snapshot call (A13): served from the primary, uncached."""
    product = await repo.get(product_id, principal)
    manages = access.is_admin(principal) or access.is_owner(principal, product)
    return ProductDetail.from_product(product, all_media=manages)


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


@router.post(
    "/{product_id}/status",
    response_model=ProductDetail,
    responses={
        202: {"model": PublishAcceptedOut, "description": "Publish accepted (workflow started)"},
        503: {"description": "The publishing workflow could not be started (CAT-9)"},
    },
)
async def change_status(
    product_id: uuid.UUID,
    body: StatusChange,
    principal: Principal = Depends(require_catalog_writer),
    repo: ProductRepo = Depends(product_repo),
    publishing: PublishingRepo = Depends(publishing_repo),
) -> ProductDetail | JSONResponse:
    """``published`` starts ``ProductPublishingWorkflow`` and returns ``202`` (B9)."""
    if body.status == ProductStatus.PUBLISHED:
        accepted = await publishing.accept(principal, product_id, body.version)
        out = PublishAcceptedOut.from_accepted(accepted)
        return JSONResponse(
            out.model_dump(mode="json", by_alias=True),
            status_code=status.HTTP_202_ACCEPTED,
            headers={"Location": out.links.publish_status},
        )
    product = await repo.change_status(principal, product_id, body.status, body.version)
    return ProductDetail.from_product(product)


@router.get("/{product_id}/publish-status", response_model=PublishStatusOut)
async def publish_status(
    product_id: uuid.UUID,
    principal: Principal = Depends(require_catalog_writer),
    publishing: PublishingRepo = Depends(publishing_repo),
) -> PublishStatusOut:
    """Why a publish failed, without a Temporal login (Q8 (0002))."""
    return PublishStatusOut.from_status(await publishing.status(principal, product_id))


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


@router.post(
    "/{product_id}/media/upload",
    status_code=status.HTTP_201_CREATED,
    response_model=MediaWritten,
    responses={413: {"description": "Too large"}, 415: {"description": "Type not allowed"}},
)
async def upload_media(
    product_id: uuid.UUID,
    response: Response,
    file: UploadFile = File(...),
    version: int = Form(..., ge=1),
    alt_text: str | None = Form(None, max_length=500),
    position: int = Form(0, ge=0, le=10_000),
    principal: Principal = Depends(require_catalog_writer),
    repo: MediaRepo = Depends(media_repo),
    storage: MediaStorage = Depends(media_storage),
    settings: Settings = Depends(get_settings),
) -> MediaWritten:
    """Binary upload to object storage (B12); processed in the next publish run."""
    # Read one byte past the ceiling so an oversize upload is refused, not truncated.
    data = await file.read(settings.media_max_bytes + 1)
    write = await repo.upload(
        principal,
        product_id,
        Upload(data, file.filename, file.content_type),
        alt_text=alt_text,
        position=position,
        version=version,
        storage=storage,
        settings=settings,
    )
    response.headers["Location"] = f"/api/v1/media/{write.media.id}"
    return MediaWritten.from_write(write)
