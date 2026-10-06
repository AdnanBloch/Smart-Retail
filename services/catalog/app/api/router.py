from fastapi import APIRouter, Depends, status

from app.api.handlers import categories, media, products, variants
from app.api.handlers.utils import require_admin
from app.models.category import CategoryDetail, CategoryNode, CategoryOut
from app.models.media import MediaWritten
from app.models.product import ArchivedOut, AuditEntry, ProductDetail, ProductSummary
from app.models.variant import VariantDeactivated, VariantWritten
from smartretail_common.pagination import Page

# Route: /api/v1/
router = APIRouter(prefix="/api/v1")

# Route: /categories/
categories.router.get("", response_model=list[CategoryNode] | Page[CategoryOut])(
    categories.list_categories
)
categories.router.get("/{category_id}", response_model=CategoryDetail)(categories.get_category)
categories.router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=CategoryOut,
    dependencies=[Depends(require_admin)],
)(categories.create_category)
categories.router.patch(
    "/{category_id}", response_model=CategoryOut, dependencies=[Depends(require_admin)]
)(categories.update_category)
categories.router.delete(
    "/{category_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_admin)]
)(categories.delete_category)

router.include_router(categories.router, prefix="/categories", tags=["categories"])

# Route: /products/
products.router.get("", response_model=Page[ProductSummary])(products.browse_products)
# Registered before /{product_id} so "me" is not parsed as an id.
products.router.get("/me", response_model=Page[ProductSummary])(products.my_products)
products.router.post("", status_code=status.HTTP_201_CREATED, response_model=ProductDetail)(
    products.create_product
)
products.router.get("/{product_id}", response_model=ProductDetail)(products.get_product)
products.router.patch("/{product_id}", response_model=ProductDetail)(products.update_product)
products.router.delete("/{product_id}", response_model=ArchivedOut)(products.archive_product)
products.router.post("/{product_id}/status", response_model=ProductDetail)(products.change_status)
products.router.get("/{product_id}/history", response_model=Page[AuditEntry])(
    products.product_history
)
products.router.post(
    "/{product_id}/variants", status_code=status.HTTP_201_CREATED, response_model=VariantWritten
)(products.add_variant)
products.router.post(
    "/{product_id}/media", status_code=status.HTTP_201_CREATED, response_model=MediaWritten
)(products.add_media)

router.include_router(products.router, prefix="/products", tags=["products"])

# Route: /variants/
variants.router.patch("/{variant_id}", response_model=VariantWritten)(variants.update_variant)
variants.router.delete("/{variant_id}", response_model=VariantDeactivated)(
    variants.deactivate_variant
)

router.include_router(variants.router, prefix="/variants", tags=["variants"])

# Route: /media/
media.router.delete("/{media_id}", status_code=status.HTTP_204_NO_CONTENT)(media.remove_media)

router.include_router(media.router, prefix="/media", tags=["media"])
