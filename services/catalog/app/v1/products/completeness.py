"""What a product needs before it may be public (RFC 0004 ``validate_product``).

Pure: takes a product with its category, variants and media loaded. The same
rule guards publishing (steps 1 and 5) and every later change to a published
product, so "published" always means "complete".
"""

from app.v1.media.enums import MediaProcessingStatus
from app.v1.products.models import Product


def missing_fields(product: Product, *, require_ready_media: bool = False) -> list[str]:
    missing: list[str] = []
    if not product.name.strip():
        missing.append("name")
    if not (product.description or "").strip():
        missing.append("description")
    if product.category_id is None:
        missing.append("category_id")
    if not any(v.is_active for v in product.variants):
        missing.append("variants")
    media = product.media
    if require_ready_media:
        media = [m for m in media if m.processing_status == MediaProcessingStatus.READY]
    if not media:
        missing.append("media")
    return missing
