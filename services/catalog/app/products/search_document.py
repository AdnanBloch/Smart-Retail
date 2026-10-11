"""The flattened projection ``build_search_document`` persists (RFC 0004 §3.1.2.3).

Descriptive fields only: live facts (stock, the displayed price) are hydrated
at read time. Pure — a function of the current rows, overwritten on every run.
"""

from datetime import datetime
from typing import Any

from pydantic_core import to_jsonable_python

from app.media.enums import MediaProcessingStatus
from app.products.models import Product


def build(product: Product, *, category_path: list[str], built_at: datetime) -> dict[str, Any]:
    """``product`` must have its category, variants and media loaded."""
    document = {
        "product_id": product.id,
        "merchant_id": product.merchant_id,
        "version": product.version,
        "name": product.name,
        "sku": product.sku,
        "description": product.description,
        "category": (
            {"id": product.category.id, "name": product.category.name, "path": category_path}
            if product.category
            else None
        ),
        "attributes": product.attributes,
        "variants": [
            {
                "variant_id": v.id,
                "sku": v.sku,
                "options": v.options,
                "price": v.price_override if v.price_override is not None else product.base_price,
                "is_active": v.is_active,
            }
            for v in product.variants
        ],
        "base_price": product.base_price,
        "currency": product.currency,
        "media": [
            {"media_id": m.id, "url": m.url, "alt_text": m.alt_text, "position": m.position}
            for m in product.media
            if m.processing_status == MediaProcessingStatus.READY
        ],
        "built_at": built_at,
    }
    jsonable: dict[str, Any] = to_jsonable_python(document)
    return jsonable
