"""Outbox payloads for ``catalog.product.*`` (RFC 0004 §3.1.2.3, CAT-14).

State-carrying: each payload holds the product's full public state, so every
consumer acts without calling back into catalog. Aggregate and Kafka key are
the product id (C3). Changes to a product that is not published emit nothing.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.products.enums import CatalogEvent, ProductStatus, UnpublishReason
from app.products.models import Product
from smartretail_common.outbox import write_outbox

AGGREGATE = "product"


def state(product: Product) -> dict[str, Any]:
    """``product`` must have its variants loaded."""
    return {
        "product_id": product.id,
        "merchant_id": product.merchant_id,
        "version": product.version,
        "sku": product.sku,
        "name": product.name,
        "status": str(product.status),
        "category_id": product.category_id,
        "base_price": product.base_price,
        "currency": product.currency,
        "published_at": product.published_at,
        "variants": [
            {
                "variant_id": v.id,
                "sku": v.sku,
                "options": v.options,
                "price_override": v.price_override,
                "is_active": v.is_active,
            }
            for v in product.variants
        ],
        "search_document": product.search_document,
    }


def published(
    session: AsyncSession, product: Product, *, correlation_id: str | None = None
) -> uuid.UUID:
    return write_outbox(
        session,
        event_type=CatalogEvent.PUBLISHED.value,
        aggregate_type=AGGREGATE,
        aggregate_id=product.id,
        payload={**state(product), "changed_fields": []},
        correlation_id=correlation_id,
    )


def updated(session: AsyncSession, product: Product, changed_fields: list[str]) -> uuid.UUID:
    return write_outbox(
        session,
        event_type=CatalogEvent.UPDATED.value,
        aggregate_type=AGGREGATE,
        aggregate_id=product.id,
        payload={**state(product), "changed_fields": sorted(changed_fields)},
    )


def unpublished(
    session: AsyncSession,
    product: Product,
    *,
    new_status: ProductStatus,
    reason: UnpublishReason,
    variant_ids: list[uuid.UUID],
) -> uuid.UUID:
    return write_outbox(
        session,
        event_type=CatalogEvent.UNPUBLISHED.value,
        aggregate_type=AGGREGATE,
        aggregate_id=product.id,
        payload={
            "product_id": product.id,
            "merchant_id": product.merchant_id,
            "version": product.version,
            "previous_status": ProductStatus.PUBLISHED.value,
            "new_status": new_status.value,
            "reason": reason.value,
            "variant_ids": variant_ids,
            "unpublished_at": datetime.now(UTC),
        },
    )
