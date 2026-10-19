"""``catalog.product.published`` → stock rows for the product's variants (RFC 0006 §3.1.2.4).

The publishing workflow's ``sync_downstream`` normally created these rows
already, over HTTP, before the product went live; this handler makes the event
sufficient on its own (a replay, a product published while inventory was
being rebuilt). Idempotent by state as well as by ``processed_events``: the
topic is compacted, so a replay can reach events older than the dedupe
retention, and existing rows are never touched (INV-4).
"""

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.v1.stock.repo import StockRepo, VariantRef
from smartretail_common.events.consumer import Handler
from smartretail_common.events.envelope import Envelope

ACTOR = "consumer:catalog.product.published"


def handler(settings: Settings) -> Handler:
    async def handle(event: Envelope, session: AsyncSession) -> None:
        payload = event.payload
        variants = [
            VariantRef(uuid.UUID(v["variant_id"]), v["sku"])
            for v in payload["variants"]
            if v["is_active"]
        ]
        await StockRepo(session, settings).insert_initial(
            uuid.UUID(payload["product_id"]),
            uuid.UUID(payload["merchant_id"]),
            variants,
            actor=ACTOR,
        )

    return handle
