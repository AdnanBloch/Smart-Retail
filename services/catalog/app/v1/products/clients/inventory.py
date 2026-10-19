"""inventory's internal stock-initialisation route, called by ``sync_downstream``
with the worker's service token (RFC 0006 INV-4). Off every request path."""

import uuid

import httpx

from smartretail_common.service_token import ServiceTokenClient
from smartretail_common.temporal import raise_for_activity


class InventoryClient:
    def __init__(self, base_url: str, tokens: ServiceTokenClient, http: httpx.AsyncClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.tokens = tokens
        self.http = http

    async def initialise_stock(
        self, product_id: uuid.UUID, merchant_id: uuid.UUID, variants: list[tuple[uuid.UUID, str]]
    ) -> None:
        response = await self.http.post(
            f"{self.base_url}/internal/v1/inventory/stock-items",
            json={
                "product_id": str(product_id),
                "merchant_id": str(merchant_id),
                "variants": [{"variant_id": str(v), "sku": sku} for v, sku in variants],
            },
            headers=await self.tokens.headers(),
        )
        raise_for_activity(response, tokens=self.tokens)
