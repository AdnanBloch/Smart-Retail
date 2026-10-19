"""``GET inventory /api/v1/inventory/{variant_id}``: live availability (D9, R5).

Public and cached for 10 s by inventory. A 404 means the variant has no stock
row (not purchasable). Anything else that is not an answer raises
:class:`InventoryUnavailable`, which hydration turns into ``in_stock: null``.
"""

import uuid
from typing import Any

import httpx

from smartretail_common.errors import DependencyUnavailable


class InventoryUnavailable(DependencyUnavailable):
    type_slug = "inventory-unavailable"
    title = "Inventory is unavailable"


class InventoryClient:
    def __init__(self, base_url: str, http: httpx.AsyncClient, *, timeout: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = http
        self.timeout = timeout

    async def get_availability(self, variant_id: uuid.UUID | str) -> dict[str, Any] | None:
        url = f"{self.base_url}/api/v1/inventory/{variant_id}"
        try:
            response = await self.http.get(url, timeout=self.timeout)
        except httpx.HTTPError as exc:
            raise InventoryUnavailable(f"inventory unreachable: {type(exc).__name__}") from exc
        if response.status_code == 200:
            body: dict[str, Any] = response.json()
            return body
        if response.status_code == 404:
            return None
        raise InventoryUnavailable(f"inventory answered {response.status_code}")
