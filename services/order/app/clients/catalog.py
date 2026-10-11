"""The checkout snapshot call: ``GET catalog /api/v1/products/{id}`` (A13, R5).

The platform's one synchronous cross-service call. It forwards **the end
user's token**, so catalog applies exactly the visibility rule it applies at
the edge: an unpublished product is a 404 to a customer. One retry on a
connect error; anything else that is not an answer is ``503``.
"""

import uuid
from typing import Any

import httpx

from smartretail_common.errors import DependencyUnavailable


class CatalogUnavailable(DependencyUnavailable):
    type_slug = "catalog-unavailable"
    title = "The catalog is unavailable"


class CatalogClient:
    def __init__(self, base_url: str, http: httpx.AsyncClient, *, timeout: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = http
        self.timeout = timeout

    async def get_product(self, product_id: uuid.UUID, token: str) -> dict[str, Any] | None:
        """The product as the caller may see it, or ``None`` if it is not visible to them."""
        url = f"{self.base_url}/api/v1/products/{product_id}"
        headers = {"Authorization": f"Bearer {token}"}
        for attempt in (1, 2):
            try:
                response = await self.http.get(url, headers=headers, timeout=self.timeout)
            except httpx.ConnectError as exc:
                if attempt == 2:
                    raise CatalogUnavailable("Retry the order shortly") from exc
                continue
            except httpx.HTTPError as exc:
                raise CatalogUnavailable("Retry the order shortly") from exc
            if response.status_code == 200:
                body: dict[str, Any] = response.json()
                return body
            if response.status_code in (403, 404):
                return None
            raise CatalogUnavailable(f"catalog answered {response.status_code}")
        raise AssertionError("unreachable")  # pragma: no cover
