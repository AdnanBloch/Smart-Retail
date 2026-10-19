"""``GET catalog /api/v1/products/{id}``: live price for hydration, ownership for generation.

Anonymous reads see published products only (a 404 otherwise), so a 404 on
hydration means the index is stale and the result must not be shown (fail
closed, AST-D11). A merchant-token read returns the merchant's own products,
drafts included (generation ownership, §3.1.1.2). Anything that is not an
answer — timeout, connection error, 5xx — is :class:`CatalogUnavailable`.
"""

import uuid
from typing import Any

import httpx

from app.core.errors import CatalogUnavailable


class CatalogClient:
    def __init__(self, base_url: str, http: httpx.AsyncClient, *, timeout: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = http
        self.timeout = timeout

    async def get_product(
        self, product_id: uuid.UUID, *, token: str | None = None
    ) -> dict[str, Any] | None:
        """The product as the caller may see it, or ``None`` if it is not visible."""
        url = f"{self.base_url}/api/v1/products/{product_id}"
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        try:
            response = await self.http.get(url, headers=headers, timeout=self.timeout)
        except httpx.HTTPError as exc:
            raise CatalogUnavailable(f"catalog unreachable: {type(exc).__name__}") from exc
        if response.status_code == 200:
            body: dict[str, Any] = response.json()
            return body
        if response.status_code in (403, 404):
            return None
        raise CatalogUnavailable(f"catalog answered {response.status_code}")
