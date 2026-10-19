"""inventory's reservation API, called by the saga's activities with the worker's
service token (ORD-11). Business outcomes are non-retryable (INV-5)."""

import uuid

import httpx

from smartretail_common.service_token import ServiceTokenClient
from smartretail_common.temporal import raise_for_activity

BASE = "/internal/v1/inventory/reservations"


class InventoryClient:
    def __init__(self, base_url: str, tokens: ServiceTokenClient, http: httpx.AsyncClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.tokens = tokens
        self.http = http

    async def _call(self, method: str, path: str, **kwargs: object) -> httpx.Response:
        response = await self.http.request(
            method,
            f"{self.base_url}{path}",
            headers=await self.tokens.headers(),
            **kwargs,  # type: ignore[arg-type]
        )
        raise_for_activity(response, tokens=self.tokens)
        return response

    async def reserve(
        self,
        order_id: uuid.UUID,
        lines: list[tuple[uuid.UUID, int]],
        correlation_id: str | None,
    ) -> list[str]:
        """All-or-nothing; a repeat for the order replays the same reservations (B3)."""
        response = await self._call(
            "POST",
            BASE,
            json={
                "order_id": str(order_id),
                "lines": [{"variant_id": str(v), "quantity": q} for v, q in lines],
                "correlation_id": correlation_id,
            },
        )
        return [r["id"] for r in response.json()["reservations"]]

    async def commit(self, reservation_id: str) -> None:
        await self._call("POST", f"{BASE}/{reservation_id}/commit")

    async def release(self, reservation_id: str) -> None:
        await self._call("DELETE", f"{BASE}/{reservation_id}", params={"reason": "compensation"})

    async def reverse(self, reservation_id: str) -> None:
        await self._call("POST", f"{BASE}/{reservation_id}/reverse")
