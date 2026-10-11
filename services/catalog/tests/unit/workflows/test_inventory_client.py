import json
import uuid

import httpx
import pytest
from temporalio.exceptions import ApplicationError

from app.clients.inventory import InventoryClient
from smartretail_common.service_token import ServiceCredentials, ServiceTokenClient


class StaticTokens(ServiceTokenClient):
    def __init__(self) -> None:
        super().__init__(ServiceCredentials("u", "c", "s"))
        self._token = "tok"
        self._refresh_after = float("inf")


async def test_initialise_stock_sends_the_variants_with_a_service_token() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        status = 200 if len(seen) == 1 else 422
        return httpx.Response(status, json={"type": "https://x/errors/validation"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = InventoryClient("http://inventory:8004/", StaticTokens(), http)
        product, merchant, variant = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        await client.initialise_stock(product, merchant, [(variant, "SKU")])
        with pytest.raises(ApplicationError) as err:
            await client.initialise_stock(product, merchant, [])
    assert err.value.non_retryable
    request = seen[0]
    assert str(request.url) == "http://inventory:8004/internal/v1/inventory/stock-items"
    assert request.headers["authorization"] == "Bearer tok"
    assert json.loads(request.content)["variants"] == [{"variant_id": str(variant), "sku": "SKU"}]
