import uuid

import httpx
import pytest

from app.clients.catalog import CatalogClient, CatalogUnavailable
from app.clients.inventory import InventoryClient
from app.core.errors import problem_body
from app.orders.snapshot import LineRequest, check_shape
from smartretail_common.errors import Unprocessable
from smartretail_common.service_token import ServiceCredentials, ServiceTokenClient


def test_shape_rules() -> None:
    v = uuid.uuid4()
    lines = [LineRequest(uuid.uuid4(), v, 1), LineRequest(uuid.uuid4(), v, 5)]
    with pytest.raises(Unprocessable) as err:
        check_shape(lines, max_lines=1, max_quantity=3)
    codes = [x["code"] for x in err.value.extras["violations"]]
    assert codes == ["too_many", "duplicate", "too_large"]
    body = problem_body(err.value, "/api/v1/orders")
    assert body["status"] == 422 and body["instance"] == "/api/v1/orders"
    assert "request_id" not in body


async def test_catalog_client_maps_answers() -> None:
    answers = iter(
        [httpx.Response(200, json={"id": "x"}), httpx.Response(403), httpx.Response(500)]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return next(answers)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = CatalogClient("http://catalog/", http, timeout=1)
        assert await client.get_product(uuid.uuid4(), "t") == {"id": "x"}
        assert await client.get_product(uuid.uuid4(), "t") is None
        with pytest.raises(CatalogUnavailable):
            await client.get_product(uuid.uuid4(), "t")

    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(timeout)) as http:
        with pytest.raises(CatalogUnavailable):
            await CatalogClient("http://catalog", http, timeout=1).get_product(uuid.uuid4(), "t")


async def test_inventory_client_calls_the_internal_routes() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        return httpx.Response(201, json={"reservations": [{"id": "r1"}]})

    tokens = ServiceTokenClient(ServiceCredentials("u", "c", "s"))
    tokens._token, tokens._refresh_after = "tok", float("inf")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = InventoryClient("http://inventory", tokens, http)
        assert await client.reserve(uuid.uuid4(), [(uuid.uuid4(), 2)], "corr") == ["r1"]
        await client.commit("r1")
        await client.release("r1")
        await client.reverse("r1")
    base = "/internal/v1/inventory/reservations"
    assert seen == [
        ("POST", base),
        ("POST", f"{base}/r1/commit"),
        ("DELETE", f"{base}/r1"),
        ("POST", f"{base}/r1/reverse"),
    ]
