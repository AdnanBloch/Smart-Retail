"""catalog and inventory HTTP clients over ``httpx.MockTransport``: what is an
answer (200 / not visible) and what is an outage (AST-D11, D9)."""

import uuid

import httpx
import pytest

from app.core.clients.catalog import CatalogClient
from app.core.clients.inventory import InventoryClient, InventoryUnavailable
from app.core.errors import CatalogUnavailable
from smartretail_common.errors import DependencyUnavailable


def http(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def raising(exc_type):
    def handler(request: httpx.Request) -> httpx.Response:
        raise exc_type("simulated", request=request)

    return handler


async def test_catalog_sends_the_token_only_when_given_and_strips_the_base_slash():
    seen: list[httpx.Request] = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"id": request.url.path.rsplit("/", 1)[-1]})

    client = CatalogClient("http://catalog/", http(handler), timeout=1.0)
    pid = uuid.uuid4()
    assert await client.get_product(pid) == {"id": str(pid)}
    assert await client.get_product(pid, token="tok") == {"id": str(pid)}
    assert [str(r.url) for r in seen] == [f"http://catalog/api/v1/products/{pid}"] * 2
    assert "authorization" not in seen[0].headers
    assert seen[1].headers["authorization"] == "Bearer tok"


@pytest.mark.parametrize("status", [403, 404])
async def test_catalog_not_visible_is_none(status):
    client = CatalogClient("http://catalog", http(lambda r: httpx.Response(status)), timeout=1.0)
    assert await client.get_product(uuid.uuid4()) is None


@pytest.mark.parametrize(
    "handler",
    [
        raising(httpx.ConnectTimeout),
        raising(httpx.ReadTimeout),
        raising(httpx.ConnectError),
        lambda r: httpx.Response(500),
        lambda r: httpx.Response(503),
        lambda r: httpx.Response(401),
    ],
    ids=["connect-timeout", "read-timeout", "connect-error", "500", "503", "401"],
)
async def test_catalog_anything_but_an_answer_is_unavailable(handler):
    client = CatalogClient("http://catalog", http(handler), timeout=1.0)
    with pytest.raises(CatalogUnavailable) as caught:
        await client.get_product(uuid.uuid4())
    assert isinstance(caught.value, DependencyUnavailable)


async def test_inventory_answers_and_missing_rows():
    variant = uuid.uuid4()

    def handler(request):
        if request.url.path == f"/api/v1/inventory/{variant}":
            return httpx.Response(200, json={"available": 3})
        return httpx.Response(404)

    client = InventoryClient("http://inventory/", http(handler), timeout=1.0)
    assert await client.get_availability(variant) == {"available": 3}
    assert await client.get_availability(str(variant)) == {"available": 3}
    assert await client.get_availability(uuid.uuid4()) is None


@pytest.mark.parametrize(
    "handler",
    [
        raising(httpx.ReadTimeout),
        raising(httpx.ConnectError),
        lambda r: httpx.Response(502),
        lambda r: httpx.Response(429),
    ],
    ids=["read-timeout", "connect-error", "502", "429"],
)
async def test_inventory_outage_is_inventory_unavailable(handler):
    client = InventoryClient("http://inventory", http(handler), timeout=1.0)
    with pytest.raises(InventoryUnavailable) as caught:
        await client.get_availability(uuid.uuid4())
    assert caught.value.status == 503
