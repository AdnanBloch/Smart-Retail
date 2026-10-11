import asyncio
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from app.core.config import Settings
from main import create_application
from smartretail_common.auth import JwtVerifier
from smartretail_common.testing import provision_test_database, truncate_all

SERVICE_DIR = str(Path(__file__).resolve().parents[2])
TABLES = ["order_items", "order_status_history", "idempotency_keys", "outbox", "orders"]


class FakeCatalog:
    """catalog's ``GET /api/v1/products/{id}`` as a customer sees it."""

    def __init__(self) -> None:
        self.products: dict[str, dict[str, Any]] = {}
        self.down = False
        self.calls = 0
        self.tokens: list[str] = []

    def add(
        self,
        merchant_id: str,
        *,
        price: str = "10.00",
        currency: str = "USD",
        status: str = "published",
        variants: int = 1,
        override: str | None = None,
        active: bool = True,
    ) -> dict[str, Any]:
        product_id = str(uuid.uuid4())
        product = {
            "id": product_id,
            "merchant_id": merchant_id,
            "name": f"Product {product_id[:4]}",
            "sku": f"P-{product_id[:4]}",
            "status": status,
            "base_price": price,
            "currency": currency,
            "version": 3,
            "variants": [
                {
                    "id": str(uuid.uuid4()),
                    "sku": f"P-{product_id[:4]}-{i}",
                    "options": {"size": str(i)},
                    "price_override": override,
                    "is_active": active,
                }
                for i in range(variants)
            ],
        }
        self.products[product_id] = product
        return product

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        self.tokens.append(request.headers.get("authorization", ""))
        if self.down:
            raise httpx.ConnectError("catalog down", request=request)
        product = self.products.get(request.url.path.rsplit("/", 1)[-1])
        if product is None or product["status"] != "published":
            return httpx.Response(404, json={"type": "x/product-not-found"})
        return httpx.Response(200, json=product)


class FakeWorkflows:
    """Temporal at the API edge; the workflow itself is tested on a test server."""

    def __init__(self) -> None:
        self.started: list[dict[str, Any]] = []
        self.fail_start = False
        self.stage: str | None = "charging"
        self.cancellations: list[dict[str, Any]] = []

    async def start(self, *, order_id, workflow_id, correlation_id, payment_method) -> str:
        if self.fail_start:
            raise ConnectionError("temporal unreachable")
        self.started.append(
            {"order_id": str(order_id), "workflow_id": workflow_id, "method": payment_method}
        )
        return f"run-{len(self.started)}"

    async def state(self, workflow_id: str) -> dict[str, Any]:
        if self.stage is None:
            raise ConnectionError("temporal unreachable")
        return {"stage": self.stage, "failure_reason": None, "cancel_requested": False}

    async def request_cancellation(self, workflow_id, *, actor, reason) -> None:
        self.cancellations.append({"workflow_id": workflow_id, "actor": actor, "reason": reason})

    async def describe(self, workflow_id: str) -> dict[str, Any]:
        if self.stage is None:
            raise ConnectionError("temporal unreachable")
        return {
            "run_id": "run-1",
            "execution_status": "RUNNING",
            "started_at": None,
            "closed_at": None,
            "stage": self.stage,
            "failure_reason": None,
            "cancel_requested": False,
        }


@pytest.fixture(scope="session")
def database_url() -> str:
    url = provision_test_database("order_test", SERVICE_DIR)
    if url is None:
        pytest.skip("test Postgres not reachable; run `make test-db`")
    return url


@pytest.fixture
def settings(database_url) -> Settings:
    return Settings(
        service_name="order",
        database_url=database_url,
        jwt_public_key_path=None,
        db_pool_size=20,
        db_max_overflow=20,
    )


@pytest.fixture
def catalog() -> FakeCatalog:
    return FakeCatalog()


@pytest.fixture
def workflows() -> FakeWorkflows:
    return FakeWorkflows()


@pytest.fixture
async def app(settings, keypair, catalog, workflows):
    verifier = JwtVerifier(
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        static_keys={None: keypair.public_key},
    )
    application = create_application(
        settings,
        verifier=verifier,
        workflows=workflows,
        catalog_http=httpx.AsyncClient(transport=httpx.MockTransport(catalog.handler)),
    )
    async with application.router.lifespan_context(application):
        yield application
    await truncate_all(settings.database_url, TABLES)


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://order") as c:
        yield c


ADDRESS = {
    "name": "Ada Customer",
    "line1": "1 Main St",
    "city": "Springfield",
    "postal_code": "12345",
    "country": "US",
}


def checkout_body(*lines: tuple[dict, int], method: str = "tok_ok", **address) -> dict:
    """One line per ``(product, quantity)``, using the product's first variant."""
    return {
        "items": [
            {"product_id": p["id"], "variant_id": p["variants"][0]["id"], "quantity": q}
            for p, q in lines
        ],
        "shipping_address": {**ADDRESS, **address},
        "payment_method": method,
    }


async def place(client, headers, body, key: str | None = None, **extra_headers):
    key = key or str(uuid.uuid4())
    return await client.post(
        "/api/v1/orders", json=body, headers={**headers, "Idempotency-Key": key, **extra_headers}
    )


async def outbox_rows(database_url: str, event_type: str | None = None) -> list[dict]:
    import json

    import asyncpg

    conn = await asyncpg.connect(database_url.replace("postgresql+asyncpg://", "postgresql://", 1))
    try:
        rows = await conn.fetch(
            "SELECT event_type, payload::text AS p, client_id, causation_id FROM outbox "
            "WHERE $1::text IS NULL OR event_type = $1 ORDER BY id",
            event_type,
        )
    finally:
        await conn.close()
    return [
        {
            "type": r["event_type"],
            "client_id": r["client_id"],
            "causation_id": r["causation_id"],
            **json.loads(r["p"]),
        }
        for r in rows
    ]


async def gather(*calls):
    return await asyncio.gather(*calls)
