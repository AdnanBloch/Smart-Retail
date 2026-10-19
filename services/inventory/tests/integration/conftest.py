import uuid
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from app.core.config import Settings
from main import create_application
from smartretail_common.auth import JwtVerifier
from smartretail_common.testing import provision_test_database, truncate_all

SERVICE_DIR = str(Path(__file__).resolve().parents[2])
TABLES = ["reservations", "stock_movements", "stock_items", "outbox", "processed_events"]


@pytest.fixture(scope="session")
def database_url() -> str:
    url = provision_test_database("inventory_test", SERVICE_DIR)
    if url is None:
        pytest.skip("test Postgres not reachable; run `make test-db`")
    return url


@pytest.fixture
def settings(database_url) -> Settings:
    return Settings(
        service_name="inventory",
        database_url=database_url,
        jwt_public_key_path=None,
        db_pool_size=20,
        db_max_overflow=40,
    )


@pytest.fixture
async def app(settings, keypair):
    verifier = JwtVerifier(
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        static_keys={None: keypair.public_key},
    )
    application = create_application(settings, verifier=verifier)
    yield application
    await application.state.db.dispose()
    await truncate_all(settings.database_url, TABLES)


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://inventory") as c:
        yield c


async def init_stock(client, callers, merchant_id: str, *, variants: int = 1, on_hand: int = 0):
    """Initialise stock rows the way the publishing workflow does, then set quantities."""
    body = {
        "product_id": str(uuid.uuid4()),
        "merchant_id": merchant_id,
        "variants": [{"variant_id": str(uuid.uuid4()), "sku": f"SKU-{i}"} for i in range(variants)],
    }
    r = await client.post("/internal/v1/inventory/stock-items", json=body, headers=callers.service)
    assert r.status_code == 200, r.text
    items = r.json()["items"]
    if on_hand:
        for item in items:
            r = await client.put(
                f"/api/v1/inventory/{item['variant_id']}",
                json={"on_hand": on_hand},
                headers=callers.admin,
            )
            assert r.status_code == 200, r.text
    return [item["variant_id"] for item in items]


async def reserve(client, callers, order_id: str, lines: list[tuple[str, int]]):
    return await client.post(
        "/internal/v1/inventory/reservations",
        json={
            "order_id": order_id,
            "lines": [{"variant_id": v, "quantity": q} for v, q in lines],
        },
        headers=callers.service,
    )


async def public(client, variant_id: str) -> dict:
    r = await client.get(f"/api/v1/inventory/{variant_id}")
    assert r.status_code == 200, r.text
    return r.json()
