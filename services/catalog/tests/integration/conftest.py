from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from app.core.config import Settings
from app.v1.media.storage import MemoryStorage
from main import create_application
from smartretail_common.auth import JwtVerifier
from smartretail_common.testing import provision_test_database, truncate_all

SERVICE_DIR = str(Path(__file__).resolve().parents[2])
TABLES = [
    "product_audit_log",
    "product_media",
    "product_variants",
    "product_publish_failures",
    "outbox",
    "products",
    "categories",
]


class FakeStarter:
    """Stands in for Temporal at the API edge; the workflow itself is tested apart."""

    def __init__(self) -> None:
        self.started: list[dict] = []
        self.running: set[str] = set()
        self.fail_start = False
        self.step: dict | None = {"step": "validate_product", "started_at": "2026-10-11T00:00:00"}

    async def start(self, *, product_id, workflow_id, actor_id, correlation_id) -> None:
        if self.fail_start:
            raise ConnectionError("temporal unreachable")
        self.started.append(
            {"product_id": str(product_id), "workflow_id": workflow_id, "actor_id": actor_id}
        )
        self.running.add(workflow_id)

    async def is_running(self, workflow_id: str) -> bool:
        return workflow_id in self.running

    async def current_step(self, workflow_id: str) -> dict | None:
        if self.step is None:
            raise ConnectionError("temporal unreachable")
        return self.step


@pytest.fixture(scope="session")
def database_url() -> str:
    url = provision_test_database("catalog_test", SERVICE_DIR)
    if url is None:
        pytest.skip("test Postgres not reachable; run `make test-db`")
    return url


@pytest.fixture
def starter() -> FakeStarter:
    return FakeStarter()


@pytest.fixture
def storage() -> MemoryStorage:
    return MemoryStorage()


@pytest.fixture
async def app(database_url, keypair, starter, storage):
    settings = Settings(service_name="catalog", database_url=database_url, jwt_public_key_path=None)
    verifier = JwtVerifier(
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        static_keys={None: keypair.public_key},
    )
    application = create_application(
        settings, verifier=verifier, publish_starter=starter, storage=storage
    )
    yield application
    await application.state.db.dispose()
    await truncate_all(database_url, TABLES)


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://catalog") as c:
        yield c


async def create_category(client, callers, slug="shoes", parent_id=None) -> dict:
    r = await client.post(
        "/api/v1/categories",
        json={"name": slug.title(), "slug": slug, "parent_id": parent_id},
        headers=callers.admin,
    )
    assert r.status_code == 201, r.text
    return r.json()


async def create_product(client, headers, **overrides) -> dict:
    body = {"sku": "SKU-1", "name": "Trail Shoe", "base_price": "49.99", **overrides}
    r = await client.post("/api/v1/products", json=body, headers=headers)
    assert r.status_code == 201, r.text
    return r.json()


async def set_status(client, headers, product, status) -> dict:
    r = await client.post(
        f"/api/v1/products/{product['id']}/status",
        json={"status": status, "version": product["version"]},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    return r.json()


async def force_published(app, product_id: str) -> None:
    """Sets the state directly, for tests about what a published product allows.

    Publishing itself goes through the workflow; see tests/integration/publishing.
    """
    import uuid

    from sqlalchemy import update

    from app.models import Product

    async with app.state.db.sessionmaker() as s:
        await s.execute(
            update(Product).where(Product.id == uuid.UUID(product_id)).values(status="published")
        )
        await s.commit()


async def complete_product(client, callers, *, headers=None, sku="PUB-1", category_slug=None):
    """A draft that passes validate_product: description, category, a variant, media."""
    import uuid as _uuid

    headers = headers or callers.merchant_a
    category = await create_category(
        client, callers, slug=category_slug or f"c-{_uuid.uuid4().hex[:8]}"
    )
    product = await create_product(
        client, headers, sku=sku, description="A real shoe.", category_id=category["id"]
    )
    r = await client.post(
        f"/api/v1/products/{product['id']}/variants",
        json={"sku": f"{sku}-M", "options": {"size": "M"}, "version": product["version"]},
        headers=headers,
    )
    assert r.status_code == 201, r.text
    r = await client.post(
        f"/api/v1/products/{product['id']}/media",
        json={"url": "https://img.example/shoe.jpg", "version": r.json()["version"]},
        headers=headers,
    )
    assert r.status_code == 201, r.text
    r = await client.get(f"/api/v1/products/{product['id']}", headers=headers)
    return r.json()


async def publish(client, headers, product) -> "object":
    return await client.post(
        f"/api/v1/products/{product['id']}/status",
        json={"status": "published", "version": product["version"]},
        headers=headers,
    )


async def outbox_rows(database_url: str, event_type: str) -> list[dict]:
    import json

    import asyncpg

    conn = await asyncpg.connect(database_url.replace("postgresql+asyncpg://", "postgresql://", 1))
    try:
        rows = await conn.fetch(
            "SELECT payload::text AS p FROM outbox WHERE event_type = $1 ORDER BY id", event_type
        )
    finally:
        await conn.close()
    return [json.loads(r["p"]) for r in rows]
