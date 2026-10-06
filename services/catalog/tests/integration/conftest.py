from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from app.core.config import Settings
from main import create_application
from smartretail_common.auth import JwtVerifier
from smartretail_common.testing import provision_test_database, truncate_all

SERVICE_DIR = str(Path(__file__).resolve().parents[2])
TABLES = ["product_audit_log", "product_media", "product_variants", "products", "categories"]


@pytest.fixture(scope="session")
def database_url() -> str:
    url = provision_test_database("catalog_test", SERVICE_DIR)
    if url is None:
        pytest.skip("test Postgres not reachable; run `make test-db`")
    return url


@pytest.fixture
async def app(database_url, keypair):
    settings = Settings(service_name="catalog", database_url=database_url, jwt_public_key_path=None)
    verifier = JwtVerifier(
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        static_keys={None: keypair.public_key},
    )
    application = create_application(settings, verifier=verifier)
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
    """Publishing is a Milestone 2 workflow; tests set the state directly."""
    import uuid

    from sqlalchemy import update

    from app.models import Product

    async with app.state.db.sessionmaker() as s:
        await s.execute(
            update(Product).where(Product.id == uuid.UUID(product_id)).values(status="published")
        )
        await s.commit()
