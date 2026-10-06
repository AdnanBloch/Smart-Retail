"""Media through /api/v1/products/{id}/media and /api/v1/media."""

from sqlalchemy import func, select

from app.models import ProductMedia
from tests.integration.conftest import create_product


async def test_media_attach_and_remove(app, client, callers) -> None:
    product = await create_product(client, callers.merchant_a)
    r = await client.post(
        f"/api/v1/products/{product['id']}/media",
        json={"url": "https://cdn.example.com/a.jpg", "alt_text": "front", "version": 1},
        headers=callers.merchant_a,
    )
    assert r.status_code == 201
    media = r.json()
    assert media["version"] == 2

    r = await client.post(
        f"/api/v1/products/{product['id']}/media",
        json={"url": "http://cdn.example.com/a.jpg", "version": 2},
        headers=callers.merchant_a,
    )
    assert r.status_code == 422

    listing = (await client.get("/api/v1/products/me", headers=callers.merchant_a)).json()
    assert listing["items"][0]["primary_media_url"] == "https://cdn.example.com/a.jpg"

    stale = await client.delete(
        f"/api/v1/media/{media['id']}?version=1", headers=callers.merchant_a
    )
    assert stale.status_code == 409
    r = await client.delete(f"/api/v1/media/{media['id']}?version=2", headers=callers.merchant_a)
    assert r.status_code == 204
    async with app.state.db.sessionmaker() as s:
        assert await s.scalar(select(func.count()).select_from(ProductMedia)) == 0


async def test_media_cascade_on_product_delete(app, client, callers) -> None:
    import uuid

    from sqlalchemy import delete

    from app.models import Product

    product = await create_product(client, callers.merchant_a)
    await client.post(
        f"/api/v1/products/{product['id']}/media",
        json={"url": "https://cdn.example.com/b.jpg", "version": 1},
        headers=callers.merchant_a,
    )
    async with app.state.db.sessionmaker() as s:
        await s.execute(delete(Product).where(Product.id == uuid.UUID(product["id"])))
        await s.commit()
        assert await s.scalar(select(func.count()).select_from(ProductMedia)) == 0
