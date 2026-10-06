from sqlalchemy import func, select

from app.models import ProductMedia
from tests.integration.conftest import create_product


async def test_variant_lifecycle(client, callers) -> None:
    product = await create_product(client, callers.merchant_a)
    r = await client.post(
        f"/api/v1/products/{product['id']}/variants",
        json={"sku": "V-M", "options": {"size": "M", "color": "red"}, "version": 1},
        headers=callers.merchant_a,
    )
    assert r.status_code == 201
    variant = r.json()
    assert variant["version"] == 2 and variant["is_active"]
    assert variant["_links"]["availability"] == f"/api/v1/inventory/{variant['id']}"

    dup = await client.post(
        f"/api/v1/products/{product['id']}/variants",
        json={"sku": "V-M", "options": {"size": "L"}, "version": 2},
        headers=callers.merchant_a,
    )
    assert dup.status_code == 409

    r = await client.patch(
        f"/api/v1/variants/{variant['id']}",
        json={"price_override": "55.00", "version": 2},
        headers=callers.merchant_a,
    )
    assert (
        r.status_code == 200 and r.json()["price_override"] == "55.00" and r.json()["version"] == 3
    )

    other = await client.patch(
        f"/api/v1/variants/{variant['id']}",
        json={"is_active": False, "version": 3},
        headers=callers.merchant_b,
    )
    assert other.status_code == 404

    r = await client.delete(
        f"/api/v1/variants/{variant['id']}?version=3", headers=callers.merchant_a
    )
    assert r.status_code == 200 and r.json() == {
        "id": variant["id"],
        "is_active": False,
        "version": 4,
    }

    detail = (
        await client.get(f"/api/v1/products/{product['id']}", headers=callers.merchant_a)
    ).json()
    assert detail["variants"][0]["is_active"] is False  # deactivated, not deleted
    assert detail["version"] == 4

    history = (
        await client.get(f"/api/v1/products/{product['id']}/history", headers=callers.merchant_a)
    ).json()
    assert [e["action"] for e in history["items"]][:3] == [
        "variant_removed",
        "variant_updated",
        "variant_added",
    ]


async def test_customer_cannot_add_variant(client, callers) -> None:
    product = await create_product(client, callers.merchant_a)
    r = await client.post(
        f"/api/v1/products/{product['id']}/variants",
        json={"sku": "V", "options": {}, "version": 1},
        headers=callers.customer,
    )
    assert r.status_code == 403


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
