"""Variants through /api/v1/products/{id}/variants and /api/v1/variants."""

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
