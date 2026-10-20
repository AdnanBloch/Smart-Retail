"""Variant edge cases: SKU uniqueness scope, price-override rules, no-op writes."""

import uuid

import pytest

from tests.integration.conftest import create_product


async def _add(client, headers, product, *, sku="V-1", version=None, **extra):
    return await client.post(
        f"/api/v1/products/{product['id']}/variants",
        json={"sku": sku, "options": {}, "version": version or product["version"], **extra},
        headers=headers,
    )


async def test_variant_sku_is_unique_per_product_only(client, callers) -> None:
    one = await create_product(client, callers.merchant_a, sku="P1")
    two = await create_product(client, callers.merchant_a, sku="P2")
    assert (await _add(client, callers.merchant_a, one, sku="SHARED")).status_code == 201
    # Another product of the same merchant may reuse it.
    assert (await _add(client, callers.merchant_a, two, sku="SHARED")).status_code == 201


async def test_patch_to_a_taken_sku_is_409_and_changes_nothing(client, callers) -> None:
    product = await create_product(client, callers.merchant_a)
    await _add(client, callers.merchant_a, product, sku="A")
    b = (await _add(client, callers.merchant_a, product, sku="B", version=2)).json()
    r = await client.patch(
        f"/api/v1/variants/{b['id']}", json={"sku": "A", "version": 3}, headers=callers.merchant_a
    )
    assert r.status_code == 409 and r.json()["type"].endswith("/duplicate-sku")
    detail = (
        await client.get(f"/api/v1/products/{product['id']}", headers=callers.merchant_a)
    ).json()
    assert sorted(v["sku"] for v in detail["variants"]) == ["A", "B"]
    assert detail["version"] == 3


@pytest.mark.parametrize(
    ("price", "status"),
    [
        ("0", 201),  # a free variant is allowed
        ("12.50", 201),
        ("-0.01", 422),
        ("1.005", 422),  # two decimal places
        ("10000000000.00", 422),  # Numeric(12, 2)
        ("abc", 422),
    ],
)
async def test_price_override_rules(client, callers, price, status) -> None:
    product = await create_product(client, callers.merchant_a)
    r = await _add(client, callers.merchant_a, product, price_override=price)
    assert r.status_code == status, r.text
    if status == 201:
        assert r.json()["price_override"] == f"{float(price):.2f}"


async def test_price_override_can_be_cleared_but_required_fields_cannot(client, callers) -> None:
    product = await create_product(client, callers.merchant_a)
    v = (await _add(client, callers.merchant_a, product, price_override="5.00")).json()
    url = f"/api/v1/variants/{v['id']}"
    r = await client.patch(
        url, json={"price_override": None, "version": 2}, headers=callers.merchant_a
    )
    assert r.status_code == 200 and r.json()["price_override"] is None
    for field in ("sku", "options", "is_active"):
        r = await client.patch(url, json={field: None, "version": 3}, headers=callers.merchant_a)
        assert r.status_code == 422, (field, r.text)


async def test_noop_patch_and_repeated_deactivate_do_not_bump(client, callers) -> None:
    product = await create_product(client, callers.merchant_a)
    v = (await _add(client, callers.merchant_a, product, sku="SAME")).json()
    url = f"/api/v1/variants/{v['id']}"
    r = await client.patch(url, json={"sku": "SAME", "version": 2}, headers=callers.merchant_a)
    assert r.status_code == 200 and r.json()["version"] == 2

    assert (await client.delete(f"{url}?version=2", headers=callers.merchant_a)).json()[
        "version"
    ] == 3
    again = await client.delete(f"{url}?version=3", headers=callers.merchant_a)
    assert again.status_code == 200 and again.json()["version"] == 3
    history = (
        await client.get(f"/api/v1/products/{product['id']}/history", headers=callers.merchant_a)
    ).json()
    assert [e["action"] for e in history["items"]].count("variant_removed") == 1


async def test_unknown_variant_is_404(client, callers) -> None:
    url = f"/api/v1/variants/{uuid.uuid4()}"
    r = await client.patch(url, json={"sku": "x", "version": 1}, headers=callers.admin)
    assert r.status_code == 404 and r.json()["type"].endswith("/variant-not-found")
    assert (await client.delete(f"{url}?version=1", headers=callers.admin)).status_code == 404


async def test_options_map_is_bounded(client, callers) -> None:
    product = await create_product(client, callers.merchant_a)
    for options in ({"k": {"nested": 1}}, {"": "x"}, {f"k{i}": i for i in range(51)}):
        r = await client.post(
            f"/api/v1/products/{product['id']}/variants",
            json={"sku": "V", "options": options, "version": 1},
            headers=callers.merchant_a,
        )
        assert r.status_code == 422, options
