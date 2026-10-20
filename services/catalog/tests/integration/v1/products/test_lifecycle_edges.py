"""Product lifecycle edge cases (RFC 0004 §3.1.1.4, mentor review 2026-10-20).

The state machine's edges at the API: what each status refuses, what a
no-op write does to ``version``, and how the publish path treats products
that are not ``draft`` or ``inactive``.
"""

import uuid

import pytest

from tests.integration.conftest import (
    complete_product,
    create_category,
    create_product,
    force_published,
    publish,
    set_status,
)


async def _status(client, headers, product, status):
    return await client.post(
        f"/api/v1/products/{product['id']}/status",
        json={"status": status, "version": product["version"]},
        headers=headers,
    )


async def test_publish_from_inactive_is_accepted(client, callers, starter) -> None:
    """T4: an inactive product may be published straight away."""
    product = await complete_product(client, callers)
    product = await set_status(client, callers.merchant_a, product, "inactive")
    r = await publish(client, callers.merchant_a, product)
    assert r.status_code == 202 and r.json()["status"] == "publishing"
    assert len(starter.started) == 1


async def test_publishing_an_incomplete_product_is_accepted_then_validated(
    client, callers, starter
) -> None:
    """Completeness is the workflow's step 1, not the API's: the accept only commits
    ``publishing``. The workflow test covers the compensation back to ``draft``."""
    product = await create_product(client, callers.merchant_a)
    r = await publish(client, callers.merchant_a, product)
    assert r.status_code == 202
    assert [s["product_id"] for s in starter.started] == [product["id"]]


@pytest.mark.parametrize("start", ["published", "archived"])
async def test_publish_from_published_or_archived_is_an_illegal_transition(
    app, client, callers, starter, start
) -> None:
    product = await complete_product(client, callers)
    if start == "published":
        await force_published(app, product["id"])
    else:
        r = await client.delete(
            f"/api/v1/products/{product['id']}?version={product['version']}",
            headers=callers.merchant_a,
        )
        product = {**product, "version": r.json()["version"]}
    r = await publish(client, callers.merchant_a, product)
    assert r.status_code == 409 and r.json()["type"].endswith("/illegal-transition")
    assert (r.json()["from"], r.json()["to"]) == (start, "publishing")
    assert starter.started == []


@pytest.mark.parametrize(
    ("chain", "target"),
    [
        ([], "draft"),  # draft → draft
        (["inactive"], "inactive"),  # inactive → inactive
    ],
)
async def test_self_transitions_are_refused(client, callers, chain, target) -> None:
    product = await create_product(client, callers.merchant_a)
    for status in chain:
        product = await set_status(client, callers.merchant_a, product, status)
    r = await _status(client, callers.merchant_a, product, target)
    assert r.status_code == 409 and r.json()["type"].endswith("/illegal-transition")
    after = (
        await client.get(f"/api/v1/products/{product['id']}", headers=callers.merchant_a)
    ).json()
    assert after["version"] == product["version"]  # a refused transition bumps nothing


@pytest.mark.parametrize("target", ["draft", "inactive", "archived"])
async def test_archived_is_terminal(client, callers, target) -> None:
    product = await create_product(client, callers.merchant_a)
    archived = (
        await client.delete(f"/api/v1/products/{product['id']}?version=1", headers=callers.admin)
    ).json()
    if target == "archived":
        r = await client.delete(
            f"/api/v1/products/{product['id']}?version={archived['version']}",
            headers=callers.admin,
        )
    else:
        r = await _status(client, callers.admin, {**product, **archived}, target)
    assert r.status_code == 409 and r.json()["type"].endswith("/illegal-transition")


async def test_published_to_inactive_unpublishes(app, client, callers) -> None:
    product = await complete_product(client, callers)
    await force_published(app, product["id"])
    r = await _status(client, callers.merchant_a, product, "inactive")
    assert r.status_code == 200 and r.json()["status"] == "inactive"
    assert (await client.get(f"/api/v1/products/{product['id']}")).status_code == 404


async def test_unknown_status_value_is_422(client, callers) -> None:
    product = await create_product(client, callers.merchant_a)
    r = await _status(client, callers.merchant_a, product, "deleted")
    assert r.status_code == 422


async def test_writes_to_an_unknown_product_are_404(client, callers) -> None:
    missing = str(uuid.uuid4())
    base = f"/api/v1/products/{missing}"
    calls = [
        client.get(base, headers=callers.admin),
        client.patch(base, json={"name": "x", "version": 1}, headers=callers.admin),
        client.post(
            f"{base}/status", json={"status": "draft", "version": 1}, headers=callers.admin
        ),
        client.post(
            f"{base}/status", json={"status": "published", "version": 1}, headers=callers.admin
        ),
        client.delete(f"{base}?version=1", headers=callers.admin),
        client.get(f"{base}/history", headers=callers.admin),
        client.post(
            f"{base}/variants",
            json={"sku": "V", "options": {}, "version": 1},
            headers=callers.admin,
        ),
    ]
    for call in calls:
        r = await call
        assert r.status_code == 404 and r.json()["type"].endswith("/product-not-found"), r.text


async def test_noop_patch_keeps_the_version_and_writes_no_audit(client, callers) -> None:
    product = await create_product(client, callers.merchant_a, description="same")
    r = await client.patch(
        f"/api/v1/products/{product['id']}",
        json={"name": product["name"], "description": "same", "version": 1},
        headers=callers.merchant_a,
    )
    assert r.status_code == 200 and r.json()["version"] == 1
    history = (
        await client.get(f"/api/v1/products/{product['id']}/history", headers=callers.merchant_a)
    ).json()
    assert [e["action"] for e in history["items"]] == ["created"]


async def test_patch_category_must_exist_and_can_be_cleared(client, callers) -> None:
    product = await create_product(client, callers.merchant_a)
    url = f"/api/v1/products/{product['id']}"
    r = await client.patch(
        url, json={"category_id": str(uuid.uuid4()), "version": 1}, headers=callers.merchant_a
    )
    assert r.status_code == 422
    assert r.json()["violations"][0]["field"] == "category_id"

    category = await create_category(client, callers, "boots")
    r = await client.patch(
        url, json={"category_id": category["id"], "version": 1}, headers=callers.merchant_a
    )
    assert r.status_code == 200 and r.json()["category_id"] == category["id"]
    r = await client.patch(
        url, json={"category_id": None, "version": 2}, headers=callers.merchant_a
    )
    assert r.status_code == 200 and r.json()["category_id"] is None


@pytest.mark.parametrize(
    "body",
    [
        {"base_price": "-1"},
        {"base_price": "1.001"},
        {"base_price": "10000000000.00"},  # 12 digits max
        {"currency": "usd"},
        {"sku": ""},
        {"sku": "x" * 65},
        {"attributes": {"nested": {"a": 1}}},
        {"unexpected": 1},
    ],
)
async def test_create_body_rules(client, callers, body) -> None:
    r = await client.post(
        "/api/v1/products",
        json={"sku": "S", "name": "n", "base_price": "1", **body},
        headers=callers.merchant_a,
    )
    assert r.status_code == 422, r.text


async def test_my_products_status_filter(client, callers) -> None:
    await create_product(client, callers.merchant_a, sku="D")
    inactive = await create_product(client, callers.merchant_a, sku="I")
    await set_status(client, callers.merchant_a, inactive, "inactive")
    mine = (
        await client.get("/api/v1/products/me?status=inactive", headers=callers.merchant_a)
    ).json()
    assert [p["sku"] for p in mine["items"]] == ["I"]
