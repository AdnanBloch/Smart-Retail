import uuid

import pytest
from sqlalchemy import func, select

from app.models import ProductAuditLog
from tests.integration.conftest import create_category, create_product, force_published, set_status


async def test_merchant_creates_draft_owned_by_token_subject(client, callers) -> None:
    product = await create_product(client, callers.merchant_a, merchant_id=str(uuid.uuid4()))
    assert product["status"] == "draft" and product["version"] == 1
    # A merchant's body merchant_id is ignored, not obeyed.
    assert product["merchant_id"] == callers.merchant_a_id


async def test_create_role_matrix(client, callers) -> None:
    body = {"sku": "X", "name": "X", "base_price": "1.00"}
    assert (await client.post("/api/v1/products", json=body)).status_code == 401
    assert (
        await client.post("/api/v1/products", json=body, headers=callers.customer)
    ).status_code == 403
    r = await client.post("/api/v1/products", json=body, headers=callers.admin)
    assert r.status_code == 422  # admin must name the merchant
    r = await client.post(
        "/api/v1/products",
        json={**body, "merchant_id": callers.merchant_b_id},
        headers=callers.admin,
    )
    assert r.status_code == 201 and r.json()["merchant_id"] == callers.merchant_b_id


async def test_duplicate_sku_per_merchant(client, callers) -> None:
    await create_product(client, callers.merchant_a, sku="DUP")
    r = await client.post(
        "/api/v1/products",
        json={"sku": "DUP", "name": "n", "base_price": "1"},
        headers=callers.merchant_a,
    )
    assert r.status_code == 409 and r.json()["type"].endswith("/duplicate-sku")
    # Same SKU under another merchant is fine.
    await create_product(client, callers.merchant_b, sku="DUP")


async def test_unknown_category_is_422(client, callers) -> None:
    r = await client.post(
        "/api/v1/products",
        json={"sku": "X", "name": "n", "base_price": "1", "category_id": str(uuid.uuid4())},
        headers=callers.merchant_a,
    )
    assert r.status_code == 422


async def test_update_ownership_and_version(client, callers) -> None:
    product = await create_product(client, callers.merchant_a)
    url = f"/api/v1/products/{product['id']}"

    r = await client.patch(url, json={"name": "Renamed", "version": 1}, headers=callers.merchant_a)
    assert r.status_code == 200 and r.json()["name"] == "Renamed" and r.json()["version"] == 2

    stale = await client.patch(
        url, json={"name": "Again", "version": 1}, headers=callers.merchant_a
    )
    assert stale.status_code == 409
    assert (
        stale.json()["type"].endswith("/version-conflict") and stale.json()["current_version"] == 2
    )

    # Another merchant: a draft is invisible to them, so 404 rather than 403.
    other = await client.patch(
        url, json={"name": "Hijack", "version": 2}, headers=callers.merchant_b
    )
    assert other.status_code == 404

    r = await client.patch(url, json={"base_price": "10.00", "version": 2}, headers=callers.admin)
    assert r.status_code == 200 and r.json()["base_price"] == "10.00"


async def test_other_merchant_gets_403_on_visible_product(app, client, callers) -> None:
    product = await create_product(client, callers.merchant_a)
    await force_published(app, product["id"])
    r = await client.patch(
        f"/api/v1/products/{product['id']}",
        json={"name": "x", "version": 1},
        headers=callers.merchant_b,
    )
    assert r.status_code == 403


async def test_visibility(app, client, callers) -> None:
    draft = await create_product(client, callers.merchant_a, sku="D")
    published = await create_product(client, callers.merchant_a, sku="P")
    await force_published(app, published["id"])

    anon = (await client.get("/api/v1/products")).json()
    assert [p["sku"] for p in anon["items"]] == ["P"]

    for headers, status in (
        (callers.anonymous, 404),
        (callers.customer, 404),
        (callers.merchant_b, 404),
        (callers.merchant_a, 200),
        (callers.admin, 200),
    ):
        assert (
            await client.get(f"/api/v1/products/{draft['id']}", headers=headers)
        ).status_code == status

    mine = (await client.get("/api/v1/products/me", headers=callers.merchant_a)).json()
    assert {p["sku"] for p in mine["items"]} == {"D", "P"}
    assert (await client.get("/api/v1/products/me", headers=callers.merchant_b)).json()[
        "total"
    ] == 0
    assert (await client.get("/api/v1/products/me", headers=callers.admin)).status_code == 403
    assert (await client.get("/api/v1/products/me", headers=callers.customer)).status_code == 403

    everything = (await client.get("/api/v1/products", headers=callers.admin)).json()
    assert everything["total"] == 2


async def test_status_filter_rules(app, client, callers) -> None:
    await create_product(client, callers.merchant_a, sku="D")
    url = "/api/v1/products?status=draft"
    # Ignored for anonymous and for a merchant not filtering by their own id.
    assert (await client.get(url)).json()["total"] == 0
    assert (await client.get(url, headers=callers.merchant_a)).json()["total"] == 0
    own = f"{url}&merchant_id={callers.merchant_a_id}"
    assert (await client.get(own, headers=callers.merchant_a)).json()["total"] == 1
    assert (await client.get(own, headers=callers.merchant_b)).json()["total"] == 0
    assert (await client.get(url, headers=callers.admin)).json()["total"] == 1


async def test_status_transitions(client, callers) -> None:
    product = await create_product(client, callers.merchant_a)
    product = await set_status(client, callers.merchant_a, product, "inactive")
    assert product["status"] == "inactive"
    product = await set_status(client, callers.merchant_a, product, "draft")
    assert product["status"] == "draft" and product["version"] == 3

    r = await client.post(
        f"/api/v1/products/{product['id']}/status",
        json={"status": "published", "version": product["version"]},
        headers=callers.merchant_a,
    )
    assert r.status_code == 501

    r = await client.post(
        f"/api/v1/products/{product['id']}/status",
        json={"status": "publishing", "version": product["version"]},
        headers=callers.merchant_a,
    )
    assert r.status_code == 422

    r = await client.post(
        f"/api/v1/products/{product['id']}/status",
        json={"status": "draft", "version": product["version"]},
        headers=callers.merchant_a,
    )
    assert r.status_code == 409 and r.json()["type"].endswith("/illegal-transition")


@pytest.mark.parametrize("start", ["draft", "inactive"])
async def test_archive_from_each_state_then_terminal(client, callers, start) -> None:
    product = await create_product(client, callers.merchant_a)
    if start == "inactive":
        product = await set_status(client, callers.merchant_a, product, "inactive")
    r = await client.delete(
        f"/api/v1/products/{product['id']}?version={product['version']}", headers=callers.merchant_a
    )
    assert r.status_code == 200
    archived = r.json()
    assert archived["status"] == "archived"

    r = await client.post(
        f"/api/v1/products/{product['id']}/status",
        json={"status": "draft", "version": archived["version"]},
        headers=callers.merchant_a,
    )
    assert r.status_code == 409
    r = await client.patch(
        f"/api/v1/products/{product['id']}",
        json={"name": "x", "version": archived["version"]},
        headers=callers.merchant_a,
    )
    assert r.status_code == 409


async def test_audit_rows_and_history(app, client, callers) -> None:
    product = await create_product(client, callers.merchant_a)
    await client.patch(
        f"/api/v1/products/{product['id']}",
        json={"name": "New", "version": 1},
        headers={**callers.merchant_a, "X-Request-ID": "corr-1"},
    )
    history = (
        await client.get(f"/api/v1/products/{product['id']}/history", headers=callers.merchant_a)
    ).json()
    assert [e["action"] for e in history["items"]] == ["updated", "created"]
    update = history["items"][0]
    assert update["actor_id"] == callers.merchant_a_id
    assert update["changes"]["name"] == {"from": "Trail Shoe", "to": "New"}
    assert update["changes"]["_correlation_id"] == "corr-1"

    assert (
        await client.get(f"/api/v1/products/{product['id']}/history", headers=callers.merchant_b)
    ).status_code == 404
    assert (
        await client.get(f"/api/v1/products/{product['id']}/history", headers=callers.admin)
    ).status_code == 200


async def test_failed_update_leaves_no_audit_row(app, client, callers) -> None:
    product = await create_product(client, callers.merchant_a, sku="A")
    await create_product(client, callers.merchant_a, sku="B")
    r = await client.patch(
        f"/api/v1/products/{product['id']}",
        json={"sku": "B", "version": 1},
        headers=callers.merchant_a,
    )
    assert r.status_code == 409
    async with app.state.db.sessionmaker() as s:
        count = await s.scalar(
            select(func.count())
            .select_from(ProductAuditLog)
            .where(ProductAuditLog.action == "updated")
        )
    assert count == 0


async def test_listing_filters_sort_and_paging(app, client, callers) -> None:
    shoes = await create_category(client, callers, "shoes")
    specs = [
        ("A", "Alpha Runner", "30.00", shoes["id"]),
        ("B", "Bravo Boot", "80.00", shoes["id"]),
        ("C", "Charlie Hat", "15.00", None),
    ]
    for sku, name, price, category in specs:
        p = await create_product(
            client, callers.merchant_a, sku=sku, name=name, base_price=price, category_id=category
        )
        await force_published(app, p["id"])

    get = lambda q: client.get(f"/api/v1/products?{q}")  # noqa: E731
    assert (await get(f"category_id={shoes['id']}")).json()["total"] == 2
    assert (await get("min_price=20&max_price=50")).json()["items"][0]["sku"] == "A"
    assert (await get("q=boot")).json()["items"][0]["sku"] == "B"
    assert (await get("q=%25")).json()["total"] == 0  # LIKE wildcards are escaped
    assert [p["sku"] for p in (await get("sort=price")).json()["items"]] == ["C", "A", "B"]
    assert [p["sku"] for p in (await get("sort=-name")).json()["items"]] == ["C", "B", "A"]
    page = (await get("sort=name&limit=1&offset=1")).json()
    assert page["total"] == 3 and [p["sku"] for p in page["items"]] == ["B"]
    assert (await get("limit=1000")).json()["limit"] == 100
    assert (await get("sort=bogus")).status_code == 422
    assert (await get("min_price=50&max_price=10")).status_code == 422
    assert (await get(f"merchant_id={callers.merchant_b_id}")).json()["total"] == 0
