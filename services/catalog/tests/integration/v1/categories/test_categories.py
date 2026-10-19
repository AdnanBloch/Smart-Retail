import uuid

from tests.integration.conftest import create_category, create_product


async def test_admin_creates_others_cannot(client, callers) -> None:
    body = {"name": "Shoes", "slug": "shoes"}
    assert (await client.post("/api/v1/categories", json=body)).status_code == 401
    for headers in (callers.customer, callers.merchant_a):
        assert (
            await client.post("/api/v1/categories", json=body, headers=headers)
        ).status_code == 403
    r = await client.post("/api/v1/categories", json=body, headers=callers.admin)
    assert r.status_code == 201
    assert r.headers["location"] == f"/api/v1/categories/{r.json()['id']}"


async def test_duplicate_slug_and_bad_slug(client, callers) -> None:
    await create_category(client, callers, "shoes")
    r = await client.post(
        "/api/v1/categories", json={"name": "S", "slug": "shoes"}, headers=callers.admin
    )
    assert r.status_code == 409 and r.json()["type"].endswith("/duplicate-slug")
    r = await client.post(
        "/api/v1/categories", json={"name": "S", "slug": "Bad Slug"}, headers=callers.admin
    )
    assert r.status_code == 422


async def test_tree_flat_and_path(client, callers) -> None:
    root = await create_category(client, callers, "apparel")
    child = await create_category(client, callers, "apparel-men", root["id"])
    grandchild = await create_category(client, callers, "apparel-men-shoes", child["id"])

    tree = (await client.get("/api/v1/categories")).json()
    assert tree[0]["slug"] == "apparel"
    assert tree[0]["children"][0]["children"][0]["slug"] == "apparel-men-shoes"

    flat = (await client.get(f"/api/v1/categories?view=flat&parent_id={root['id']}")).json()
    assert flat["total"] == 1 and flat["items"][0]["slug"] == "apparel-men"

    detail = (await client.get(f"/api/v1/categories/{grandchild['id']}")).json()
    assert [p["slug"] for p in detail["path"]] == ["apparel", "apparel-men"]


async def test_unknown_parent_and_cycle_are_422(client, callers) -> None:
    r = await client.post(
        "/api/v1/categories",
        json={"name": "X", "slug": "x", "parent_id": str(uuid.uuid4())},
        headers=callers.admin,
    )
    assert r.status_code == 422
    root = await create_category(client, callers, "root")
    child = await create_category(client, callers, "child", root["id"])
    for parent in (child["id"], root["id"]):
        r = await client.patch(
            f"/api/v1/categories/{root['id']}", json={"parent_id": parent}, headers=callers.admin
        )
        assert r.status_code == 422


async def test_delete_in_use_is_409(client, callers) -> None:
    root = await create_category(client, callers, "root")
    await create_category(client, callers, "child", root["id"])
    assert (
        await client.delete(f"/api/v1/categories/{root['id']}", headers=callers.admin)
    ).status_code == 409

    used = await create_category(client, callers, "used")
    await create_product(client, callers.merchant_a, category_id=used["id"])
    r = await client.delete(f"/api/v1/categories/{used['id']}", headers=callers.admin)
    assert r.status_code == 409 and r.json()["type"].endswith("/category-in-use")

    free = await create_category(client, callers, "free")
    assert (
        await client.delete(f"/api/v1/categories/{free['id']}", headers=callers.admin)
    ).status_code == 204
    assert (await client.get(f"/api/v1/categories/{free['id']}")).status_code == 404
