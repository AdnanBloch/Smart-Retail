"""Category edge cases: partial updates, re-parenting, deletes of what is in use."""

import uuid

from tests.integration.conftest import create_category, create_product


async def test_patch_renames_and_ignores_null_name_and_slug(client, callers) -> None:
    category = await create_category(client, callers, "boots")
    url = f"/api/v1/categories/{category['id']}"
    r = await client.patch(
        url, json={"name": "Winter Boots", "slug": "winter-boots"}, headers=callers.admin
    )
    assert r.status_code == 200
    assert (r.json()["name"], r.json()["slug"]) == ("Winter Boots", "winter-boots")
    # null for a required column means "leave as is", not "clear".
    r = await client.patch(url, json={"name": None, "slug": None}, headers=callers.admin)
    assert r.status_code == 200
    assert (r.json()["name"], r.json()["slug"]) == ("Winter Boots", "winter-boots")


async def test_patch_to_a_taken_slug_is_409(client, callers) -> None:
    await create_category(client, callers, "taken")
    other = await create_category(client, callers, "other")
    r = await client.patch(
        f"/api/v1/categories/{other['id']}", json={"slug": "taken"}, headers=callers.admin
    )
    assert r.status_code == 409 and r.json()["type"].endswith("/duplicate-slug")
    assert (await client.get(f"/api/v1/categories/{other['id']}")).json()["slug"] == "other"


async def test_reparent_and_detach(client, callers) -> None:
    a = await create_category(client, callers, "a")
    b = await create_category(client, callers, "b")
    child = await create_category(client, callers, "child", a["id"])
    url = f"/api/v1/categories/{child['id']}"

    r = await client.patch(url, json={"parent_id": b["id"]}, headers=callers.admin)
    assert r.status_code == 200 and r.json()["parent_id"] == b["id"]
    assert [p["slug"] for p in (await client.get(url)).json()["path"]] == ["b"]

    r = await client.patch(url, json={"parent_id": None}, headers=callers.admin)
    assert r.status_code == 200 and r.json()["parent_id"] is None
    assert (await client.get(url)).json()["path"] == []


async def test_patch_unknown_parent_or_self_is_422(client, callers) -> None:
    category = await create_category(client, callers, "solo")
    url = f"/api/v1/categories/{category['id']}"
    for parent in (str(uuid.uuid4()), category["id"]):
        r = await client.patch(url, json={"parent_id": parent}, headers=callers.admin)
        assert r.status_code == 422 and r.json()["violations"][0]["field"] == "parent_id"


async def test_unknown_category_is_404_everywhere(client, callers) -> None:
    url = f"/api/v1/categories/{uuid.uuid4()}"
    assert (await client.get(url)).status_code == 404
    assert (await client.patch(url, json={"name": "x"}, headers=callers.admin)).status_code == 404
    assert (await client.delete(url, headers=callers.admin)).status_code == 404


async def test_writes_are_admin_only(client, callers) -> None:
    category = await create_category(client, callers, "guarded")
    url = f"/api/v1/categories/{category['id']}"
    for headers in (callers.merchant_a, callers.customer):
        assert (await client.patch(url, json={"name": "x"}, headers=headers)).status_code == 403
        assert (await client.delete(url, headers=headers)).status_code == 403


async def test_an_archived_product_still_holds_its_category(client, callers) -> None:
    """Products are soft-deleted (S5), so the category stays referenced."""
    category = await create_category(client, callers, "kept")
    product = await create_product(client, callers.merchant_a, category_id=category["id"])
    await client.delete(f"/api/v1/products/{product['id']}?version=1", headers=callers.merchant_a)
    r = await client.delete(f"/api/v1/categories/{category['id']}", headers=callers.admin)
    assert r.status_code == 409 and r.json()["type"].endswith("/category-in-use")
