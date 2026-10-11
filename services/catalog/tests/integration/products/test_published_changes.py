"""CAT-13 and the unpublish events: what writing to a published product does."""

from tests.integration.conftest import complete_product, force_published, outbox_rows


async def _published(client, callers, app) -> dict:
    product = await complete_product(client, callers)
    await force_published(app, product["id"])
    r = await client.get(f"/api/v1/products/{product['id']}", headers=callers.merchant_a)
    return r.json()


async def test_patch_writes_updated_with_a_fresh_search_document(
    client, callers, app, database_url
) -> None:
    product = await _published(client, callers, app)
    r = await client.patch(
        f"/api/v1/products/{product['id']}",
        json={"name": "Trail Shoe II", "version": product["version"]},
        headers=callers.merchant_a,
    )
    assert r.status_code == 200, r.text
    [event] = await outbox_rows(database_url, "catalog.product.updated")
    assert event["changed_fields"] == ["name"]
    assert event["search_document"]["name"] == "Trail Shoe II"
    assert event["version"] == r.json()["version"]


async def test_changes_that_would_break_a_published_product_are_refused(
    client, callers, app, database_url
) -> None:
    product = await _published(client, callers, app)
    pid, version = product["id"], product["version"]

    r = await client.patch(
        f"/api/v1/products/{pid}",
        json={"category_id": None, "version": version},
        headers=callers.merchant_a,
    )
    assert r.status_code == 409 and r.json()["missing"] == ["category_id"]

    [variant] = product["variants"]
    r = await client.delete(
        f"/api/v1/variants/{variant['id']}?version={version}", headers=callers.merchant_a
    )
    assert r.status_code == 409 and r.json()["type"].endswith("/would-break-published")

    [media] = product["media"]
    r = await client.delete(
        f"/api/v1/media/{media['id']}?version={version}", headers=callers.merchant_a
    )
    assert r.status_code == 409

    # Nothing was changed by the refused writes.
    after = (await client.get(f"/api/v1/products/{pid}")).json()
    assert after["version"] == version and after["category_id"] is not None
    assert await outbox_rows(database_url, "catalog.product.updated") == []

    # A second variant makes the first removable, and adding it is an update too.
    r = await client.post(
        f"/api/v1/products/{pid}/variants",
        json={"sku": "PUB-1-L", "options": {"size": "L"}, "version": version},
        headers=callers.merchant_a,
    )
    assert r.status_code == 201
    r = await client.patch(
        f"/api/v1/variants/{variant['id']}",
        json={"price_override": "10.00", "version": r.json()["version"]},
        headers=callers.merchant_a,
    )
    r = await client.delete(
        f"/api/v1/variants/{variant['id']}?version={r.json()['version']}",
        headers=callers.merchant_a,
    )
    assert r.status_code == 200
    assert len(await outbox_rows(database_url, "catalog.product.updated")) == 3


async def test_unpublish_and_archive_write_unpublished(client, callers, app, database_url):
    product = await _published(client, callers, app)
    r = await client.post(
        f"/api/v1/products/{product['id']}/status",
        json={"status": "draft", "version": product["version"]},
        headers=callers.merchant_a,
    )
    assert r.status_code == 200 and r.json()["status"] == "draft"

    other = await complete_product(client, callers, sku="PUB-2")
    await force_published(app, other["id"])
    r = await client.delete(
        f"/api/v1/products/{other['id']}?version={other['version']}", headers=callers.admin
    )
    assert r.status_code == 200

    events = await outbox_rows(database_url, "catalog.product.unpublished")
    assert [(e["new_status"], e["reason"]) for e in events] == [
        ("draft", "merchant_unpublished"),
        ("archived", "archived"),
    ]
    assert events[0]["variant_ids"] == [v["id"] for v in product["variants"]]


async def test_changes_to_unpublished_products_emit_nothing(client, callers, database_url):
    product = await complete_product(client, callers)
    r = await client.patch(
        f"/api/v1/products/{product['id']}",
        json={"name": "Draft rename", "version": product["version"]},
        headers=callers.merchant_a,
    )
    assert r.status_code == 200
    assert await outbox_rows(database_url, "catalog.product.updated") == []
