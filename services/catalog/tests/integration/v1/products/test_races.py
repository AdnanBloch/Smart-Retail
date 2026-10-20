"""Races on one product (mentor review 2026-10-20 §8, CAT-7, CAT-12).

Every write locks the product row (``SELECT … FOR UPDATE``) and checks the
``version`` the caller sent, so of N concurrent writes at one version exactly
one wins and the rest are told the current version. SKU uniqueness is the
database's (``uq_products_merchant_id_sku``), so N concurrent creates of one
SKU leave one row. Each test runs several rounds so the interleavings vary.
"""

import asyncio
import uuid

from sqlalchemy import func, select

from app.models import Product, ProductAuditLog
from tests.integration.conftest import complete_product, create_product, publish

ROUNDS = 5
WRITERS = 4


async def _count(app, model, *where) -> int:
    async with app.state.db.sessionmaker() as s:
        return await s.scalar(select(func.count()).select_from(model).where(*where)) or 0


async def test_concurrent_creates_of_one_sku_exactly_one_wins(app, client, callers) -> None:
    for i in range(ROUNDS):
        sku = f"RACE-{i}"
        responses = await asyncio.gather(
            *(
                client.post(
                    "/api/v1/products",
                    json={"sku": sku, "name": f"n{w}", "base_price": "1"},
                    headers=callers.merchant_a,
                )
                for w in range(WRITERS)
            )
        )
        codes = sorted(r.status_code for r in responses)
        assert codes == [201] + [409] * (WRITERS - 1), [r.text for r in responses]
        assert all(
            r.json()["type"].endswith("/duplicate-sku") for r in responses if r.status_code == 409
        )
        assert await _count(app, Product, Product.sku == sku) == 1
    # Losers left no audit rows behind: one "created" per surviving product.
    assert await _count(app, ProductAuditLog, ProductAuditLog.action == "created") == ROUNDS


async def test_concurrent_updates_at_one_version_one_wins_rest_409(app, client, callers) -> None:
    for i in range(ROUNDS):
        product = await create_product(client, callers.merchant_a, sku=f"U-{i}")
        url = f"/api/v1/products/{product['id']}"
        responses = await asyncio.gather(
            *(
                client.patch(
                    url, json={"name": f"writer-{w}", "version": 1}, headers=callers.merchant_a
                )
                for w in range(WRITERS)
            )
        )
        winners = [r for r in responses if r.status_code == 200]
        losers = [r for r in responses if r.status_code != 200]
        assert len(winners) == 1, [r.text for r in responses]
        for r in losers:
            assert r.status_code == 409 and r.json()["type"].endswith("/version-conflict")
            assert r.json()["current_version"] == 2
        final = (await client.get(url, headers=callers.merchant_a)).json()
        # The stored name is the winner's, never a loser's (no lost update).
        assert final["version"] == 2 and final["name"] == winners[0].json()["name"]
        assert (
            await _count(
                app,
                ProductAuditLog,
                ProductAuditLog.product_id == uuid.UUID(product["id"]),
                ProductAuditLog.action == "updated",
            )
            == 1
        )


async def test_concurrent_variant_adds_at_one_version_one_wins(client, callers) -> None:
    for i in range(ROUNDS):
        product = await create_product(client, callers.merchant_a, sku=f"V-{i}")
        responses = await asyncio.gather(
            *(
                client.post(
                    f"/api/v1/products/{product['id']}/variants",
                    json={"sku": f"V-{i}-{w}", "options": {"w": w}, "version": 1},
                    headers=callers.merchant_a,
                )
                for w in range(WRITERS)
            )
        )
        assert sorted(r.status_code for r in responses) == [201] + [409] * (WRITERS - 1)
        detail = (
            await client.get(f"/api/v1/products/{product['id']}", headers=callers.merchant_a)
        ).json()
        assert len(detail["variants"]) == 1 and detail["version"] == 2


async def test_concurrent_publishes_start_exactly_one_workflow(client, callers, starter) -> None:
    for i in range(ROUNDS):
        product = await complete_product(client, callers, sku=f"P-{i}")
        responses = await asyncio.gather(
            *(publish(client, callers.merchant_a, product) for _ in range(WRITERS))
        )
        accepted = [r for r in responses if r.status_code == 202]
        assert len(accepted) == 1, [r.text for r in responses]
        for r in responses:
            if r.status_code != 202:
                assert r.status_code == 409 and r.json()["type"].endswith("/version-conflict")
        started = [s for s in starter.started if s["product_id"] == product["id"]]
        assert [s["workflow_id"] for s in started] == [accepted[0].json()["workflow_id"]]


async def test_concurrent_republish_at_the_new_version_is_already_running(
    client, callers, starter
) -> None:
    """Callers who retry with the accepted version still cannot start a second workflow."""
    product = await complete_product(client, callers)
    first = (await publish(client, callers.merchant_a, product)).json()
    retry = {**product, "version": first["version"]}
    responses = await asyncio.gather(
        *(publish(client, callers.merchant_a, retry) for _ in range(WRITERS))
    )
    for r in responses:
        assert r.status_code == 409 and r.json()["type"].endswith("/publish-already-running")
        assert r.json()["workflow_id"] == first["workflow_id"]
    assert len(starter.started) == 1


async def test_edit_racing_publish_exactly_one_applies(client, callers, starter) -> None:
    outcomes = set()
    for i in range(ROUNDS * 2):
        product = await complete_product(client, callers, sku=f"E-{i}")
        url = f"/api/v1/products/{product['id']}"
        edit, pub = await asyncio.gather(
            client.patch(
                url,
                json={"name": "Edited", "version": product["version"]},
                headers=callers.merchant_a,
            ),
            publish(client, callers.merchant_a, product),
        )
        final = (await client.get(url, headers=callers.admin)).json()
        started = [s for s in starter.started if s["product_id"] == product["id"]]
        if pub.status_code == 202:
            # Publish locked first: the edit sees a publishing product.
            assert edit.status_code == 409 and edit.json()["type"].endswith(
                "/product-publishing"
            ), edit.text
            assert final["status"] == "publishing" and final["name"] == "Trail Shoe"
            assert len(started) == 1
            outcomes.add("publish")
        else:
            # Edit locked first: the publish carried a stale version.
            assert edit.status_code == 200, edit.text
            assert pub.status_code == 409 and pub.json()["type"].endswith("/version-conflict")
            assert final["status"] == "draft" and final["name"] == "Edited"
            assert started == []
            outcomes.add("edit")
    assert outcomes  # at least one branch observed; both are valid serialisations


async def test_archive_racing_update_never_edits_an_archived_product(client, callers) -> None:
    for i in range(ROUNDS):
        product = await create_product(client, callers.merchant_a, sku=f"A-{i}")
        url = f"/api/v1/products/{product['id']}"
        archive, edit = await asyncio.gather(
            client.delete(f"{url}?version=1", headers=callers.merchant_a),
            client.patch(url, json={"name": "late", "version": 1}, headers=callers.merchant_a),
        )
        assert sorted([archive.status_code, edit.status_code]) == [200, 409]
        final = (await client.get(url, headers=callers.merchant_a)).json()
        assert final["version"] == 2
        if archive.status_code == 200:
            assert final["status"] == "archived" and final["name"] == "Trail Shoe"
        else:
            assert final["status"] == "draft" and final["name"] == "late"
