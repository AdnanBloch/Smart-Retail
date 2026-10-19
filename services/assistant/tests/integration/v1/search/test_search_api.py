"""``/api/v1/search/*`` against real Postgres, catalog/inventory faked (§3.1.2.1)."""

import uuid

from sqlalchemy import select, text

from app.v1.search.enums import IndexStatus
from app.v1.search.models import IndexState, QueryLog
from app.v1.search.reindex import REINDEX_LOCK_KEY
from tests.integration.conftest import index, product_payload, seed_catalogue

SEARCH = "/api/v1/search/semantic"


async def search(client, query, headers=None, **body):
    r = await client.post(SEARCH, json={"query": query, **body}, headers=headers or {})
    assert r.status_code == 200, r.text
    return r.json()


async def test_hybrid_search_hydrates_and_logs(app, client, upstreams, callers):
    cat = await seed_catalogue(app, upstreams, callers)
    body = await search(client, "breathable running shoes", callers.customer)
    top = body["items"][0]
    assert body["mode"] == "hybrid" and body["degraded"] is None
    assert top["product_id"] == cat.shoes
    assert top["price"] == "89.00" and top["currency"] == "USD"
    assert top["in_stock"] is True and top["availability"] == "in_stock" and top["hydrated"]
    assert top["matched_chunk"]["content"].startswith("PRODUCT: Acme Runner X1")
    assert "semantic" in top["match_reason"]
    async with app.state.db.sessionmaker() as s:
        log = (await s.scalars(select(QueryLog))).one()
    assert log.query_text == "breathable running shoes" and log.mode == "hybrid"
    assert log.result_count == len(body["items"]) and str(log.user_id) == callers.customer_id
    # The repeated query embedding comes from the cache.
    await search(client, "breathable running shoes")
    async with app.state.db.sessionmaker() as s:
        logs = (await s.scalars(select(QueryLog).order_by(QueryLog.created_at))).all()
    assert [entry.embedding_cached for entry in logs] == [False, True]
    assert logs[1].user_id is None


async def test_filters_are_applied_in_sql(app, client, upstreams, callers):
    cat = await seed_catalogue(app, upstreams, callers)
    ids = lambda b: [i["product_id"] for i in b["items"]]  # noqa: E731
    kitchen = await search(
        client, "shoes blender boots", filters={"category_id": cat.kitchen["id"]}
    )
    assert ids(kitchen) == [cat.blender]
    cheap = await search(client, "shoes boots blender", filters={"max_price": "100"})
    assert set(ids(cheap)) == {cat.shoes, cat.blender}
    pricey = await search(client, "shoes boots blender", filters={"min_price": "100"})
    assert ids(pricey) == [cat.boots]
    merchant = await search(client, "shoes boots blender", filters={"merchant_id": cat.merchant_b})
    assert ids(merchant) == [cat.blender]
    bad = await client.post(
        SEARCH, json={"query": "x", "filters": {"min_price": "5", "max_price": "1"}}
    )
    assert bad.status_code == 422


async def test_modes_limits_and_validation(app, client, upstreams, callers):
    cat = await seed_catalogue(app, upstreams, callers)
    keyword = await search(client, "waterproof", mode="keyword")
    assert [i["product_id"] for i in keyword["items"]] == [cat.boots]
    assert keyword["items"][0]["match_reason"] == "keyword"
    dense = await search(client, "kitchen smoothie blender", mode="dense", limit=1)
    assert len(dense["items"]) == 1 and dense["items"][0]["product_id"] == cat.blender
    assert (await client.post(SEARCH, json={"query": "x" * 501})).status_code == 422
    assert (await client.post(SEARCH, json={"query": "   "})).status_code == 422
    assert (await client.post(SEARCH, json={"query": "x", "mode": "fuzzy"})).status_code == 422
    assert (await search(client, "!!!", mode="keyword"))["items"] == []


async def test_embedding_outage_degrades_to_keyword(app, client, upstreams, callers, embeddings):
    cat = await seed_catalogue(app, upstreams, callers)
    embeddings.down = True
    body = await search(client, "waterproof boots")
    assert body["degraded"] == "keyword_only"
    assert body["items"][0]["product_id"] == cat.boots


async def test_hydration_outage_returns_unhydrated_results(app, client, upstreams, callers):
    cat = await seed_catalogue(app, upstreams, callers)
    upstreams.inventory_down = True
    item = (await search(client, "running shoes"))["items"][0]
    assert item["price"] == "89.00" and item["in_stock"] is None and item["hydrated"] is False
    upstreams.catalog_down = True
    items = (await search(client, "running shoes"))["items"]
    assert items and all(i["price"] is None and not i["hydrated"] for i in items)
    assert items[0]["product_id"] == cat.shoes  # served from local chunks (R7)


async def test_catalog_404_drops_the_result_and_removes_it(app, client, upstreams, callers):
    cat = await seed_catalogue(app, upstreams, callers)
    upstreams.products[cat.boots]["status"] = "archived"  # catalog hides it; event not yet seen
    ids = [i["product_id"] for i in (await search(client, "waterproof hiking boots"))["items"]]
    assert cat.boots not in ids
    async with app.state.db.sessionmaker() as s:
        state = await s.get(IndexState, uuid.UUID(cat.boots))
    assert state.status == IndexStatus.REMOVED and state.chunk_count == 0


async def test_out_of_stock_and_unpublished_never_surface(app, client, upstreams, callers):
    pid = str(uuid.uuid4())
    upstreams.add_product(pid, callers.merchant_a_id, name="Sold Out Sneaker", stock=0)
    await index(app, product_payload(pid, callers.merchant_a_id, name="Sold Out Sneaker"))
    item = (await search(client, "sneaker"))["items"][0]
    assert item["in_stock"] is False and item["availability"] == "out_of_stock"
    draft = str(uuid.uuid4())
    await index(
        app, product_payload(draft, callers.merchant_a_id, name="Secret Sneaker", status="draft")
    )
    for mode in ("hybrid", "dense", "keyword"):
        ids = [
            i["product_id"] for i in (await search(client, "secret sneaker", mode=mode))["items"]
        ]
        assert draft not in ids


async def test_similar_excludes_the_product(app, client, upstreams, callers):
    cat = await seed_catalogue(app, upstreams, callers)
    r = await client.get(f"/api/v1/search/similar/{cat.shoes}?limit=5")
    assert r.status_code == 200
    ids = [i["product_id"] for i in r.json()["items"]]
    assert cat.shoes not in ids and ids[0] == cat.boots
    missing = await client.get(f"/api/v1/search/similar/{uuid.uuid4()}")
    assert missing.status_code == 404
    assert missing.json()["type"].endswith("/product-not-indexed")


async def test_index_admin_endpoints_and_roles(app, client, upstreams, callers, enqueued):
    cat = await seed_catalogue(app, upstreams, callers)
    status = await client.get("/api/v1/search/index/status", headers=callers.admin)
    assert status.status_code == 200
    body = status.json()
    assert body["indexed_products"] == 3 and body["chunk_count"] > 3
    assert body["products_by_status"]["indexed"] == 3 and body["reindex_running"] is False
    for who in (callers.customer, callers.merchant_a):
        assert (await client.get("/api/v1/search/index/status", headers=who)).status_code == 403
    assert (await client.get("/api/v1/search/index/status")).status_code == 401

    r = await client.post(
        "/api/v1/search/index/reindex", json={"scope": "full"}, headers=callers.admin
    )
    assert r.status_code == 202 and r.json()["status_url"] == "/api/v1/search/index/status"
    r = await client.post(
        "/api/v1/search/index/reindex",
        json={"scope": {"product_ids": [cat.shoes]}},
        headers=callers.admin,
    )
    assert r.status_code == 202
    r = await client.post(
        "/api/v1/search/index/reindex",
        json={"scope": {"merchant_id": cat.merchant_b}},
        headers=callers.admin,
    )
    assert r.status_code == 202
    assert [e[0] for e in enqueued] == ["smartretail.assistant.run_reindex"] * 3
    assert enqueued[1][1]["product_ids"] == [cat.shoes]
    assert (
        await client.post("/api/v1/search/index/reindex", headers=callers.merchant_a)
    ).status_code == 403

    own = await client.get(f"/api/v1/search/index/{cat.shoes}", headers=callers.merchant_a)
    assert own.status_code == 200
    detail = own.json()
    assert detail["status"] == "indexed" and len(detail["chunks"]) == detail["chunk_count"]
    assert (
        await client.get(f"/api/v1/search/index/{cat.shoes}", headers=callers.merchant_b)
    ).status_code == 403
    assert (
        await client.get(f"/api/v1/search/index/{cat.shoes}", headers=callers.admin)
    ).status_code == 200
    assert (
        await client.get(f"/api/v1/search/index/{cat.shoes}", headers=callers.customer)
    ).status_code == 403
    assert (
        await client.get(f"/api/v1/search/index/{uuid.uuid4()}", headers=callers.admin)
    ).status_code == 404
    # Service tokens are refused on user routes.
    assert (
        await client.post(SEARCH, json={"query": "x"}, headers=callers.service)
    ).status_code == 401


async def test_full_reindex_conflict(app, client, callers):
    async with app.state.db.engine.connect() as holder:
        await holder.execute(text("SELECT pg_advisory_lock(:k)"), {"k": REINDEX_LOCK_KEY})
        r = await client.post("/api/v1/search/index/reindex", headers=callers.admin)
        assert r.status_code == 409
        status = await client.get("/api/v1/search/index/status", headers=callers.admin)
        assert status.json()["reindex_running"] is True
        await holder.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": REINDEX_LOCK_KEY})


async def test_health(client):
    assert (await client.get("/health")).json() == {"status": "ok"}
    assert (await client.get("/health/ready")).status_code == 200
