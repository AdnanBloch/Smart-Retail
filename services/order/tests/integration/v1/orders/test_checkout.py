"""Idempotent checkout (RFC 0005 §3.1.1.4, B7, ORD-7, ORD-8; G4)."""

import asyncio
import uuid
from datetime import timedelta

import asyncpg
import pytest

from tests.integration.conftest import checkout_body, outbox_rows, place


def _pg(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def _count(database_url: str, sql: str, *args) -> int:
    conn = await asyncpg.connect(_pg(database_url))
    try:
        return await conn.fetchval(sql, *args)
    finally:
        await conn.close()


async def test_checkout_is_accepted_with_202(client, callers, catalog, workflows, database_url):
    a = catalog.add(callers.merchant_a_id, price="10.00")
    b = catalog.add(callers.merchant_b_id, price="99.99", override="7.25")
    headers = callers.customer_a
    r = await place(client, headers, checkout_body((a, 3), (b, 2)), **{"X-Client-Id": "web-1"})
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["status"] == "pending" and body["order_number"].startswith("SR-")
    assert r.headers["location"] == body["_links"]["self"] == f"/api/v1/orders/{body['order_id']}"
    assert "idempotency-replayed" not in r.headers
    # The user's own token went to catalog (A13), once per distinct product.
    assert catalog.calls == 2 and set(catalog.tokens) == {headers["Authorization"]}
    [started] = workflows.started
    assert started == {
        "order_id": body["order_id"],
        "workflow_id": f"order-{body['order_id']}",
        "method": "tok_ok",
    }

    order = (await client.get(body["_links"]["self"], headers=callers.customer_a)).json()
    # Amounts come from the snapshot: the override wins over the base price (ORD-4).
    lines = sorted((i["unit_price"], i["quantity"], i["line_total"]) for i in order["items"])
    assert lines == [("10.00", 3, "30.00"), ("7.25", 2, "14.50")]
    assert (order["subtotal"], order["shipping_fee"], order["total"]) == ("44.50", "0.00", "44.50")
    assert order["payment"] == {"status": "none", "reference": None}
    assert order["timeline"] == [
        {
            "from": None,
            "to": "pending",
            "reason": None,
            "actor": f"customer:{callers.customer_a_id}",
            "at": order["timeline"][0]["at"],
        }
    ]
    [created] = await outbox_rows(database_url, "order.created")
    assert created["order_id"] == body["order_id"] and created["item_count"] == 5
    assert created["client_id"] == "web-1" and created["total"] == "44.50"
    assert (
        await _count(database_url, "SELECT count(*) FROM orders WHERE workflow_run_id = 'run-1'")
        == 1
    )


async def test_same_key_replays_verbatim(client, callers, catalog, workflows) -> None:
    product = catalog.add(callers.merchant_a_id)
    body = checkout_body((product, 1))
    first = await place(client, callers.customer_a, body, key="KEY-1")
    again = await place(client, callers.customer_a, body, key="  KEY-1 ")  # canonicalised
    assert again.status_code == 202 and again.json() == first.json()
    assert again.headers["idempotency-replayed"] == "true"
    assert again.headers["location"] == first.headers["location"]
    assert len(workflows.started) == 1

    # Same key, different body: a client bug. The existing order is untouched.
    other = checkout_body((product, 2))
    r = await place(client, callers.customer_a, other, key="KEY-1")
    assert r.status_code == 422 and r.json()["type"].endswith("/idempotency-key-reused")

    # Keys are scoped per user: another customer's KEY-1 is a different key.
    r = await place(client, callers.customer_b, body, key="KEY-1")
    assert r.status_code == 202 and r.json()["order_id"] != first.json()["order_id"]


async def test_ten_parallel_requests_with_one_key_make_one_order(
    client, callers, catalog, database_url
) -> None:
    """G4: the database arbitrates; never a second order."""
    product = catalog.add(callers.merchant_a_id)
    body = checkout_body((product, 1))
    results = await asyncio.gather(
        *(place(client, callers.customer_a, body, key="burst") for _ in range(10))
    )
    statuses = [r.status_code for r in results]
    assert set(statuses) <= {202, 409}, statuses
    ids = {r.json()["order_id"] for r in results if r.status_code == 202}
    assert len(ids) == 1
    for r in results:
        if r.status_code == 409:
            assert r.headers["retry-after"] == "1"
    assert await _count(database_url, "SELECT count(*) FROM orders") == 1


async def test_business_rejections_are_completed_and_replayed(client, callers, catalog) -> None:
    draft = catalog.add(callers.merchant_a_id, status="draft")
    body = checkout_body((draft, 1))
    first = await place(client, callers.customer_a, body, key="K-REJECT")
    assert first.status_code == 422, first.text
    assert first.json()["violations"][0]["field"] == "items[0].product_id"
    calls = catalog.calls
    again = await place(client, callers.customer_a, body, key="K-REJECT")
    assert again.status_code == 422 and again.json() == first.json()
    assert again.headers["idempotency-replayed"] == "true"
    assert catalog.calls == calls  # replayed without a second lookup


@pytest.mark.parametrize(
    ("setup", "field", "code"),
    [
        (lambda c, m: [(c.add(m, active=False), 1)], "items[0].variant_id", "inactive"),
        (
            lambda c, m: [(c.add(m, currency="USD"), 1), (c.add(m, currency="EUR"), 1)],
            "items",
            "currency",
        ),
        (lambda c, m: [(c.add(m), 1000)], "items[0].quantity", "too_large"),
    ],
)
async def test_snapshot_rules(client, callers, catalog, setup, field, code) -> None:
    body = checkout_body(*setup(catalog, callers.merchant_a_id))
    r = await place(client, callers.customer_a, body)
    assert r.status_code == 422, r.text
    assert {(v["field"], v["code"]) for v in r.json()["violations"]} >= {(field, code)}


async def test_duplicate_variant_and_unknown_variant(client, callers, catalog) -> None:
    product = catalog.add(callers.merchant_a_id)
    body = checkout_body((product, 1), (product, 2))
    r = await place(client, callers.customer_a, body)
    assert r.status_code == 422 and r.json()["violations"][0]["code"] == "duplicate"
    body = checkout_body((product, 1))
    body["items"][0]["variant_id"] = str(uuid.uuid4())
    r = await place(client, callers.customer_a, body)
    assert r.status_code == 422 and r.json()["violations"][0]["code"] == "unknown"


async def test_catalog_down_releases_the_claim(client, callers, catalog, database_url) -> None:
    product = catalog.add(callers.merchant_a_id)
    body = checkout_body((product, 1))
    catalog.down = True
    r = await place(client, callers.customer_a, body, key="K-DOWN")
    assert r.status_code == 503 and r.json()["type"].endswith("/catalog-unavailable")
    assert "retry-after" in r.headers
    assert await _count(database_url, "SELECT count(*) FROM idempotency_keys") == 0
    catalog.down = False
    r = await place(client, callers.customer_a, body, key="K-DOWN")
    assert r.status_code == 202  # the retry proceeds


async def test_in_flight_stale_and_expired_claims(client, callers, catalog, database_url) -> None:
    from app.v1.orders.schemas import CheckoutIn
    from smartretail_common.idempotency import request_hash

    product = catalog.add(callers.merchant_a_id)
    body = checkout_body((product, 1))
    # The route hashes the validated body, defaults included.
    digest = request_hash(CheckoutIn.model_validate(body).model_dump(mode="json"))
    conn = await asyncpg.connect(_pg(database_url))
    insert = (
        "INSERT INTO idempotency_keys (user_id, key, request_hash, state, created_at, expires_at) "
        "VALUES ($1, $2, $3, 'in_progress', now() - $4::interval, now() + $5::interval)"
    )
    user = uuid.UUID(callers.customer_a_id)
    try:
        day = timedelta(days=1)
        await conn.execute(insert, user, "fresh", digest, timedelta(0), day)
        await conn.execute(insert, user, "stale", digest, timedelta(minutes=5), day)
        await conn.execute(insert, user, "expired", "other-body", 2 * day, -timedelta(hours=1))
    finally:
        await conn.close()

    r = await place(client, callers.customer_a, body, key="fresh")
    assert r.status_code == 409 and r.json()["type"].endswith("/request-in-progress")
    # ORD-8: a claim older than the 30 s lease belonged to a request that died.
    assert (await place(client, callers.customer_a, body, key="stale")).status_code == 202
    # An expired row is treated as absent, whatever body it held.
    assert (await place(client, callers.customer_a, body, key="expired")).status_code == 202


async def test_temporal_down_still_accepts(client, callers, catalog, workflows, database_url):
    """ORD-9: acceptance does not depend on the orchestrator; the run id stays null."""
    workflows.fail_start = True
    product = catalog.add(callers.merchant_a_id)
    r = await place(client, callers.customer_a, checkout_body((product, 1)))
    assert r.status_code == 202
    assert (
        await _count(database_url, "SELECT count(*) FROM orders WHERE workflow_run_id IS NULL") == 1
    )


async def test_checkout_roles_and_headers(client, callers, catalog) -> None:
    body = checkout_body((catalog.add(callers.merchant_a_id), 1))
    for who in ("merchant_a", "admin"):
        r = await place(client, getattr(callers, who), body)
        assert r.status_code == 403, who
    r = await place(client, callers.service, body)
    assert r.status_code == 401  # service tokens never reach user routes
    r = await client.post("/api/v1/orders", json=body, headers=callers.customer_a)
    assert r.status_code == 400 and r.json()["type"].endswith("/idempotency-key-required")
    r = await place(client, callers.customer_a, {**body, "unit_price": "0.01"})
    assert r.status_code == 422  # no price field is accepted (I9)
