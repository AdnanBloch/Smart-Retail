"""Checkout edge cases (RFC 0005 §3.1.1.4, §3.1.1.5, ORD-7, ORD-9): line and quantity
limits at their boundaries, malformed bodies that never claim a key, and
failures on either side of the order being persisted."""

import uuid

import asyncpg
import pytest

from app.v1.orders.repo import OrderRepo
from tests.integration.conftest import checkout_body, place


def _pg(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def _scalar(database_url: str, sql: str, *args):
    conn = await asyncpg.connect(_pg(database_url))
    try:
        return await conn.fetchval(sql, *args)
    finally:
        await conn.close()


def _violations(r) -> set[tuple[str, str]]:
    return {(v["field"], v["code"]) for v in r.json()["violations"]}


async def test_line_count_limit_is_inclusive(app, client, callers, catalog) -> None:
    app.state.settings.order_max_lines = 2
    products = [catalog.add(callers.merchant_a_id) for _ in range(3)]
    r = await place(client, callers.customer_a, checkout_body(*[(p, 1) for p in products[:2]]))
    assert r.status_code == 202, r.text
    r = await place(client, callers.customer_a, checkout_body(*[(p, 1) for p in products]))
    assert r.status_code == 422 and ("items", "too_many") in _violations(r)


async def test_quantity_limit_is_inclusive(app, client, callers, catalog) -> None:
    app.state.settings.order_max_quantity_per_line = 5
    product = catalog.add(callers.merchant_a_id)
    assert (await place(client, callers.customer_a, checkout_body((product, 5)))).status_code == 202
    r = await place(client, callers.customer_a, checkout_body((product, 6)))
    assert r.status_code == 422 and _violations(r) == {("items[0].quantity", "too_large")}


async def test_shape_is_checked_before_any_catalog_lookup(app, client, callers, catalog) -> None:
    app.state.settings.order_max_quantity_per_line = 5
    r = await place(
        client, callers.customer_a, checkout_body((catalog.add(callers.merchant_a_id), 6))
    )
    assert r.status_code == 422 and catalog.calls == 0


@pytest.mark.parametrize(
    "mutate",
    [
        lambda b: b.update(items=[]),
        lambda b: b["items"][0].update(quantity=0),
        lambda b: b["items"][0].update(quantity=-1),
        lambda b: b["items"][0].update(product_id="not-a-uuid"),
        lambda b: b.update(payment_method=""),
        lambda b: b["shipping_address"].update(country="usa"),
    ],
    ids=["no-lines", "zero", "negative", "bad-uuid", "no-method", "bad-country"],
)
async def test_malformed_bodies_never_claim_the_key(
    client, callers, catalog, database_url, mutate
) -> None:
    body = checkout_body((catalog.add(callers.merchant_a_id), 1))
    mutate(body)
    r = await place(client, callers.customer_a, body)
    assert r.status_code == 422, r.text
    assert catalog.calls == 0
    assert await _scalar(database_url, "SELECT count(*) FROM idempotency_keys") == 0


async def test_unknown_product_is_rejected_and_the_rejection_replayed(
    client, callers, catalog, database_url
) -> None:
    ghost = {"id": str(uuid.uuid4()), "variants": [{"id": str(uuid.uuid4())}]}
    known = catalog.add(callers.merchant_a_id)
    body = checkout_body((known, 1), (ghost, 1))
    r = await place(client, callers.customer_a, body, key="K-GHOST")
    assert r.status_code == 422
    assert r.headers["content-type"] == "application/problem+json"
    assert _violations(r) == {("items[1].product_id", "unavailable")}
    assert await _scalar(database_url, "SELECT count(*) FROM orders") == 0
    state = await _scalar(database_url, "SELECT state FROM idempotency_keys WHERE key = 'K-GHOST'")
    assert state == "completed"
    # Publishing the product later does not change the stored answer for this key.
    catalog.products[ghost["id"]] = {**known, "id": ghost["id"]}
    again = await place(client, callers.customer_a, body, key="K-GHOST")
    assert again.status_code == 422 and again.headers["idempotency-replayed"] == "true"


async def test_different_body_under_an_in_flight_key_is_422_not_409(
    client, callers, catalog, database_url
) -> None:
    conn = await asyncpg.connect(_pg(database_url))
    try:
        await conn.execute(
            "INSERT INTO idempotency_keys (user_id, key, request_hash, state, expires_at) "
            "VALUES ($1, 'K-FLIGHT', 'another-body', 'in_progress', now() + interval '1 day')",
            uuid.UUID(callers.customer_a_id),
        )
    finally:
        await conn.close()
    body = checkout_body((catalog.add(callers.merchant_a_id), 1))
    r = await place(client, callers.customer_a, body, key="K-FLIGHT")
    assert r.status_code == 422 and r.json()["type"].endswith("/idempotency-key-reused")


async def test_a_failed_persist_releases_the_claim(
    app, client, callers, catalog, database_url, monkeypatch
) -> None:
    body = checkout_body((catalog.add(callers.merchant_a_id), 1))

    async def broken(self, *args, **kwargs):
        raise RuntimeError("disk full")

    with monkeypatch.context() as m:
        m.setattr(OrderRepo, "_persist", broken)
        r = await place(client, callers.customer_a, body, key="K-PERSIST")
    assert r.status_code == 500
    assert await _scalar(database_url, "SELECT count(*) FROM idempotency_keys") == 0
    assert await _scalar(database_url, "SELECT count(*) FROM outbox") == 0
    r = await place(client, callers.customer_a, body, key="K-PERSIST")
    assert r.status_code == 202 and "idempotency-replayed" not in r.headers


async def test_unstarted_workflow_leaves_the_order_pending_and_replays_its_acceptance(
    client, callers, catalog, workflows, database_url
) -> None:
    """ORD-9: the replay returns the stored 202; starting it is the resume task's job."""
    workflows.fail_start = True
    body = checkout_body((catalog.add(callers.merchant_a_id), 1))
    first = await place(client, callers.customer_a, body, key="K-TEMPORAL")
    assert first.status_code == 202
    workflows.fail_start = False
    again = await place(client, callers.customer_a, body, key="K-TEMPORAL")
    assert again.status_code == 202 and again.json() == first.json()
    assert again.headers["idempotency-replayed"] == "true"
    assert workflows.started == []
    row = await _scalar(
        database_url,
        "SELECT status || ':' || coalesce(workflow_run_id, '-') FROM orders WHERE id = $1",
        uuid.UUID(first.json()["order_id"]),
    )
    assert row == "pending:-"
