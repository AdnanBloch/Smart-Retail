"""Stock reads, merchant set/adjust, ledger reads and initialisation (RFC 0006 §3.1.2)."""

import os
import subprocess
import sys
import uuid

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError

from tests.integration.conftest import SERVICE_DIR, init_stock, public, reserve


async def test_public_read_and_missing_row(client, callers) -> None:
    [v] = await init_stock(client, callers, callers.merchant_a_id)
    assert (await public(client, v))["in_stock"] is False
    r = await client.get(f"/api/v1/inventory/{uuid.uuid4()}")
    assert r.status_code == 404 and r.json()["type"].endswith("/stock-item-not-found")


async def test_initialisation_is_idempotent_and_never_resets(client, callers) -> None:
    product, variant = str(uuid.uuid4()), str(uuid.uuid4())
    body = {
        "product_id": product,
        "merchant_id": callers.merchant_a_id,
        "variants": [{"variant_id": variant, "sku": "S"}],
    }
    url = "/internal/v1/inventory/stock-items"
    assert (await client.post(url, json=body, headers=callers.service)).status_code == 200
    await client.put(f"/api/v1/inventory/{variant}", json={"on_hand": 9}, headers=callers.admin)
    r = await client.post(url, json=body, headers=callers.service)
    assert r.status_code == 200 and r.json()["items"][0]["on_hand"] == 9
    assert (await client.post(url, json=body, headers=callers.admin)).status_code == 403
    r = await client.post(url, json={**body, "variants": []}, headers=callers.service)
    assert r.json() == {"items": []}


async def test_list_is_scoped_by_the_token(client, callers) -> None:
    a1, a2 = await init_stock(client, callers, callers.merchant_a_id, variants=2, on_hand=10)
    [b1] = await init_stock(client, callers, callers.merchant_b_id, on_hand=0)
    await reserve(client, callers, str(uuid.uuid4()), [(a1, 6)])  # a1: available 4 <= 5

    def ids(r) -> set[str]:
        assert r.status_code == 200, r.text
        return {i["variant_id"] for i in r.json()["items"]}

    # A merchant's merchant_id filter is ignored; the token decides.
    url = f"/api/v1/inventory?merchant_id={callers.merchant_b_id}"
    assert ids(await client.get(url, headers=callers.merchant_a)) == {a1, a2}
    assert ids(await client.get(url, headers=callers.admin)) == {b1}
    assert len(ids(await client.get("/api/v1/inventory", headers=callers.admin))) == 3
    low = "/api/v1/inventory?low_stock=true"
    assert ids(await client.get(low, headers=callers.merchant_a)) == {a1}
    empty = "/api/v1/inventory?out_of_stock=true"
    assert ids(await client.get(empty, headers=callers.admin)) == {b1}
    product = (await client.get("/api/v1/inventory", headers=callers.admin)).json()["items"][0]
    by_product = f"/api/v1/inventory?product_id={product['product_id']}"
    assert product["variant_id"] in ids(await client.get(by_product, headers=callers.admin))
    assert (await client.get("/api/v1/inventory", headers=callers.customer)).status_code == 403
    assert (await client.get("/api/v1/inventory")).status_code == 401


async def test_set_ownership_if_match_and_reserved_floor(client, callers) -> None:
    [v] = await init_stock(client, callers, callers.merchant_a_id)
    url = f"/api/v1/inventory/{v}"

    r = await client.put(url, json={"on_hand": 5}, headers=callers.merchant_b)
    assert r.status_code == 403
    r = await client.put(url, json={"on_hand": 5}, headers=callers.merchant_a)
    assert r.status_code == 200 and (r.json()["on_hand"], r.json()["version"]) == (5, 2)

    stale = {**callers.merchant_a, "If-Match": "1"}
    r = await client.put(url, json={"on_hand": 6}, headers=stale)
    assert r.status_code == 409 and r.json()["type"].endswith("/version-mismatch")
    fresh = {**callers.merchant_a, "If-Match": "2"}
    r = await client.put(url, json={"on_hand": 5, "low_stock_threshold": 1}, headers=fresh)
    assert r.status_code == 200 and r.json()["low_stock_threshold"] == 1
    assert r.json()["version"] == 3
    # Same values again: nothing changes, no version bump.
    r = await client.put(url, json={"on_hand": 5, "low_stock_threshold": 1}, headers=callers.admin)
    assert r.json()["version"] == 3

    await reserve(client, callers, str(uuid.uuid4()), [(v, 4)])
    r = await client.put(url, json={"on_hand": 3}, headers=callers.admin)
    assert r.status_code == 409 and r.json()["type"].endswith("/stock-below-reserved")
    missing = await client.put(
        f"/api/v1/inventory/{uuid.uuid4()}", json={"on_hand": 1}, headers=callers.admin
    )
    assert missing.status_code == 404


async def test_adjust_with_idempotency_key_and_movements(client, callers) -> None:
    [v] = await init_stock(client, callers, callers.merchant_a_id)
    url = f"/api/v1/inventory/{v}/adjust"
    keyed = {**callers.merchant_a, "Idempotency-Key": "restock-001"}
    for _ in range(2):
        r = await client.post(url, json={"delta": 7, "reason": "restock"}, headers=keyed)
        assert r.status_code == 200 and r.json()["on_hand"] == 7  # replay: applied once

    order = str(uuid.uuid4())
    r = await client.post(
        url, json={"delta": 1, "reason": "return", "reference_id": order}, headers=callers.admin
    )
    assert r.json()["on_hand"] == 8
    r = await client.post(
        url,
        json={"delta": -2, "reason": "manual_adjustment", "reference_id": order},
        headers=callers.admin,
    )
    assert r.json()["on_hand"] == 6
    r = await client.post(
        url, json={"delta": -1, "reason": "manual_adjustment"}, headers=callers.admin
    )
    assert r.json()["on_hand"] == 5
    await reserve(client, callers, str(uuid.uuid4()), [(v, 5)])
    r = await client.post(
        url, json={"delta": -1, "reason": "manual_adjustment"}, headers=callers.admin
    )
    assert r.status_code == 409 and r.json()["type"].endswith("/stock-below-reserved")
    r = await client.post(url, json={"delta": 0, "reason": "restock"}, headers=callers.admin)
    assert r.status_code == 422
    r = await client.post(url, json={"delta": 1, "reason": "restock"}, headers=callers.merchant_b)
    assert r.status_code == 403

    moves = await client.get(f"/api/v1/inventory/{v}/movements", headers=callers.merchant_a)
    assert moves.status_code == 200
    reasons = [m["reason"] for m in moves.json()["items"]]
    assert reasons == [
        "reserve",
        "manual_adjustment",
        "manual_adjustment",
        "return",
        "restock",
        "initial",
    ]
    returned = next(m for m in moves.json()["items"] if m["reason"] == "return")
    assert (returned["reference_type"], returned["reference_id"]) == ("order", order)
    only = await client.get(
        f"/api/v1/inventory/{v}/movements?reason=restock&from=2000-01-01T00:00:00Z"
        "&to=2999-01-01T00:00:00Z",
        headers=callers.admin,
    )
    assert [m["reason"] for m in only.json()["items"]] == ["restock"]
    denied = await client.get(f"/api/v1/inventory/{v}/movements", headers=callers.merchant_b)
    assert denied.status_code == 403


async def test_contention_is_retried_then_reported(client, callers, app, monkeypatch) -> None:
    """INV-1: lock timeouts retry the whole transaction, then become a retryable 503."""
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=5)
    from app.v1.stock import locking

    class LockNotAvailable(Exception):
        sqlstate = "55P03"

    calls = 0
    original = locking.set_timeouts

    async def busy(session, settings):
        nonlocal calls
        calls += 1
        await original(session, settings)
        raise DBAPIError("SELECT ... FOR UPDATE", None, LockNotAvailable())

    monkeypatch.setattr(locking, "set_timeouts", busy)
    r = await reserve(client, callers, str(uuid.uuid4()), [(v, 1)])
    assert r.status_code == 503 and r.json()["type"].endswith("/inventory-contended")
    assert r.headers["retry-after"] == "1"
    assert calls == app.state.settings.inventory_lock_retry_attempts


async def test_check_violation_maps_to_409_and_other_errors_propagate(client, callers, monkeypatch):
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=5)
    from app.v1.stock import locking

    class Fault(Exception):
        def __init__(self, code: str) -> None:
            self.sqlstate = code

    code = "23514"

    async def failing(session, settings):
        raise DBAPIError("UPDATE", None, Fault(code))

    monkeypatch.setattr(locking, "set_timeouts", failing)
    r = await reserve(client, callers, str(uuid.uuid4()), [(v, 1)])
    assert r.status_code == 409 and r.json()["type"].endswith("/stock-constraint-violated")
    code = "XX000"
    r = await reserve(client, callers, str(uuid.uuid4()), [(v, 1)])
    assert r.status_code == 500


async def test_health(client) -> None:
    assert (await client.get("/health/ready")).json()["status"] == "ready"


def _pg(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def test_schema_conventions(database_url) -> None:
    conn = await asyncpg.connect(_pg(database_url))
    try:
        assert await conn.fetchval("SELECT count(*) FROM pg_type WHERE typtype = 'e'") == 0
        generated = await conn.fetchval(
            "SELECT generation_expression FROM information_schema.columns "
            "WHERE table_name = 'stock_items' AND column_name = 'available'"
        )
        assert generated == "(on_hand - reserved)"
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO stock_items (variant_id, product_id, merchant_id, sku, on_hand, "
                "reserved) VALUES ($1, $1, $1, 'x', 1, 2)",
                uuid.uuid4(),
            )
    finally:
        await conn.close()


def test_head_base_head_and_no_pending_diff(database_url) -> None:
    for args in (("downgrade", "base"), ("upgrade", "head"), ("check",)):
        result = subprocess.run(  # noqa: S603
            [sys.executable, "-m", "alembic", *args],
            cwd=SERVICE_DIR,
            env={**os.environ, "DATABASE_URL": database_url},
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
