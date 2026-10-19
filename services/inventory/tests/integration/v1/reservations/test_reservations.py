"""Reserve / commit / release / reverse / expire against real Postgres (RFC 0006 §7)."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
from sqlalchemy import update

from app.v1.reservations import sweeper
from app.v1.reservations.models import Reservation
from tests.integration.conftest import init_stock, public, reserve


def _pg(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def _events(database_url: str, event_type: str) -> list[dict]:
    conn = await asyncpg.connect(_pg(database_url))
    try:
        rows = await conn.fetch(
            "SELECT payload::text AS p FROM outbox WHERE event_type = $1 ORDER BY id", event_type
        )
    finally:
        await conn.close()
    import json

    return [json.loads(r["p"]) for r in rows]


async def _assert_ledger_reconciles(database_url: str) -> None:
    """I5/I6: quantities equal the ledger's sums and the held reservations."""
    conn = await asyncpg.connect(_pg(database_url))
    try:
        drift = await conn.fetch(
            """
            SELECT s.variant_id FROM stock_items s
            LEFT JOIN (SELECT variant_id, sum(delta_on_hand) AS oh, sum(delta_reserved) AS rs
                       FROM stock_movements GROUP BY variant_id) m USING (variant_id)
            LEFT JOIN (SELECT variant_id, sum(quantity) AS held FROM reservations
                       WHERE status = 'held' GROUP BY variant_id) r USING (variant_id)
            WHERE s.on_hand <> coalesce(m.oh, 0) OR s.reserved <> coalesce(m.rs, 0)
               OR s.reserved <> coalesce(r.held, 0)
            """
        )
    finally:
        await conn.close()
    assert drift == []


async def test_reserve_then_replay_then_conflict(client, callers, database_url) -> None:
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=10)
    order = str(uuid.uuid4())

    r = await reserve(client, callers, order, [(v, 2), (v, 1)])  # duplicate lines merge
    assert r.status_code == 201, r.text
    [held] = r.json()["reservations"]
    assert held["quantity"] == 3 and held["status"] == "held"
    assert await public(client, v) == {
        "variant_id": v,
        "on_hand": 10,
        "reserved": 3,
        "available": 7,
        "in_stock": True,
    }

    replay = await reserve(client, callers, order, [(v, 3)])
    assert replay.status_code == 200 and replay.json()["reservations"][0]["id"] == held["id"]
    assert (await public(client, v))["reserved"] == 3  # nothing reserved twice

    conflict = await reserve(client, callers, order, [(v, 4)])
    assert conflict.status_code == 409 and conflict.json()["type"].endswith("/reservation-conflict")

    [event] = await _events(database_url, "inventory.reserved")
    assert event["order_id"] == order and event["available_after"] == 7
    await _assert_ledger_reconciles(database_url)


async def test_replay_is_answered_even_when_stock_is_now_exhausted(client, callers) -> None:
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=2)
    order = str(uuid.uuid4())
    assert (await reserve(client, callers, order, [(v, 2)])).status_code == 201
    # A retried activity must replay, not be fast-rejected against its own holding.
    assert (await reserve(client, callers, order, [(v, 2)])).status_code == 200


async def test_insufficient_stock_is_all_or_nothing(client, callers, database_url) -> None:
    v1, v2 = await init_stock(client, callers, callers.merchant_a_id, variants=2, on_hand=3)
    r = await reserve(client, callers, str(uuid.uuid4()), [(v1, 1), (v2, 5)])
    assert r.status_code == 409 and r.json()["type"].endswith("/insufficient-stock")
    [violation] = r.json()["violations"]
    assert violation["variant_id"] == v2
    assert (violation["requested"], violation["available"]) == (5, 3)
    # Nothing was reserved on the line that would have fitted.
    assert (await public(client, v1))["reserved"] == 0
    [refused] = await _events(database_url, "inventory.stockout")
    assert refused["trigger"] == "reservation_refused" and refused["requested"] == 5


async def test_shortage_found_only_under_the_lock_is_refused(client, callers, app, monkeypatch):
    """The unlocked fast path saw enough; the locked re-check is what decides."""
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=1)
    from app.v1.reservations.repo import ReservationRepo

    async def no_fast_reject(self, lines, *, lock):
        return []

    monkeypatch.setattr(ReservationRepo, "_shortages", no_fast_reject)
    r = await reserve(client, callers, str(uuid.uuid4()), [(v, 2)])
    assert r.status_code == 409 and r.json()["type"].endswith("/insufficient-stock")


async def test_unknown_variant(client, callers) -> None:
    r = await reserve(client, callers, str(uuid.uuid4()), [(str(uuid.uuid4()), 1)])
    assert r.status_code == 422 and r.json()["type"].endswith("/unknown-variant")


async def test_depleted_and_low_stock_signals(client, callers, database_url) -> None:
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=7)
    await reserve(client, callers, str(uuid.uuid4()), [(v, 3)])  # 7 → 4: crosses threshold 5
    await reserve(client, callers, str(uuid.uuid4()), [(v, 1)])  # 4 → 3: no new edge
    await reserve(client, callers, str(uuid.uuid4()), [(v, 3)])  # 3 → 0: depleted
    triggers = [e["trigger"] for e in await _events(database_url, "inventory.stockout")]
    assert triggers == ["low_stock", "depleted"]


async def test_commit_release_reverse_paths(client, callers, database_url) -> None:
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=10)
    base = "/internal/v1/inventory/reservations"
    [a] = (await reserve(client, callers, str(uuid.uuid4()), [(v, 4)])).json()["reservations"]
    [b] = (await reserve(client, callers, str(uuid.uuid4()), [(v, 2)])).json()["reservations"]

    # commit a: on_hand and reserved both drop; repeating is a no-op.
    for _ in range(2):
        r = await client.post(f"{base}/{a['id']}/commit", headers=callers.service)
        assert r.status_code == 200 and r.json()["status"] == "committed"
    assert await public(client, v) == {
        "variant_id": v,
        "on_hand": 6,
        "reserved": 2,
        "available": 4,
        "in_stock": True,
    }
    # A committed reservation is never released.
    r = await client.delete(f"{base}/{a['id']}", headers=callers.service)
    assert r.status_code == 409 and r.json()["type"].endswith("/reservation-committed")

    # release b twice: the second is a no-op; then commit is refused.
    for _ in range(2):
        r = await client.delete(f"{base}/{b['id']}", headers=callers.service)
        assert r.status_code == 200 and r.json()["status"] == "released"
    r = await client.post(f"{base}/{b['id']}/commit", headers=callers.service)
    assert r.status_code == 409 and r.json()["type"].endswith("/reservation-not-held")
    r = await client.post(f"{base}/{b['id']}/reverse", headers=callers.service)
    assert r.status_code == 409 and r.json()["type"].endswith("/reservation-not-committed")

    # reverse a twice: stock returns once, status stays committed (I8).
    for _ in range(2):
        r = await client.post(f"{base}/{a['id']}/reverse", headers=callers.service)
        assert r.status_code == 200 and r.json()["status"] == "committed"
    assert (await public(client, v))["on_hand"] == 10

    [released] = await _events(database_url, "inventory.released")
    assert released["reason"] == "compensation"
    unknown = await client.post(f"{base}/{uuid.uuid4()}/commit", headers=callers.service)
    assert unknown.status_code == 404
    await _assert_ledger_reconciles(database_url)


async def test_fifty_parallel_reserves_on_ten_units_yield_ten(client, callers, database_url):
    """RFC 0006 §7: oversell is impossible under concurrency."""
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=10)
    results = await asyncio.gather(
        *(reserve(client, callers, str(uuid.uuid4()), [(v, 1)]) for _ in range(50))
    )
    statuses = sorted(r.status_code for r in results)
    assert statuses.count(201) == 10, statuses
    assert statuses.count(409) == 40, statuses
    assert await public(client, v) == {
        "variant_id": v,
        "on_hand": 10,
        "reserved": 10,
        "available": 0,
        "in_stock": False,
    }
    await _assert_ledger_reconciles(database_url)


async def test_opposite_line_orders_do_not_deadlock(client, callers, database_url) -> None:
    v1, v2 = await init_stock(client, callers, callers.merchant_a_id, variants=2, on_hand=100)
    calls = []
    for i in range(20):
        lines = [(v1, 1), (v2, 1)] if i % 2 else [(v2, 1), (v1, 1)]
        calls.append(reserve(client, callers, str(uuid.uuid4()), lines))
    results = await asyncio.gather(*calls)
    assert {r.status_code for r in results} == {201}
    assert (await public(client, v1))["reserved"] == 20
    await _assert_ledger_reconciles(database_url)


async def test_sweeper_expires_due_reservations(client, callers, app, settings, database_url):
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=5)
    [due] = (await reserve(client, callers, str(uuid.uuid4()), [(v, 2)])).json()["reservations"]
    [fresh] = (await reserve(client, callers, str(uuid.uuid4()), [(v, 1)])).json()["reservations"]
    async with app.state.db.sessionmaker() as s:
        await s.execute(
            update(Reservation)
            .where(Reservation.id == uuid.UUID(due["id"]))
            .values(expires_at=datetime.now(UTC) - timedelta(minutes=1))
        )
        await s.commit()

    settings.inventory_sweep_batch_size = 1  # exercise the batch loop
    assert await sweeper.sweep_once(app.state.db, settings) == 1
    assert await sweeper.sweep_once(app.state.db, settings) == 0  # safe to run twice
    assert (await public(client, v))["reserved"] == 1
    [expired] = await _events(database_url, "inventory.released")
    assert expired["reason"] == "expired" and expired["reservation_id"] == due["id"]
    # The workflow can no longer commit it; releasing it is a harmless no-op.
    base = "/internal/v1/inventory/reservations"
    r = await client.post(f"{base}/{due['id']}/commit", headers=callers.service)
    assert r.status_code == 409
    assert (await client.delete(f"{base}/{due['id']}", headers=callers.service)).status_code == 200
    assert fresh["status"] == "held"
    await _assert_ledger_reconciles(database_url)


async def test_in_process_sweeper_runs_with_the_app(settings, keypair, monkeypatch) -> None:
    from main import create_application
    from smartretail_common.auth import JwtVerifier

    ticks = asyncio.Event()

    async def fake_sweep(db, s):
        ticks.set()
        raise RuntimeError("a failing sweep is logged and retried")

    monkeypatch.setattr(sweeper, "sweep_once", fake_sweep)
    settings.inventory_sweeper_inprocess = True
    settings.inventory_sweeper_interval_seconds = 0.01
    verifier = JwtVerifier(
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        static_keys={None: keypair.public_key},
    )
    application = create_application(settings, verifier=verifier)
    async with application.router.lifespan_context(application):
        await asyncio.wait_for(ticks.wait(), 2)


@pytest.mark.parametrize("who", ["admin", "merchant_a", "customer", "wrong_scope"])
async def test_internal_routes_need_the_service_scope(client, callers, who) -> None:
    body = {
        "order_id": str(uuid.uuid4()),
        "lines": [{"variant_id": str(uuid.uuid4()), "quantity": 1}],
    }
    r = await client.post(
        "/internal/v1/inventory/reservations", json=body, headers=getattr(callers, who)
    )
    assert r.status_code == 403
    assert (await client.post("/internal/v1/inventory/reservations", json=body)).status_code == 401
