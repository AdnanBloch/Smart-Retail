"""``reconcile_inventory_ledger``: I5/I6 hold after real traffic, and a drift
is reported but never corrected (RFC 0006 §7, INV-D23, INV-D32)."""

import asyncio
import logging
import uuid

import pytest
from sqlalchemy import text

from app.core.config import get_settings
from app.v1.stock import reconcile
from app.v1.stock.tasks.maintenance import reconcile_inventory_ledger
from tests.integration.conftest import init_stock, public, reserve


@pytest.fixture
def task_env(monkeypatch, database_url):
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("JWT_PUBLIC_KEY_PATH", "")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def test_an_empty_store_reconciles(app) -> None:
    assert await reconcile.reconcile_once(app.state.db) == []


async def test_real_traffic_reconciles(app, client, callers) -> None:
    [a, b] = await init_stock(client, callers, str(uuid.uuid4()), variants=2, on_hand=10)
    held = await reserve(client, callers, str(uuid.uuid4()), [(a, 3), (b, 1)])
    committed = await reserve(client, callers, str(uuid.uuid4()), [(a, 2)])
    released = await reserve(client, callers, str(uuid.uuid4()), [(b, 4)])
    assert {held.status_code, committed.status_code, released.status_code} == {201}
    [c] = committed.json()["reservations"]
    [r] = released.json()["reservations"]
    base = "/internal/v1/inventory/reservations"
    assert (await client.post(f"{base}/{c['id']}/commit", headers=callers.service)).is_success
    assert (await client.delete(f"{base}/{r['id']}", headers=callers.service)).is_success

    assert await reconcile.reconcile_once(app.state.db) == []


async def test_injected_drift_is_reported_and_not_corrected(app, client, callers, caplog) -> None:
    [variant] = await init_stock(client, callers, str(uuid.uuid4()), on_hand=10)
    assert (await reserve(client, callers, str(uuid.uuid4()), [(variant, 2)])).status_code == 201
    # A write that bypassed the repo: the counter moves, the rows and the ledger do not.
    async with app.state.db.engine.begin() as conn:
        await conn.execute(
            text("UPDATE stock_items SET reserved = reserved + 1 WHERE variant_id = :v"),
            {"v": variant},
        )

    with caplog.at_level(logging.WARNING, logger="inventory.reconcile"):
        [drift] = await reconcile.reconcile_once(app.state.db)

    assert str(drift.variant_id) == variant
    assert (drift.reserved, drift.held, drift.ledger_reserved) == (3, 2, 2)
    assert (drift.on_hand, drift.ledger_on_hand) == (10, 10)
    assert any(rec.message == "inventory ledger drift" for rec in caplog.records)
    # Detects, never corrects: the stored counter is untouched.
    assert (await public(client, variant))["reserved"] == 3
    assert len(await reconcile.reconcile_once(app.state.db)) == 1


async def test_on_hand_drift_against_the_ledger_is_reported(app, client, callers) -> None:
    [variant] = await init_stock(client, callers, str(uuid.uuid4()), on_hand=5)
    async with app.state.db.engine.begin() as conn:
        await conn.execute(
            text("UPDATE stock_items SET on_hand = 7 WHERE variant_id = :v"), {"v": variant}
        )
    [drift] = await reconcile.reconcile_once(app.state.db)
    assert (drift.on_hand, drift.ledger_on_hand, drift.reserved) == (7, 5, 0)


async def test_the_task_returns_the_drift_count(app, client, callers, task_env) -> None:
    [variant] = await init_stock(client, callers, str(uuid.uuid4()), on_hand=4)
    run = lambda: reconcile_inventory_ledger.apply().get()  # noqa: E731
    assert await asyncio.to_thread(run) == 0
    async with app.state.db.engine.begin() as conn:
        await conn.execute(
            text("UPDATE stock_items SET reserved = 1 WHERE variant_id = :v"), {"v": variant}
        )
    assert await asyncio.to_thread(run) == 1
