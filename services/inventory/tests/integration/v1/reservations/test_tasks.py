"""The Celery tasks, run the way a worker runs them: synchronously, each on its
own event loop (here, in a thread, as a prefork child would)."""

import asyncio
import uuid

import pytest
from sqlalchemy import text

from app.core.celery_app import app as celery_app
from app.core.config import get_settings
from app.core.tasks import sweep_processed_events
from app.v1.reservations.tasks.maintenance import sweep_expired_reservations
from tests.integration.conftest import init_stock, public, reserve


@pytest.fixture
def task_env(monkeypatch, database_url):
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("JWT_PUBLIC_KEY_PATH", "")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_tasks_are_routed_to_the_inventory_maintenance_queue() -> None:
    conf = celery_app.conf
    assert conf.task_default_queue == "inventory.maintenance"
    assert conf.task_acks_late and conf.task_reject_on_worker_lost
    assert conf.worker_prefetch_multiplier == 1
    schedule = {e["task"] for e in conf.beat_schedule.values()}
    assert schedule == {
        "smartretail.inventory.sweep_expired_reservations",
        "smartretail.inventory.sweep_processed_events",
    }


async def test_the_sweep_task_expires_overdue_reservations(app, client, callers, task_env) -> None:
    merchant = str(uuid.uuid4())
    [variant] = await init_stock(client, callers, merchant, on_hand=5)
    r = await reserve(client, callers, str(uuid.uuid4()), [(variant, 2)])
    assert r.status_code in (200, 201), r.text
    async with app.state.db.engine.begin() as conn:
        await conn.execute(text("UPDATE reservations SET expires_at = now() - interval '1 minute'"))

    result = await asyncio.to_thread(lambda: sweep_expired_reservations.apply().get())
    assert result == 1
    assert (await public(client, variant))["available"] == 5
    # Safe to run twice: nothing left to expire.
    assert await asyncio.to_thread(lambda: sweep_expired_reservations.apply().get()) == 0


async def test_the_dedupe_sweep_task_runs(app, task_env) -> None:
    async with app.state.db.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO processed_events (consumer_group, event_id, event_type, processed_at)"
                " VALUES ('inventory', gen_random_uuid(), 'x', now() - interval '40 days'),"
                "        ('inventory', gen_random_uuid(), 'x', now())"
            )
        )
    assert await asyncio.to_thread(lambda: sweep_processed_events.apply().get()) == 1
