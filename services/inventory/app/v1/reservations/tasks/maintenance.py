"""``smartretail.inventory.sweep_expired_reservations`` — Celery Beat, every minute (B5, INV-10).

The same ``sweep_once`` the in-process sweeper calls. Safe to run twice and
alongside the in-process loop: every expiry is re-checked under its row
locks, so a lost race is a no-op.
"""

from app.core.celery_app import app
from app.core.config import get_settings
from app.core.db import create_database
from app.v1.reservations import sweeper
from smartretail_common.celery_app import TASK_DEFAULTS, run_async


async def sweep() -> int:
    settings = get_settings()
    db = create_database(settings)
    try:
        return await sweeper.sweep_once(db, settings)
    finally:
        await db.dispose()


@app.task(name="smartretail.inventory.sweep_expired_reservations", **TASK_DEFAULTS)
def sweep_expired_reservations() -> int:
    return run_async(sweep)
