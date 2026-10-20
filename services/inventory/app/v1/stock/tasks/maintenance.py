"""``smartretail.inventory.reconcile_inventory_ledger`` — Celery Beat, daily (INV-D23).

Read-only, so safe to run twice. Returns the number of drifting variants; any
drift is logged per variant and left for a human to correct.
"""

from app.core.celery_app import app
from app.core.config import get_settings
from app.core.db import create_database
from app.v1.stock import reconcile
from smartretail_common.celery_app import TASK_DEFAULTS, run_async


async def run() -> int:
    db = create_database(get_settings())
    try:
        return len(await reconcile.reconcile_once(db))
    finally:
        await db.dispose()


@app.task(name="smartretail.inventory.reconcile_inventory_ledger", **TASK_DEFAULTS)
def reconcile_inventory_ledger() -> int:
    return run_async(run)
