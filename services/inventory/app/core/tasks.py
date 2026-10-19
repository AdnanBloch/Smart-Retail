"""Platform maintenance for inventory's consumer (PLT-17): shared by both
consuming entities, so it lives in ``core`` (ARC-D27)."""

from app.core.celery_app import app
from app.core.config import get_settings
from app.core.db import create_database
from smartretail_common.celery_app import TASK_DEFAULTS, run_async
from smartretail_common.events import dedupe


async def sweep() -> int:
    db = create_database(get_settings())
    try:
        async with db.sessionmaker() as session:
            return await dedupe.sweep(session)
    finally:
        await db.dispose()


@app.task(name="smartretail.inventory.sweep_processed_events", **TASK_DEFAULTS)
def sweep_processed_events() -> int:
    return run_async(sweep)
