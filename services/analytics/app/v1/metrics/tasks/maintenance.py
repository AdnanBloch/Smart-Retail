"""Celery task shims for ``metrics`` (ANL-D27): each opens its own Mongo client
on the task's own event loop and calls the repo."""

from typing import Any

from app.core.celery_app import app
from app.core.config import get_settings
from app.core.db import create_mongo
from app.v1.metrics.models import prepare_store
from app.v1.metrics.reconcile_repo import rebuild_daily_rollups as rebuild
from smartretail_common.celery_app import TASK_DEFAULTS, run_async


async def _rebuild() -> dict[str, Any]:
    settings = get_settings()
    mongo = create_mongo(settings)
    try:
        await prepare_store(mongo, settings)
        report = await rebuild(mongo, settings)
        return {"days": report.days, "drift": report.drift}
    finally:
        await mongo.close()


@app.task(name="smartretail.analytics.rebuild_daily_rollups", **TASK_DEFAULTS)
def rebuild_daily_rollups() -> dict[str, Any]:
    return run_async(_rebuild)
