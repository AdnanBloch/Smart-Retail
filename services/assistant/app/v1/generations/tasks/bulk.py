"""generations/ Celery task: ``generate_bulk_content(job_id)`` on ``assistant.bulk`` (E12)."""

import uuid

from app.core.celery_app import GENERATE_BULK, app
from app.core.clients.llm import build_llm
from app.core.config import get_settings
from app.core.db import create_database
from app.v1.generations.bulk_repo import run_bulk_job
from smartretail_common.celery_app import TASK_DEFAULTS, run_async


@app.task(name=GENERATE_BULK, **TASK_DEFAULTS)
def generate_bulk_content(job_id: str) -> str | None:
    async def work() -> str | None:
        settings = get_settings()
        db = create_database(settings)
        try:
            status = await run_bulk_job(db, build_llm(settings), settings, uuid.UUID(job_id))
            return str(status) if status else None
        finally:
            await db.dispose()

    return run_async(work)
