"""``sweep_stale_stream_cursors`` — Celery Beat, daily (C22, §3.1.3 stream_cursors).

Deletes cursors idle for 90 days, then enforces 20 per user by
``last_seen_at``, so a client rotating its id cannot grow the table.
"""

from app.core.celery_app import SWEEP_CURSORS_TASK, app
from app.core.config import get_settings
from app.core.db import create_database
from app.v1.stream.cursor_repo import CursorRepo
from smartretail_common.celery_app import TASK_DEFAULTS, run_async


async def sweep() -> int:
    settings = get_settings()
    db = create_database(settings)
    try:
        async with db.sessionmaker() as session:
            return await CursorRepo(session).sweep(
                retention_days=settings.stream_cursor_retention_days,
                max_per_user=settings.stream_cursor_max_per_user,
            )
    finally:
        await db.dispose()


@app.task(name=SWEEP_CURSORS_TASK, **TASK_DEFAULTS)
def sweep_stale_stream_cursors() -> int:
    return run_async(sweep)
