"""chat/ Celery task: ``sweep_anonymous_conversations`` (hourly, Q5 (0005))."""

from app.core.celery_app import TASK_PREFIX, app
from app.core.config import get_settings
from app.core.db import create_database
from app.v1.chat.repo import sweep_anonymous
from smartretail_common.celery_app import TASK_DEFAULTS, run_async


@app.task(name=TASK_PREFIX + "sweep_anonymous_conversations", **TASK_DEFAULTS)
def sweep_anonymous_conversations() -> int:
    async def work() -> int:
        settings = get_settings()
        db = create_database(settings)
        try:
            async with db.sessionmaker() as session:
                return await sweep_anonymous(session, hours=settings.anon_conversation_ttl_hours)
        finally:
            await db.dispose()

    return run_async(work)
