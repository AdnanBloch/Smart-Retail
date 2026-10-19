"""search/ Celery tasks: ``run_reindex`` and ``anonymise_query_log`` (§3.1.1.4)."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.celery_app import RUN_REINDEX, TASK_PREFIX, app
from app.core.config import get_settings
from app.core.db import create_database
from app.v1.search.models import QueryLog
from app.v1.search.reindex import ReindexScope, reindex_with_settings
from smartretail_common.celery_app import TASK_DEFAULTS, run_async


async def anonymise(session: AsyncSession, *, days: int, now: datetime | None = None) -> int:
    cutoff = (now or datetime.now(UTC)) - timedelta(days=days)
    result = await session.execute(
        update(QueryLog)
        .where(QueryLog.created_at < cutoff, QueryLog.user_id.is_not(None))
        .values(user_id=None)
    )
    await session.commit()
    return int(getattr(result, "rowcount", 0) or 0)


@app.task(name=TASK_PREFIX + "anonymise_query_log", **TASK_DEFAULTS)
def anonymise_query_log() -> int:
    async def work() -> int:
        settings = get_settings()
        db = create_database(settings)
        try:
            async with db.sessionmaker() as session:
                return await anonymise(session, days=settings.query_log_retention_days)
        finally:
            await db.dispose()

    return run_async(work)


@app.task(name=RUN_REINDEX, **TASK_DEFAULTS)
def run_reindex(scope: dict[str, object]) -> dict[str, int]:
    async def work() -> dict[str, int]:
        report = await reindex_with_settings(get_settings(), ReindexScope.from_dict(scope))
        return {"indexed": report.indexed, "skipped": report.skipped, "failed": report.failed}

    return run_async(work)
