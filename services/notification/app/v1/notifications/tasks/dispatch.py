"""``dispatch_notification`` and ``redispatch_undispatched_notifications`` (NTF-5, NTF-6).

Dispatch retries are driven by the attempt rows, not by Celery's counter: a
failed retryable attempt with attempts left schedules ``self.retry`` with the
NTF-5 backoff; the fifth failure is the last row written. A redelivered or
re-enqueued task finds a ``sent`` attempt and returns.
"""

import logging
import uuid
from typing import Any

from app.core.celery_app import DISPATCH_TASK, REDISPATCH_TASK, app
from app.core.config import get_settings
from app.core.db import create_database
from app.v1.notifications.adapters import registry
from app.v1.notifications.dispatch_repo import DispatchOutcome, DispatchRepo, DispatchResult
from app.v1.notifications.utils import backoff_seconds
from smartretail_common.celery_app import TASK_DEFAULTS, run_async

logger = logging.getLogger("notification.tasks")


async def dispatch(notification_id: str) -> DispatchResult:
    settings = get_settings()
    db = create_database(settings)
    try:
        async with db.sessionmaker() as session:
            return await DispatchRepo(session).dispatch_once(
                uuid.UUID(notification_id),
                registry(settings),
                max_attempts=settings.dispatch_max_retries,
            )
    finally:
        await db.dispose()


@app.task(
    name=DISPATCH_TASK,
    bind=True,
    max_retries=None,  # the ceiling is the attempt rows' (NTF-5)
    soft_time_limit=30,
    time_limit=60,
    **{k: v for k, v in TASK_DEFAULTS.items() if k != "max_retries"},
)
def dispatch_notification(self: Any, notification_id: str) -> str:
    result = run_async(lambda: dispatch(notification_id))
    if result.outcome == DispatchOutcome.RETRY:
        settings = get_settings()
        countdown = backoff_seconds(
            result.attempt,
            base=settings.dispatch_backoff_base_seconds,
            cap=settings.dispatch_backoff_max_seconds,
        )
        raise self.retry(countdown=countdown)
    return str(result.outcome)


async def redispatch() -> list[uuid.UUID]:
    settings = get_settings()
    db = create_database(settings)
    try:
        async with db.sessionmaker() as session:
            return await DispatchRepo(session).undispatched(
                min_age_seconds=settings.redispatch_min_age_seconds,
                max_age_hours=settings.redispatch_max_age_hours,
                stale_pending_seconds=settings.dispatch_stale_pending_seconds,
                max_attempts=settings.dispatch_max_retries,
            )
    finally:
        await db.dispose()


@app.task(name=REDISPATCH_TASK, **TASK_DEFAULTS)
def redispatch_undispatched_notifications() -> int:
    ids = run_async(redispatch)
    for notification_id in ids:
        dispatch_notification.delay(str(notification_id))
    if ids:
        logger.info("redispatched notifications", extra={"count": len(ids)})
    return len(ids)
