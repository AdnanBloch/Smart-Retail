"""After the consumer's transaction commits: fan out and enqueue (RFC 0007 §3.1.1.4).

Re-reads the event's rows by ``event_id`` (NTF-D34), then for each
``in_app`` row ``PUBLISH``es on ``nt:user:{user_id}`` with the recipient's
unread count (NTF-11), and for each ``email``/``sms`` row enqueues
``dispatch_notification``. Every failure is logged and swallowed: the rows
are durable, a missed publish is replayed on the next connect (C16, C18) and
a missed enqueue is recovered by the redispatch sweep (NTF-6).
"""

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.v1.notifications.enums import NotificationChannel
from app.v1.notifications.repo import NotificationRepo
from app.v1.notifications.utils import DISPATCHED_CHANNELS
from app.v1.stream.fanout import notification_message, publish
from smartretail_common.events.envelope import Envelope

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger = logging.getLogger("notification.after_commit")

Enqueue = Callable[[uuid.UUID], Awaitable[None]]


async def celery_enqueue(notification_id: uuid.UUID) -> None:
    """``dispatch_notification`` on RabbitMQ; ``send_task`` blocks, so off the loop."""
    from app.core.celery_app import DISPATCH_TASK
    from app.core.celery_app import app as celery_app

    await asyncio.to_thread(celery_app.send_task, DISPATCH_TASK, args=[str(notification_id)])


def after_commit(
    sessionmaker: async_sessionmaker[AsyncSession],
    redis: "Redis",
    enqueue: Enqueue = celery_enqueue,
) -> Callable[[Envelope], Awaitable[None]]:
    async def run(envelope: Envelope) -> None:
        try:
            async with sessionmaker() as session:
                repo = NotificationRepo(session)
                rows = await repo.by_event(envelope.event_id)
                unread = {
                    row.user_id: await repo.unread_count(row.user_id)
                    for row in rows
                    if row.channel == NotificationChannel.IN_APP
                }
        except Exception as exc:
            logger.warning(
                "after-commit re-read failed",
                extra={"event_id": str(envelope.event_id), "error": repr(exc)},
            )
            return
        for row in rows:
            if row.channel == NotificationChannel.IN_APP:
                await publish(
                    redis,
                    row.user_id,
                    notification_message(
                        id=str(row.id),
                        seq=row.seq,
                        type=str(row.type),
                        title=row.title,
                        body=row.body,
                        created_at=row.created_at.isoformat(),
                        origin_client_id=row.origin_client_id,
                        scope=str(row.delivery_scope),
                        unread_count=unread[row.user_id],
                    ),
                )
            elif row.channel in DISPATCHED_CHANNELS:
                try:
                    await enqueue(row.id)
                except Exception as exc:
                    logger.warning(
                        "dispatch enqueue failed; the redispatch sweep recovers it",
                        extra={"notification_id": str(row.id), "error": repr(exc)},
                    )

    return run
