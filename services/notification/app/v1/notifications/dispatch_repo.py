"""Attempt rows and the adapter call (NTF-4, NTF-5, NTF-6; §3.1.1.3 Delivery attempt).

``dispatch_once`` is one attempt: no-op if a ``sent`` attempt exists or the
ceiling is reached; otherwise insert attempt *n + 1* as ``pending``, commit,
call the adapter, and record ``sent`` or ``failed``. History is append-only.
The Celery task decides whether to retry from the returned outcome.
"""

import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy import and_, exists, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.v1.notifications.adapters.base import Failed, NotificationAdapter, NotificationView
from app.v1.notifications.enums import DeliveryStatus, NotificationChannel
from app.v1.notifications.models import DeliveryAttempt, Notification
from app.v1.notifications.utils import DISPATCHED_CHANNELS

logger = logging.getLogger("notification.dispatch")


class DispatchOutcome(StrEnum):
    SENT = "sent"
    ALREADY_SENT = "already_sent"
    RETRY = "retry"  # failed, retryable, attempts remain
    EXHAUSTED = "exhausted"  # failed with no attempts left, or not retryable
    SKIPPED = "skipped"  # unknown id, an in_app row, or a channel with no adapter


@dataclass(frozen=True)
class DispatchResult:
    outcome: DispatchOutcome
    attempt: int = 0


class DispatchRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def dispatch_once(
        self,
        notification_id: uuid.UUID,
        adapters: Mapping[NotificationChannel, NotificationAdapter],
        *,
        max_attempts: int,
    ) -> DispatchResult:
        row = await self.session.get(Notification, notification_id)
        if row is None or row.channel not in DISPATCHED_CHANNELS:
            return DispatchResult(DispatchOutcome.SKIPPED)
        adapter = adapters.get(NotificationChannel(row.channel))
        if adapter is None:
            return DispatchResult(DispatchOutcome.SKIPPED)
        # Serialise concurrent dispatches of one notification (a redispatch
        # racing a retry): the row lock is held until the pending row commits.
        await self.session.execute(
            select(Notification.id).where(Notification.id == notification_id).with_for_update()
        )
        sent = await self.session.scalar(
            select(
                exists().where(
                    DeliveryAttempt.notification_id == notification_id,
                    DeliveryAttempt.status == DeliveryStatus.SENT,
                )
            )
        )
        if sent:
            await self.session.commit()
            return DispatchResult(DispatchOutcome.ALREADY_SENT)
        last = int(
            await self.session.scalar(
                select(func.coalesce(func.max(DeliveryAttempt.attempt), 0)).where(
                    DeliveryAttempt.notification_id == notification_id
                )
            )
            or 0
        )
        if last >= max_attempts:
            await self.session.commit()
            return DispatchResult(DispatchOutcome.EXHAUSTED, last)
        attempt = DeliveryAttempt(
            notification_id=notification_id,
            channel=row.channel,
            status=DeliveryStatus.PENDING,
            attempt=last + 1,
        )
        self.session.add(attempt)
        view = NotificationView(
            id=row.id,
            user_id=row.user_id,
            channel=str(row.channel),
            type=str(row.type),
            title=row.title,
            body=row.body,
            correlation_id=row.correlation_id,
        )
        await self.session.commit()

        try:
            result = await adapter.send(view)
        except Exception as exc:  # an adapter that raises is a retryable failure
            result = Failed(f"{type(exc).__name__}: {exc}", retryable=True)

        if isinstance(result, Failed):
            await self._finish(attempt.id, DeliveryStatus.FAILED, result.error[:1000])
            logger.warning(
                "dispatch attempt failed",
                extra={
                    "notification_id": str(notification_id),
                    "channel": str(row.channel),
                    "attempt": attempt.attempt,
                    "error": result.error,
                },
            )
            if result.retryable and attempt.attempt < max_attempts:
                return DispatchResult(DispatchOutcome.RETRY, attempt.attempt)
            logger.error(
                "dispatch exhausted",
                extra={"notification_id": str(notification_id), "attempt": attempt.attempt},
            )
            return DispatchResult(DispatchOutcome.EXHAUSTED, attempt.attempt)
        await self._finish(attempt.id, DeliveryStatus.SENT, None)
        return DispatchResult(DispatchOutcome.SENT, attempt.attempt)

    async def _finish(
        self, attempt_id: uuid.UUID, status: DeliveryStatus, error: str | None
    ) -> None:
        await self.session.execute(
            update(DeliveryAttempt)
            .where(DeliveryAttempt.id == attempt_id)
            .values(status=status, error=error)
        )
        await self.session.commit()

    async def undispatched(
        self,
        *,
        min_age_seconds: int,
        max_age_hours: int,
        stale_pending_seconds: int,
        max_attempts: int,
        limit: int = 500,
    ) -> list[uuid.UUID]:
        """NTF-6: dispatched-channel rows with no attempt (2 min to 24 h old), and
        rows whose newest attempt is a ``pending`` older than the task's hard
        time limit (a crashed attempt) while attempts remain."""
        age = and_(
            Notification.channel.in_(sorted(DISPATCHED_CHANNELS)),
            Notification.created_at
            < func.now() - func.make_interval(0, 0, 0, 0, 0, 0, min_age_seconds),
            Notification.created_at > func.now() - func.make_interval(0, 0, 0, 0, max_age_hours),
        )
        never = select(Notification.id).where(
            age, ~exists().where(DeliveryAttempt.notification_id == Notification.id)
        )
        newest = (
            select(
                DeliveryAttempt.notification_id,
                DeliveryAttempt.status,
                DeliveryAttempt.attempt,
                DeliveryAttempt.created_at,
                func.row_number()
                .over(
                    partition_by=DeliveryAttempt.notification_id,
                    order_by=DeliveryAttempt.attempt.desc(),
                )
                .label("rank"),
            )
        ).subquery()
        crashed = (
            select(Notification.id)
            .join(newest, newest.c.notification_id == Notification.id)
            .where(
                age,
                newest.c.rank == 1,
                newest.c.status == DeliveryStatus.PENDING,
                newest.c.attempt < max_attempts,
                newest.c.created_at
                < func.now() - func.make_interval(0, 0, 0, 0, 0, 0, stale_pending_seconds),
            )
        )
        ids = await self.session.scalars(never.union(crashed).limit(limit))
        return list(ids)
