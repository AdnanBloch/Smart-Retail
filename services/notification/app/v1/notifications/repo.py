"""``NotificationRepo``: the read side — list, mark-read, read-all, unread count,
the admin view, and the after-commit re-read (RFC 0007 §3.1.2.1).

Every user-facing query is ``channel = 'in_app'`` (NTF-2) and keyed by the
caller's ``user_id`` (I7). Nothing here is scope- or cursor-filtered (I6).
"""

import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import Select, and_, exists, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.v1.notifications.enums import (
    DeliveryStatus,
    NotificationChannel,
    NotificationDeliveryScope,
    NotificationType,
)
from app.v1.notifications.models import DeliveryAttempt, Notification
from app.v1.notifications.utils import DISPATCHED_CHANNELS
from smartretail_common.pagination import PageParams, Slice

IN_APP = NotificationChannel.IN_APP


@dataclass(frozen=True)
class ListFilters:
    read: bool | None = None
    type: NotificationType | None = None
    since_seq: int | None = None


@dataclass(frozen=True)
class AdminFilters:
    user_id: uuid.UUID | None = None
    type: NotificationType | None = None
    channel: NotificationChannel | None = None
    delivery_scope: NotificationDeliveryScope | None = None
    undelivered: bool | None = None
    dispatch_status: str | None = None  # "failed" | "none"
    correlation_id: str | None = None
    since: datetime | None = None
    until: datetime | None = None


@dataclass(frozen=True)
class AdminRow:
    notification: Notification
    attempts: list[DeliveryAttempt]


class NotificationRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    # --- the owner's view -------------------------------------------------

    async def list_own(
        self, user_id: uuid.UUID, filters: ListFilters, page: PageParams
    ) -> Slice[Notification]:
        query = select(Notification).where(
            Notification.user_id == user_id, Notification.channel == IN_APP
        )
        if filters.read is True:
            query = query.where(Notification.read_at.is_not(None))
        elif filters.read is False:
            query = query.where(Notification.read_at.is_(None))
        if filters.type is not None:
            query = query.where(Notification.type == filters.type)
        if filters.since_seq is not None:
            # NTF-16: forward paging for a truncated client, ascending seq.
            query = query.where(Notification.seq > filters.since_seq).order_by(Notification.seq)
        else:
            query = query.order_by(Notification.created_at.desc(), Notification.seq.desc())
        return await self._page(query, page)

    async def unread_count(self, user_id: uuid.UUID) -> int:
        count = await self.session.scalar(
            select(func.count())
            .select_from(Notification)
            .where(
                Notification.user_id == user_id,
                Notification.channel == IN_APP,
                Notification.read_at.is_(None),
            )
        )
        return int(count or 0)

    async def max_seq(self, user_id: uuid.UUID) -> int:
        seq = await self.session.scalar(
            select(func.max(Notification.seq)).where(
                Notification.user_id == user_id, Notification.channel == IN_APP
            )
        )
        return int(seq or 0)

    async def mark_read(
        self, user_id: uuid.UUID, notification_id: uuid.UUID
    ) -> tuple[Notification, bool] | None:
        """I8: ``read_at`` is written once. Returns the row and whether this call changed it."""
        changed = await self.session.scalar(
            update(Notification)
            .where(
                Notification.id == notification_id,
                Notification.user_id == user_id,
                Notification.channel == IN_APP,
                Notification.read_at.is_(None),
            )
            .values(read_at=func.now())
            .returning(Notification.id)
        )
        row = await self.session.scalar(
            select(Notification)
            .where(
                Notification.id == notification_id,
                Notification.user_id == user_id,
                Notification.channel == IN_APP,
            )
            .execution_options(populate_existing=True)
        )
        await self.session.commit()
        if row is None:
            return None
        return row, changed is not None

    async def read_all(self, user_id: uuid.UUID) -> int:
        result = await self.session.execute(
            update(Notification)
            .where(
                Notification.user_id == user_id,
                Notification.channel == IN_APP,
                Notification.read_at.is_(None),
            )
            .values(read_at=func.now())
        )
        await self.session.commit()
        return int(getattr(result, "rowcount", 0) or 0)

    # --- the consumer's after-commit step -----------------------------------

    async def by_event(self, event_id: uuid.UUID) -> list[Notification]:
        """Every row recorded from one event (the NTF-1 index's leading column)."""
        rows = await self.session.scalars(
            select(Notification)
            .where(Notification.payload["event_id"].astext == str(event_id))
            .order_by(Notification.seq)
        )
        return list(rows)

    # --- the operational view ------------------------------------------------

    async def admin_list(self, filters: AdminFilters, page: PageParams) -> Slice[AdminRow]:
        query = select(Notification)
        conditions = []
        if filters.user_id is not None:
            conditions.append(Notification.user_id == filters.user_id)
        if filters.type is not None:
            conditions.append(Notification.type == filters.type)
        if filters.channel is not None:
            conditions.append(Notification.channel == filters.channel)
        if filters.delivery_scope is not None:
            conditions.append(Notification.delivery_scope == filters.delivery_scope)
        if filters.undelivered is True:
            conditions += [Notification.channel == IN_APP, Notification.delivered_at.is_(None)]
        elif filters.undelivered is False:
            conditions.append(Notification.delivered_at.is_not(None))
        if filters.correlation_id is not None:
            conditions.append(Notification.correlation_id == filters.correlation_id)
        if filters.since is not None:
            conditions.append(Notification.created_at >= filters.since)
        if filters.until is not None:
            conditions.append(Notification.created_at < filters.until)
        if filters.dispatch_status == "none":
            conditions += [
                Notification.channel.in_(sorted(DISPATCHED_CHANNELS)),
                ~exists().where(DeliveryAttempt.notification_id == Notification.id),
            ]
        elif filters.dispatch_status == "failed":
            newest = (
                select(DeliveryAttempt.status)
                .where(DeliveryAttempt.notification_id == Notification.id)
                .order_by(DeliveryAttempt.attempt.desc())
                .limit(1)
                .scalar_subquery()
            )
            conditions.append(newest == DeliveryStatus.FAILED)
        if conditions:
            query = query.where(and_(*conditions))
        query = query.order_by(Notification.created_at.desc(), Notification.seq.desc())
        rows = await self._page(query, page)
        attempts: dict[uuid.UUID, list[DeliveryAttempt]] = defaultdict(list)
        ids = [n.id for n in rows.items]
        if ids:
            for attempt in await self.session.scalars(
                select(DeliveryAttempt)
                .where(DeliveryAttempt.notification_id.in_(ids))
                .order_by(DeliveryAttempt.attempt)
            ):
                attempts[attempt.notification_id].append(attempt)
        return Slice(
            items=[AdminRow(n, attempts[n.id]) for n in rows.items],
            total=rows.total,
            params=page,
        )

    async def _page(
        self, query: Select[tuple[Notification]], page: PageParams
    ) -> Slice[Notification]:
        total = await self.session.scalar(select(func.count()).select_from(query.subquery()))
        items = await self.session.scalars(query.limit(page.limit).offset(page.offset))
        return Slice(items=list(items), total=int(total or 0), params=page)
