"""``stream_cursors``: cold start, the clamped ``greatest()`` advance, retention.

The cursor is the device's replay floor (C22). It is advanced only by the ack
(C23), never past the newest *settled* ``seq`` — a row older than the
visibility window — so a high ack cannot strand a lower ``seq`` still in
flight (the visibility trap, I5).
"""

import uuid

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.v1.notifications.enums import NotificationChannel
from app.v1.notifications.models import Notification
from app.v1.stream.models import StreamCursor


class CursorRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, user_id: uuid.UUID, client_id: str) -> StreamCursor | None:
        cursor: StreamCursor | None = await self.session.scalar(
            select(StreamCursor).where(
                StreamCursor.user_id == user_id, StreamCursor.client_id == client_id
            )
        )
        return cursor

    async def cold_start(
        self, user_id: uuid.UUID, client_id: str, at_seq: int, user_agent: str | None
    ) -> None:
        """A never-seen device starts at the user's current ``max(seq)``: zero replay."""
        await self.session.execute(
            pg_insert(StreamCursor)
            .values(user_id=user_id, client_id=client_id, cursor_seq=at_seq, user_agent=user_agent)
            .on_conflict_do_update(
                index_elements=["user_id", "client_id"],
                set_={"last_seen_at": func.now(), "updated_at": func.now()},
            )
        )

    async def touch(self, cursor: StreamCursor, user_agent: str | None) -> None:
        await self.session.execute(
            update(StreamCursor)
            .where(StreamCursor.id == cursor.id)
            .values(
                last_seen_at=func.now(),
                updated_at=func.now(),
                user_agent=user_agent or StreamCursor.user_agent,
            )
        )

    async def advance(
        self,
        user_id: uuid.UUID,
        client_id: str,
        up_to_seq: int,
        *,
        visibility_window_seconds: int,
    ) -> None:
        settled = (
            select(func.coalesce(func.max(Notification.seq), 0))
            .where(
                Notification.user_id == user_id,
                Notification.channel == NotificationChannel.IN_APP,
                Notification.created_at
                < func.now() - func.make_interval(0, 0, 0, 0, 0, 0, visibility_window_seconds),
            )
            .scalar_subquery()
        )
        insert = pg_insert(StreamCursor).values(
            user_id=user_id, client_id=client_id, cursor_seq=func.least(up_to_seq, settled)
        )
        await self.session.execute(
            insert.on_conflict_do_update(
                index_elements=["user_id", "client_id"],
                set_={
                    "cursor_seq": func.greatest(
                        StreamCursor.cursor_seq, insert.excluded.cursor_seq
                    ),
                    "last_seen_at": func.now(),
                    "updated_at": func.now(),
                },
            )
        )

    async def sweep(self, *, retention_days: int, max_per_user: int) -> int:
        """Deletes cursors idle past retention, then all but each user's newest
        ``max_per_user`` by ``last_seen_at``. Safe to run twice."""
        stale = await self.session.execute(
            delete(StreamCursor).where(
                StreamCursor.last_seen_at < func.now() - func.make_interval(0, 0, 0, retention_days)
            )
        )
        ranked = select(
            StreamCursor.id,
            func.row_number()
            .over(
                partition_by=StreamCursor.user_id,
                order_by=(StreamCursor.last_seen_at.desc(), StreamCursor.id),
            )
            .label("rank"),
        ).subquery()
        excess = await self.session.execute(
            delete(StreamCursor).where(
                StreamCursor.id.in_(select(ranked.c.id).where(ranked.c.rank > max_per_user))
            )
        )
        await self.session.commit()
        return int(getattr(stale, "rowcount", 0) or 0) + int(getattr(excess, "rowcount", 0) or 0)
