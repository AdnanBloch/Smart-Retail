"""The bounded catch-up (C22) and the replay floor (C18, NTF-12).

Floor selection on stream open:

* no ``client_id`` (ephemeral, NTF-12): ``Last-Event-ID``, or cold start;
* a ``client_id`` with no cursor row: **cold start** — a row at the user's
  ``max(seq)``, a ``hello`` with ``cold_start: true``, zero frames;
* a cursor row: resume from ``greatest(cursor_seq, Last-Event-ID)``.

Catch-up reads ``in_app`` rows of the last 24 h above the floor — plus the
5-second visibility window (the ``seq`` trap) — in ``seq`` order, at most 50.
On truncation the **oldest** 50 are sent, so the client's cursor advances
contiguously into what it pages from ``GET /notifications?since_seq=``.
Frames are not scope-filtered yet: every device receives them (NTF-D26).
"""

import uuid
from dataclasses import dataclass, field
from enum import StrEnum

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.v1.notifications.enums import NotificationChannel
from app.v1.notifications.models import Notification
from app.v1.notifications.repo import NotificationRepo
from app.v1.stream import frames
from app.v1.stream.cursor_repo import CursorRepo


class StreamMode(StrEnum):
    COLD = "cold"
    RESUME = "resume"
    EPHEMERAL = "ephemeral"


@dataclass
class CatchUp:
    rows: list[Notification]
    omitted: int


@dataclass
class Opening:
    """What a stream sends before going live, and the ids it has already sent."""

    mode: StreamMode
    frames: list[str]
    sent_ids: set[str] = field(default_factory=set)


def choose_floor(
    *, client_id: str | None, cursor_seq: int | None, last_event_id: int | None
) -> tuple[StreamMode, int | None]:
    """Pure: the mode and replay floor (``None`` = cold start, replay nothing)."""
    if client_id is None:
        if last_event_id is None:
            return StreamMode.COLD, None
        return StreamMode.EPHEMERAL, last_event_id
    if cursor_seq is None:
        return StreamMode.COLD, None
    return StreamMode.RESUME, max(cursor_seq, last_event_id or 0)


async def catch_up(
    session: AsyncSession,
    user_id: uuid.UUID,
    floor: int,
    *,
    max_frames: int,
    max_age_hours: int,
    visibility_window_seconds: int,
) -> CatchUp:
    now = func.now()
    predicate = [
        Notification.user_id == user_id,
        Notification.channel == NotificationChannel.IN_APP,
        Notification.created_at > now - func.make_interval(0, 0, 0, 0, max_age_hours),
        or_(
            Notification.seq > floor,
            Notification.created_at
            > now - func.make_interval(0, 0, 0, 0, 0, 0, visibility_window_seconds),
        ),
    ]
    rows = list(
        await session.scalars(
            select(Notification).where(*predicate).order_by(Notification.seq).limit(max_frames + 1)
        )
    )
    if len(rows) <= max_frames:
        return CatchUp(rows, 0)
    total = int(
        await session.scalar(select(func.count()).select_from(Notification).where(*predicate)) or 0
    )
    return CatchUp(rows[:max_frames], total - max_frames)


async def open_stream(
    session: AsyncSession,
    settings: Settings,
    *,
    user_id: uuid.UUID,
    client_id: str | None,
    last_event_id: int | None,
    user_agent: str | None,
    retry_ms: int,
) -> Opening:
    """Cursor read/cold start, catch-up, and the opening frames, in one transaction."""
    repo = NotificationRepo(session)
    cursors = CursorRepo(session)
    unread = await repo.unread_count(user_id)
    server_seq = await repo.max_seq(user_id)
    cursor = await cursors.get(user_id, client_id) if client_id is not None else None
    mode, floor = choose_floor(
        client_id=client_id,
        cursor_seq=cursor.cursor_seq if cursor is not None else None,
        last_event_id=last_event_id,
    )
    if client_id is not None:
        if cursor is None:
            await cursors.cold_start(user_id, client_id, server_seq, user_agent)
        else:
            await cursors.touch(cursor, user_agent)
    await session.commit()

    hello = frames.hello(
        client_id=client_id,
        cold_start=floor is None,
        unread_count=unread,
        server_seq=server_seq,
        resumed_from=floor,
        retry_ms=retry_ms,
    )
    if floor is None:
        return Opening(mode, [hello, frames.unread_count(unread)])

    caught = await catch_up(
        session,
        user_id,
        floor,
        max_frames=settings.catchup_max_frames,
        max_age_hours=settings.catchup_max_age_hours,
        visibility_window_seconds=settings.seq_visibility_window_seconds,
    )
    out = [hello]
    if caught.omitted:
        out.append(
            frames.catchup_truncated(
                omitted_count=caught.omitted,
                oldest_seq_sent=caught.rows[0].seq if caught.rows else None,
            )
        )
    out += [
        frames.notification(
            id=str(row.id),
            seq=row.seq,
            type=str(row.type),
            title=row.title,
            body=row.body,
            created_at=row.created_at,
            origin=client_id is not None and row.origin_client_id == client_id,
        )
        for row in caught.rows
    ]
    out.append(frames.unread_count(unread))
    return Opening(mode, out, {str(row.id) for row in caught.rows})
