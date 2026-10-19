"""The ack (C23): stamp ``delivered_at`` by explicit id, advance the device cursor.

Idempotent by construction — first writer wins on ``delivered_at`` (I3), and
the cursor only moves up through ``greatest()`` with the visibility clamp (I5).
Foreign and unknown ids are silently ignored: every update is guarded by the
caller's ``user_id``.
"""

import uuid
from collections.abc import Sequence

from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.v1.notifications.enums import NotificationChannel
from app.v1.notifications.models import Notification
from app.v1.stream.cursor_repo import CursorRepo


class DeliveryReportRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def ack(
        self,
        user_id: uuid.UUID,
        client_id: str,
        *,
        ids: Sequence[uuid.UUID],
        up_to_seq: int | None,
        visibility_window_seconds: int,
    ) -> int:
        """Returns how many rows this ack stamped (0 on a replay)."""
        stamped = 0
        if ids:
            result = await self.session.execute(
                update(Notification)
                .where(
                    Notification.id.in_(list(ids)),
                    Notification.user_id == user_id,
                    Notification.channel == NotificationChannel.IN_APP,
                    Notification.delivered_at.is_(None),
                )
                .values(delivered_at=func.now(), delivered_client_id=client_id)
            )
            stamped = int(getattr(result, "rowcount", 0) or 0)
        if up_to_seq is not None:
            await CursorRepo(self.session).advance(
                user_id,
                client_id,
                up_to_seq,
                visibility_window_seconds=visibility_window_seconds,
            )
        await self.session.commit()
        return stamped
