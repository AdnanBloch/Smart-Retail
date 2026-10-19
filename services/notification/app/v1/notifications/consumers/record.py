"""The six notifying event types → ``RecordingRepo`` (RFC 0007 §3.1.2.4).

``identity.user.registered``, ``order.confirmed``, ``order.failed``,
``order.cancelled``, ``shipment.status_changed`` and ``inventory.stockout``
share one thin handler: the per-type differences are data in
``notifications/utils.py`` and the templates, not code paths.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from app.v1.notifications.recording_repo import RecordingRepo
from smartretail_common.events.consumer import Handler
from smartretail_common.events.envelope import Envelope

EVENT_TYPES = (
    "identity.user.registered",
    "order.confirmed",
    "order.failed",
    "order.cancelled",
    "shipment.status_changed",
    "inventory.stockout",
)


def handler() -> Handler:
    async def handle(event: Envelope, session: AsyncSession) -> None:
        await RecordingRepo(session).record(event)

    return handle
