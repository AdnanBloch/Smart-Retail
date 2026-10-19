"""``order.cancelled`` → release the order's held reservations (RFC 0006 §3.1.2.4).

A safety net beneath the order saga, which releases its reservations itself
before it emits the event: normally every reservation is already released and
this is a no-op. It matters when an order is cancelled by a path that did not
release (an operator, a future admin tool), so no stock stays held until the
expiry sweep.
"""

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.v1.reservations.enums import ReleaseReason
from app.v1.reservations.repo import ReservationRepo
from smartretail_common.events.consumer import Handler
from smartretail_common.events.envelope import Envelope

ACTOR = "consumer:order.cancelled"


def handler(settings: Settings) -> Handler:
    async def handle(event: Envelope, session: AsyncSession) -> None:
        await ReservationRepo(session, settings).release_for_order(
            uuid.UUID(event.payload["order_id"]),
            reason=ReleaseReason.ORDER_CANCELLED,
            actor=ACTOR,
            correlation_id=event.correlation_id,
        )

    return handle
