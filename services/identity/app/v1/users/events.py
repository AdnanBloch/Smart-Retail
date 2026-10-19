"""Outbox payloads for ``identity.*`` (RFC 0003 §3.1.2.3, IDN-15).

Aggregate and Kafka key are the user id (C3). ``client_id`` is the
registering device's ``X-Client-Id`` (C21), so the welcome notification can
prefer the device that registered.
"""

import uuid
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import MerchantStatus, UserRole
from app.v1.users.models import User
from smartretail_common.outbox import write_outbox

AGGREGATE = "user"
USER_REGISTERED = "identity.user.registered"


def user_registered(
    session: AsyncSession, user: User, *, client_id: str | None = None
) -> uuid.UUID:
    registered_at = user.created_at or datetime.now(UTC)
    return write_outbox(
        session,
        event_type=USER_REGISTERED,
        aggregate_type=AGGREGATE,
        aggregate_id=user.id,
        payload={
            "user_id": user.id,
            "role": str(user.role),
            "full_name": user.full_name,
            "registered_at": registered_at,
            "merchant_status": (
                MerchantStatus.PENDING.value if user.role == UserRole.MERCHANT else None
            ),
        },
        client_id=client_id,
    )
