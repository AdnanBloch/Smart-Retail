"""Every ORM model, registered on one metadata.

Alembic's ``env.py`` imports from here, so a new entity's ``models.py`` must be
added to this list to become part of migrations. ``notification`` produces no
events, so there is no ``outbox`` (RFC 0007 §3.1.2.3).
"""

from app.v1.notifications.models import DeliveryAttempt, Notification
from app.v1.stream.models import StreamCursor
from app.v1.subscriptions.models import PushSubscription
from smartretail_common.db import Base
from smartretail_common.events.dedupe import ProcessedEvent

__all__ = [
    "Base",
    "DeliveryAttempt",
    "Notification",
    "ProcessedEvent",
    "PushSubscription",
    "StreamCursor",
]
