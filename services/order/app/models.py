"""Every ORM model, registered on one metadata.

Alembic's ``env.py`` imports from here, so a new entity's ``models.py`` must be
added to this list to become part of migrations.
"""

from app.core.idempotency import IdempotencyKey
from app.orders.models import Order, OrderItem, OrderStatusHistory
from smartretail_common.db import Base
from smartretail_common.outbox import OutboxEvent

__all__ = ["Base", "IdempotencyKey", "Order", "OrderItem", "OrderStatusHistory", "OutboxEvent"]
