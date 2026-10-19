"""Every ORM model, registered on one metadata.

Alembic's ``env.py`` imports from here, so a new entity's ``models.py`` must be
added to this list to become part of migrations.
"""

from app.v1.reservations.models import Reservation
from app.v1.stock.models import StockItem, StockMovement
from smartretail_common.db import Base
from smartretail_common.events.dedupe import ProcessedEvent
from smartretail_common.outbox import OutboxEvent

__all__ = ["Base", "OutboxEvent", "ProcessedEvent", "Reservation", "StockItem", "StockMovement"]
