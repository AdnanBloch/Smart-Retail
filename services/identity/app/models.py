"""Every ORM model, registered on one metadata.

Alembic's ``env.py`` and the seed command import from here, so a new entity's
``models.py`` must be added to this list to become part of migrations.
"""

from app.v1.auth.models import RefreshSession
from app.v1.merchants.models import MerchantProfile
from app.v1.users.models import User
from smartretail_common.db import Base
from smartretail_common.outbox import OutboxEvent

__all__ = ["Base", "MerchantProfile", "OutboxEvent", "RefreshSession", "User"]
