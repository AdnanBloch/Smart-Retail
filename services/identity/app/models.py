"""Every ORM model, registered on one metadata.

Alembic's ``env.py`` and the seed command import from here, so a new entity's
``models.py`` must be added to this list to become part of migrations.
"""

from app.auth.models import RefreshSession
from app.merchants.models import MerchantProfile
from app.users.models import User
from smartretail_common.db import Base

__all__ = ["Base", "MerchantProfile", "RefreshSession", "User"]
