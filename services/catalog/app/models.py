"""Every ORM model, registered on one metadata.

Alembic's ``env.py`` and the seed command import from here, so a new entity's
``models.py`` must be added to this list to become part of migrations.
"""

from app.v1.categories.models import Category
from app.v1.media.models import ProductMedia
from app.v1.products.models import Product, ProductAuditLog, ProductPublishFailure
from app.v1.variants.models import ProductVariant
from smartretail_common.db import Base
from smartretail_common.outbox import OutboxEvent

__all__ = [
    "Base",
    "Category",
    "OutboxEvent",
    "Product",
    "ProductAuditLog",
    "ProductMedia",
    "ProductPublishFailure",
    "ProductVariant",
]
