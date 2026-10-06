"""Every ORM model, registered on one metadata.

Alembic's ``env.py`` and the seed command import from here, so a new entity's
``models.py`` must be added to this list to become part of migrations.
"""

from app.categories.models import Category
from app.media.models import ProductMedia
from app.products.models import Product, ProductAuditLog
from app.variants.models import ProductVariant
from smartretail_common.db import Base

__all__ = ["Base", "Category", "Product", "ProductAuditLog", "ProductMedia", "ProductVariant"]
