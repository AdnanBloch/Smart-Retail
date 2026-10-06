from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.db import get_db
from app.models.enums import CallerRole
from app.services.category_repo import CategoryRepo
from app.services.listing_repo import ListingRepo
from app.services.product_repo import ProductRepo
from app.services.variant_repo import VariantRepo
from smartretail_common.auth import optional_principal, require_roles

__all__ = ["optional_principal"]

require_admin = require_roles(CallerRole.ADMIN)
require_merchant = require_roles(CallerRole.MERCHANT)
require_catalog_writer = require_roles(CallerRole.MERCHANT, CallerRole.ADMIN)


def category_repo(session: AsyncSession = Depends(get_db)) -> CategoryRepo:
    return CategoryRepo(session)


def product_repo(session: AsyncSession = Depends(get_db)) -> ProductRepo:
    return ProductRepo(session)


def listing_repo(session: AsyncSession = Depends(get_db)) -> ListingRepo:
    return ListingRepo(session)


def variant_repo(session: AsyncSession = Depends(get_db)) -> VariantRepo:
    return VariantRepo(session)
