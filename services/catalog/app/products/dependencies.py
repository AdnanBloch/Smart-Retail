"""Builds the products repos for a request."""

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_db
from app.products.listing_repo import ListingRepo
from app.products.repo import ProductRepo


def product_repo(session: AsyncSession = Depends(get_db)) -> ProductRepo:
    return ProductRepo(session)


def listing_repo(session: AsyncSession = Depends(get_db)) -> ListingRepo:
    return ListingRepo(session)
