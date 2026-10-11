"""Builds the products repos for a request."""

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_db
from app.products.listing_repo import ListingRepo
from app.products.publishing_repo import PublishingRepo
from app.products.repo import ProductRepo


def product_repo(session: AsyncSession = Depends(get_db)) -> ProductRepo:
    return ProductRepo(session)


def listing_repo(session: AsyncSession = Depends(get_db)) -> ListingRepo:
    return ListingRepo(session)


def publishing_repo(request: Request, session: AsyncSession = Depends(get_db)) -> PublishingRepo:
    return PublishingRepo(session, request.app.state.publish_starter)
