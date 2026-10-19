"""Builds the merchants repo for a request."""

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_db
from app.v1.merchants.repo import MerchantRepo


def merchant_repo(session: AsyncSession = Depends(get_db)) -> MerchantRepo:
    return MerchantRepo(session)
