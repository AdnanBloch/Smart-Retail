"""Builds the stock repo for a request."""

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.db import get_db
from app.core.dependencies import get_settings
from app.stock.repo import StockRepo


def stock_repo(
    session: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings)
) -> StockRepo:
    return StockRepo(session, settings)
