"""Builds the categories repo for a request."""

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_db
from app.v1.categories.repo import CategoryRepo


def category_repo(session: AsyncSession = Depends(get_db)) -> CategoryRepo:
    return CategoryRepo(session)
