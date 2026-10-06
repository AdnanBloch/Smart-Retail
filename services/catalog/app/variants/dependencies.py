"""Builds the variants repo for a request."""

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_db
from app.variants.repo import VariantRepo


def variant_repo(session: AsyncSession = Depends(get_db)) -> VariantRepo:
    return VariantRepo(session)
