"""Builds the media repo for a request."""

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_db
from app.media.repo import MediaRepo


def media_repo(session: AsyncSession = Depends(get_db)) -> MediaRepo:
    return MediaRepo(session)
