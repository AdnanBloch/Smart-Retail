"""Builds the media repo, and hands out the object store, for a request."""

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_db
from app.v1.media.repo import MediaRepo
from app.v1.media.storage import MediaStorage


def media_repo(session: AsyncSession = Depends(get_db)) -> MediaRepo:
    return MediaRepo(session)


def media_storage(request: Request) -> MediaStorage:
    storage: MediaStorage = request.app.state.storage
    return storage
