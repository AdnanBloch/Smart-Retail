"""Builds the users repo for a request."""

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.db import get_db
from app.core.dependencies import get_hasher, get_settings
from app.core.security import PasswordHasher
from app.v1.users.repo import UserRepo


def user_repo(
    session: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
    hasher: PasswordHasher = Depends(get_hasher),
) -> UserRepo:
    return UserRepo(session, settings, hasher)
