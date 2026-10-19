"""Builds the reservation repo for a request."""

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.db import get_db
from app.core.dependencies import get_settings
from app.v1.reservations.repo import ReservationRepo


def reservation_repo(
    session: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings)
) -> ReservationRepo:
    return ReservationRepo(session, settings)
