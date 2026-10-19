"""Builds the notification repos for a request."""

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_db
from app.v1.notifications.delivery_report_repo import DeliveryReportRepo
from app.v1.notifications.repo import NotificationRepo


def notification_repo(session: AsyncSession = Depends(get_db)) -> NotificationRepo:
    return NotificationRepo(session)


def delivery_report_repo(session: AsyncSession = Depends(get_db)) -> DeliveryReportRepo:
    return DeliveryReportRepo(session)
