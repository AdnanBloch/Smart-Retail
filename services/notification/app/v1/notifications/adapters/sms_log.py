"""``SmsLogAdapter``: as the email adapter, with the body cut to 160 characters."""

import logging
import random
from collections.abc import Callable

from app.v1.notifications.adapters.base import AdapterResult, Failed, NotificationView, Sent
from app.v1.notifications.enums import NotificationChannel

logger = logging.getLogger("notification.adapter.sms")

SMS_MAX = 160


class SmsLogAdapter:
    channel = NotificationChannel.SMS

    def __init__(
        self, failure_rate: float = 0.0, rand: Callable[[], float] = random.random
    ) -> None:
        self.failure_rate = failure_rate
        self.rand = rand

    async def send(self, notification: NotificationView) -> AdapterResult:
        if self.failure_rate > 0 and self.rand() < self.failure_rate:
            return Failed("fake sms provider failure", retryable=True)
        logger.info(
            "sms sent",
            extra={
                "notification_id": str(notification.id),
                "to": str(notification.user_id),
                "body": notification.body[:SMS_MAX],
                "correlation_id": notification.correlation_id,
            },
        )
        return Sent(provider_ref=f"log-sms-{notification.id}")
