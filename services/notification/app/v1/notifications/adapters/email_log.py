"""``EmailLogAdapter``: renders a text email and logs it as one structured line.

``to`` is the user id — notification holds no addresses (§3.1.1.4). The fake
failure rate (``NOTIFICATION_FAKE_EMAIL_FAILURE_RATE``) exists for the retry
tests, mirroring A9's fake providers.
"""

import logging
import random
from collections.abc import Callable

from app.v1.notifications.adapters.base import AdapterResult, Failed, NotificationView, Sent
from app.v1.notifications.enums import NotificationChannel

logger = logging.getLogger("notification.adapter.email")


class EmailLogAdapter:
    channel = NotificationChannel.EMAIL

    def __init__(
        self, failure_rate: float = 0.0, rand: Callable[[], float] = random.random
    ) -> None:
        self.failure_rate = failure_rate
        self.rand = rand

    async def send(self, notification: NotificationView) -> AdapterResult:
        if self.failure_rate > 0 and self.rand() < self.failure_rate:
            return Failed("fake email provider failure", retryable=True)
        logger.info(
            "email sent",
            extra={
                "notification_id": str(notification.id),
                "to": str(notification.user_id),
                "subject": notification.title,
                "body": notification.body,
                "correlation_id": notification.correlation_id,
            },
        )
        return Sent(provider_ref=f"log-email-{notification.id}")
