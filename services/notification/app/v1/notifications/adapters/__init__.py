"""The adapter registry: one line per channel. ``push`` is not registered (NTF-D27)."""

from app.core.config import Settings
from app.v1.notifications.adapters.base import NotificationAdapter
from app.v1.notifications.adapters.email_log import EmailLogAdapter
from app.v1.notifications.adapters.sms_log import SmsLogAdapter
from app.v1.notifications.enums import NotificationChannel


def registry(settings: Settings) -> dict[NotificationChannel, NotificationAdapter]:
    return {
        NotificationChannel.EMAIL: EmailLogAdapter(settings.notification_fake_email_failure_rate),
        NotificationChannel.SMS: SmsLogAdapter(settings.notification_fake_sms_failure_rate),
    }
