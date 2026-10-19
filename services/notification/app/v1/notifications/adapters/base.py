"""The channel adapter interface (RFC 0007 §3.1.1.4, Channel adapters).

A real provider is an adapter swap. ``in_app`` has no adapter — the stream
delivers it (NTF-4); ``push`` has none registered yet (NTF-D27).
"""

import uuid
from dataclasses import dataclass
from typing import Protocol

from app.v1.notifications.enums import NotificationChannel


@dataclass(frozen=True)
class NotificationView:
    """What an adapter is given: the rendered row, never the source envelope."""

    id: uuid.UUID
    user_id: uuid.UUID
    channel: str
    type: str
    title: str
    body: str
    correlation_id: str | None


@dataclass(frozen=True)
class Sent:
    provider_ref: str | None = None


@dataclass(frozen=True)
class Failed:
    error: str
    retryable: bool = True


AdapterResult = Sent | Failed


class NotificationAdapter(Protocol):
    channel: NotificationChannel

    async def send(self, notification: NotificationView) -> AdapterResult: ...
