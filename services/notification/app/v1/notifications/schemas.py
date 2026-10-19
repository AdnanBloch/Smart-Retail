"""``/api/v1/notifications`` request and response schemas (RFC 0007 §3.1.2.1)."""

import uuid
from datetime import datetime
from typing import Annotated, Any

from pydantic import Field

from app.core.schemas import OutModel, StrictModel
from app.v1.notifications.enums import (
    DeliveryStatus,
    NotificationChannel,
    NotificationDeliveryScope,
    NotificationType,
)
from smartretail_common.enums import widen_for_response

TypeOut = Annotated[NotificationType | str, widen_for_response(NotificationType)]
ChannelOut = Annotated[NotificationChannel | str, widen_for_response(NotificationChannel)]


class NotificationOut(OutModel):
    """The owner's view: identical on every device; no payload, origin or scope."""

    id: uuid.UUID
    seq: int
    type: TypeOut
    title: str
    body: str
    created_at: datetime
    read_at: datetime | None
    delivered_at: datetime | None


class ReadOut(OutModel):
    id: uuid.UUID
    read_at: datetime | None


class ReadAllOut(OutModel):
    updated_count: int


class UnreadCountOut(OutModel):
    count: int


class AckIn(StrictModel):
    client_id: str = Field(pattern=r"^[A-Za-z0-9_-]{8,64}$")
    up_to_seq: int | None = Field(None, ge=0)
    ids: list[uuid.UUID] = Field(default_factory=list)


class AttemptOut(OutModel):
    attempt: int
    channel: ChannelOut
    status: DeliveryStatus
    error: str | None
    created_at: datetime


class AdminNotificationOut(OutModel):
    id: uuid.UUID
    seq: int
    user_id: uuid.UUID
    type: TypeOut
    channel: ChannelOut
    title: str
    body: str
    read_at: datetime | None
    correlation_id: str | None
    origin_client_id: str | None
    delivery_scope: NotificationDeliveryScope
    delivered_at: datetime | None
    delivered_client_id: str | None
    promoted_at: datetime | None
    created_at: datetime
    attempts: list[AttemptOut]
    payload: dict[str, Any] | None = None
