"""``/api/v1/notifications``: list, read, read-all, unread-count, ack, admin (RFC 0007 §3.1.2.1).

Literal segments are declared before ``/{id}``. Every route but ``/admin`` is
the caller's own ``in_app`` rows (NTF-2, I7), never scope- or cursor-filtered
(I6). Mark-read and read-all publish an ``unread_count`` message so every
device's badge moves (NTF-11).
"""

import time
import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Body, Depends, Query, Request, Response
from pydantic import ValidationError

from app.core.config import Settings
from app.core.dependencies import current_principal, get_settings, require_admin
from app.core.errors import (
    ConflictingReadFilters,
    MalformedAck,
    NotificationNotFound,
    TooManyRequests,
)
from app.v1.notifications.delivery_report_repo import DeliveryReportRepo
from app.v1.notifications.dependencies import delivery_report_repo, notification_repo
from app.v1.notifications.enums import (
    NotificationChannel,
    NotificationDeliveryScope,
    NotificationType,
)
from app.v1.notifications.repo import AdminFilters, AdminRow, ListFilters, NotificationRepo
from app.v1.notifications.schemas import (
    AckIn,
    AdminNotificationOut,
    AttemptOut,
    NotificationOut,
    ReadAllOut,
    ReadOut,
    UnreadCountOut,
)
from app.v1.stream.fanout import publish, unread_count_message
from smartretail_common.auth import Principal
from smartretail_common.pagination import Page, PageParams, page_params
from smartretail_common.redis import allow

router = APIRouter()

ACK_WINDOW_SECONDS = 60


def _user(principal: Principal) -> uuid.UUID:
    return uuid.UUID(principal.subject)


async def _publish_unread(request: Request, repo: NotificationRepo, user_id: uuid.UUID) -> None:
    count = await repo.unread_count(user_id)
    await publish(request.app.state.redis, user_id, unread_count_message(count))


@router.get("", response_model=Page[NotificationOut])
async def list_notifications(
    read: bool | None = None,
    unread_only: bool = False,
    type: NotificationType | None = None,
    since_seq: int | None = Query(None, ge=0),
    page: PageParams = Depends(page_params),
    principal: Principal = Depends(current_principal),
    repo: NotificationRepo = Depends(notification_repo),
) -> Page[NotificationOut]:
    if unread_only and read is True:
        raise ConflictingReadFilters("read=true contradicts unread_only=true")
    filters = ListFilters(read=False if unread_only else read, type=type, since_seq=since_seq)
    rows = await repo.list_own(_user(principal), filters, page)
    return rows.to_page(NotificationOut.model_validate)


@router.get("/unread-count", response_model=UnreadCountOut)
async def unread_count(
    principal: Principal = Depends(current_principal),
    repo: NotificationRepo = Depends(notification_repo),
) -> UnreadCountOut:
    return UnreadCountOut(count=await repo.unread_count(_user(principal)))


@router.post("/read-all", response_model=ReadAllOut)
async def read_all(
    request: Request,
    principal: Principal = Depends(current_principal),
    repo: NotificationRepo = Depends(notification_repo),
) -> ReadAllOut:
    user_id = _user(principal)
    updated = await repo.read_all(user_id)
    if updated:
        await _publish_unread(request, repo, user_id)
    return ReadAllOut(updated_count=updated)


@router.post("/ack", status_code=204, response_class=Response)
async def ack(
    request: Request,
    raw: Any = Body(None),
    principal: Principal = Depends(current_principal),
    settings: Settings = Depends(get_settings),
    repo: DeliveryReportRepo = Depends(delivery_report_repo),
) -> Response:
    """The delivery report (C23): ``204`` for unknown, foreign and repeated ids
    alike; ``400`` only for a malformed body; ``429`` over 12/min per device."""
    try:
        body = AckIn.model_validate(raw)
    except ValidationError as exc:
        raise MalformedAck(
            "The body must carry client_id, and optionally up_to_seq >= 0 and ids",
            extras={
                "violations": [
                    {
                        "field": ".".join(str(p) for p in err["loc"]),
                        "message": err["msg"],
                        "code": err["type"],
                    }
                    for err in exc.errors()
                ]
            },
        ) from exc
    if len(body.ids) > settings.ack_max_ids:
        raise MalformedAck(f"At most {settings.ack_max_ids} ids per ack")
    user_id = _user(principal)
    window = int(time.time()) // ACK_WINDOW_SECONDS
    key = f"rl:notif-ack:{user_id}:{body.client_id}:{window}"
    if not await allow(
        request.app.state.redis, key, settings.ack_rate_limit_per_minute, ACK_WINDOW_SECONDS
    ):
        raise TooManyRequests(
            "Ack rate limit reached; drop this ack", retry_after=ACK_WINDOW_SECONDS
        )
    await repo.ack(
        user_id,
        body.client_id,
        ids=body.ids,
        up_to_seq=body.up_to_seq,
        visibility_window_seconds=settings.seq_visibility_window_seconds,
    )
    return Response(status_code=204)


def _admin_out(row: AdminRow, include_payload: bool) -> AdminNotificationOut:
    n = row.notification
    return AdminNotificationOut(
        id=n.id,
        seq=n.seq,
        user_id=n.user_id,
        type=n.type,
        channel=n.channel,
        title=n.title,
        body=n.body,
        read_at=n.read_at,
        correlation_id=n.correlation_id,
        origin_client_id=n.origin_client_id,
        delivery_scope=n.delivery_scope,
        delivered_at=n.delivered_at,
        delivered_client_id=n.delivered_client_id,
        promoted_at=n.promoted_at,
        created_at=n.created_at,
        attempts=[AttemptOut.model_validate(a) for a in row.attempts],
        payload=n.payload if include_payload else None,
    )


@router.get("/admin", response_model=Page[AdminNotificationOut])
async def admin_list(
    user_id: uuid.UUID | None = None,
    type: NotificationType | None = None,
    channel: NotificationChannel | None = None,
    delivery_scope: NotificationDeliveryScope | None = None,
    undelivered: bool | None = None,
    dispatch_status: str | None = Query(None, pattern="^(failed|none)$"),
    correlation_id: str | None = None,
    since: datetime | None = Query(None, alias="from"),
    until: datetime | None = Query(None, alias="to"),
    include: str | None = Query(None, pattern="^payload$"),
    page: PageParams = Depends(page_params),
    principal: Principal = Depends(require_admin),
    repo: NotificationRepo = Depends(notification_repo),
) -> Page[AdminNotificationOut]:
    """Every user, every channel, with attempts — "what did we record but never deliver?"."""
    filters = AdminFilters(
        user_id=user_id,
        type=type,
        channel=channel,
        delivery_scope=delivery_scope,
        undelivered=undelivered,
        dispatch_status=dispatch_status,
        correlation_id=correlation_id,
        since=since,
        until=until,
    )
    rows = await repo.admin_list(filters, page)
    return rows.to_page(lambda r: _admin_out(r, include == "payload"))


@router.post("/{notification_id}/read", response_model=ReadOut)
async def mark_read(
    notification_id: uuid.UUID,
    request: Request,
    principal: Principal = Depends(current_principal),
    repo: NotificationRepo = Depends(notification_repo),
) -> ReadOut:
    """Idempotent; another user's id is ``404``, never ``403``."""
    user_id = _user(principal)
    result = await repo.mark_read(user_id, notification_id)
    if result is None:
        raise NotificationNotFound()
    row, changed = result
    if changed:
        await _publish_unread(request, repo, user_id)
    return ReadOut(id=row.id, read_at=row.read_at)
