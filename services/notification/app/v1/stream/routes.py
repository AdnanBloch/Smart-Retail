"""``POST /stream-ticket`` and ``GET /stream`` (C15, C19, C22). HTTP only.

Every refusal happens before the response starts, so it is a problem detail:
``401`` bad ticket, ``429`` a fourth stream, ``503`` Redis or the database
down. Order on open: redeem → claim a slot → ``SUBSCRIBE`` → cursor and
catch-up (subscribe *before* catch-up, NTF-10) → stream.
"""

import logging
import time
import uuid

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import StreamingResponse

from app.core.config import Settings
from app.core.dependencies import current_principal, get_settings
from app.core.errors import StreamTicketInvalid, TooManyStreams
from app.v1.stream import frames, tickets
from app.v1.stream.catchup import open_stream
from app.v1.stream.connection import Connection, claim_slot, conns_key, events, release_slot
from app.v1.stream.fanout import FanoutHub
from app.v1.stream.schemas import StreamTicketOut
from smartretail_common.auth import Principal
from smartretail_common.client_id import client_id as request_client_id
from smartretail_common.db import Database
from smartretail_common.errors import DependencyUnavailable

logger = logging.getLogger("notification.stream")

router = APIRouter()

SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}


@router.post("/stream-ticket", response_model=StreamTicketOut)
async def stream_ticket(
    request: Request,
    principal: Principal = Depends(current_principal),
    settings: Settings = Depends(get_settings),
) -> StreamTicketOut:
    """Exchanges the bearer token for a 60-second single-use ticket bound to
    ``X-Client-Id`` (absent or malformed → an ephemeral stream, NTF-12)."""
    ticket = tickets.Ticket(
        user_id=principal.subject,
        client_id=request_client_id(request),
        jwt_exp=principal.expires_at,
    )
    try:
        token = await tickets.mint(
            request.app.state.redis, ticket, ttl_seconds=settings.stream_ticket_ttl_seconds
        )
    except Exception as exc:
        raise DependencyUnavailable("Streams are unavailable; poll unread-count") from exc
    return StreamTicketOut(ticket=token, expires_in=settings.stream_ticket_ttl_seconds)


def _last_event_id(raw: str | None) -> int | None:
    """A hint: a non-integer or negative value is ignored, never an error."""
    if raw is None:
        return None
    try:
        value = int(raw.strip())
    except ValueError:
        return None
    return value if value >= 0 else None


@router.get(
    "/stream",
    response_class=StreamingResponse,
    responses={200: {"content": {"text/event-stream": {}}}},
)
async def stream(
    request: Request,
    ticket: str = Query(..., description="From POST /stream-ticket; single use"),
    last_event_id: str | None = Header(None, alias="Last-Event-ID"),
    settings: Settings = Depends(get_settings),
) -> StreamingResponse:
    redis = request.app.state.redis
    hub: FanoutHub = request.app.state.hub
    db: Database = request.app.state.db
    try:
        redeemed = await tickets.redeem(redis, ticket)
    except Exception as exc:
        raise DependencyUnavailable("Streams are unavailable") from exc
    if redeemed is None:
        raise StreamTicketInvalid("The ticket is unknown, expired or already used")
    if redeemed.jwt_exp <= time.time():
        raise StreamTicketInvalid("The token behind this ticket has expired")
    user_id = redeemed.user_id

    try:
        claimed = await claim_slot(
            redis,
            user_id,
            limit=settings.stream_max_per_user,
            ttl_seconds=settings.stream_conn_count_ttl_seconds,
        )
    except Exception as exc:
        raise DependencyUnavailable("Streams are unavailable") from exc
    if not claimed:
        raise TooManyStreams(settings.stream_max_per_user)

    connection = Connection(user_id, redeemed.client_id, queue_max=settings.stream_queue_max)
    retry_ms = frames.retry_hint(settings.stream_retry_base_ms, settings.stream_retry_jitter_ms)
    subscribed = False
    try:
        try:
            await hub.subscribe(user_id, connection)
            subscribed = True
        except Exception as exc:
            raise DependencyUnavailable("Streams are unavailable") from exc
        async with db.sessionmaker() as session:
            opening = await open_stream(
                session,
                settings,
                user_id=uuid.UUID(user_id),
                client_id=redeemed.client_id,
                last_event_id=_last_event_id(last_event_id),
                user_agent=request.headers.get("user-agent"),
                retry_ms=retry_ms,
            )
    except BaseException:
        if subscribed:
            await hub.unsubscribe(user_id, connection)
        await release_slot(redis, user_id)
        raise
    logger.info(
        "stream opened",
        extra={"client_id": redeemed.client_id, "mode": str(opening.mode)},
    )

    async def heartbeat() -> None:
        await redis.expire(conns_key(user_id), settings.stream_conn_count_ttl_seconds)

    async def cleanup() -> None:
        await hub.unsubscribe(user_id, connection)
        await release_slot(redis, user_id)
        logger.info("stream closed", extra={"client_id": redeemed.client_id})

    return StreamingResponse(
        events(
            connection,
            opening.frames,
            opening.sent_ids,
            deadline=float(redeemed.jwt_exp),
            heartbeat_seconds=settings.stream_heartbeat_seconds,
            retry_ms=retry_ms,
            on_heartbeat=heartbeat,
            cleanup=cleanup,
        ),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )
