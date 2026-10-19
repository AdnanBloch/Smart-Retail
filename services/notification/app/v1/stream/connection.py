"""One SSE connection: its slot, its bounded queue, and its frame loop (NTF-D23).

* **Slot.** ``nt:conns:{user_id}`` counts a user's streams across replicas;
  the fourth is refused (``429``). The heartbeat refreshes its TTL, so a
  crashed replica's slots expire on their own.
* **Queue.** The hub offers live messages; on overflow the connection is
  closed rather than buffered — lossless, because reconnect catches up by
  ``seq``.
* **Loop.** Opening frames, then live messages (skipping ids the catch-up
  already sent, NTF-10), a keepalive comment and ``ping`` every 20 s, and an
  ``error`` frame at the JWT's expiry or on overflow.
"""

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, Any

import anyio

from app.v1.stream import frames
from app.v1.stream.fanout import KIND_NOTIFICATION, KIND_UNREAD_COUNT

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger = logging.getLogger("notification.stream")

CLOSE = "_close"
REASON_SLOW = "slow_consumer"


def conns_key(user_id: object) -> str:
    return f"nt:conns:{user_id}"


async def claim_slot(redis: "Redis", user_id: object, *, limit: int, ttl_seconds: int) -> bool:
    pipe = redis.pipeline()
    pipe.incr(conns_key(user_id))
    pipe.expire(conns_key(user_id), ttl_seconds)
    count, _ = await pipe.execute()
    if int(count) > limit:
        await redis.decr(conns_key(user_id))
        return False
    return True


async def release_slot(redis: "Redis", user_id: object) -> None:
    try:
        if int(await redis.decr(conns_key(user_id))) <= 0:
            await redis.delete(conns_key(user_id))
    except Exception as exc:  # the TTL reclaims it
        logger.warning("slot release failed", extra={"error": repr(exc)})


class Connection:
    """A local stream as the fan-out hub sees it."""

    def __init__(self, user_id: str, client_id: str | None, *, queue_max: int) -> None:
        self.user_id = user_id
        self.client_id = client_id
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=queue_max + 1)
        self.queue_max = queue_max
        self.closed: str | None = None

    def offer(self, message: dict[str, Any]) -> None:
        if self.closed is not None:
            return
        if self.queue.qsize() >= self.queue_max:
            logger.warning("slow consumer; closing stream", extra={"client_id": self.client_id})
            self.abort(REASON_SLOW)
            return
        self.queue.put_nowait(message)

    def abort(self, reason: str) -> None:
        """Ends the stream: drop what is queued (reconnect replays it) and wake the loop."""
        if self.closed is not None:
            return
        self.closed = reason
        while not self.queue.empty():
            self.queue.get_nowait()
        self.queue.put_nowait({"kind": CLOSE, "reason": reason})


def render_live(message: dict[str, Any], client_id: str | None, sent_ids: set[str]) -> list[str]:
    """Pure: the frames one live message becomes on this connection."""
    kind = message.get("kind")
    if kind == KIND_NOTIFICATION:
        out = []
        if str(message.get("id")) not in sent_ids:
            out.append(
                frames.notification(
                    id=str(message["id"]),
                    seq=int(message["seq"]),
                    type=str(message["type"]),
                    title=str(message["title"]),
                    body=str(message["body"]),
                    created_at=str(message["created_at"]),
                    origin=client_id is not None and message.get("origin_client_id") == client_id,
                )
            )
        if message.get("unread_count") is not None:
            out.append(frames.unread_count(int(message["unread_count"])))
        return out
    if kind == KIND_UNREAD_COUNT and message.get("count") is not None:
        return [frames.unread_count(int(message["count"]))]
    return []


async def events(
    connection: Connection,
    opening: list[str],
    sent_ids: set[str],
    *,
    deadline: float,
    heartbeat_seconds: float,
    retry_ms: int,
    on_heartbeat: Callable[[], Awaitable[None]],
    cleanup: Callable[[], Awaitable[None]],
    clock: Callable[[], float] = time.time,
) -> AsyncIterator[str]:
    try:
        for chunk in opening:
            yield chunk
        while True:
            remaining = deadline - clock()
            if remaining <= 0:
                yield frames.token_expired()
                return
            try:
                message = await asyncio.wait_for(
                    connection.queue.get(), min(heartbeat_seconds, remaining)
                )
            except TimeoutError:
                if deadline - clock() <= 0:
                    continue
                yield frames.comment("keepalive")
                yield frames.ping()
                with contextlib.suppress(Exception):
                    await on_heartbeat()
                continue
            if message.get("kind") == CLOSE:
                if message.get("reason") == REASON_SLOW:
                    yield frames.slow_consumer()
                else:  # shutdown or a lost fan-out connection: reconnect soon
                    yield f"retry: {retry_ms}\n\n"
                return
            for chunk in render_live(message, connection.client_id, sent_ids):
                yield chunk
    finally:
        with anyio.CancelScope(shield=True):
            await cleanup()
