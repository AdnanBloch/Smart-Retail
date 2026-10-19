"""Fan-out between replicas over Redis Pub/Sub (C17, NTF-10, NTF-11).

Publishing: the consumer (and mark-read / read-all) ``PUBLISH``es a JSON
message on ``nt:user:{user_id}`` after its transaction commits. At-most-once
is enough: the row is already durable (C16).

Subscribing: one ``FanoutHub`` per process holds **one** Redis ``PubSub``
connection. The first local stream of a user ``SUBSCRIBE``s the user's
channel; the last one to close ``UNSUBSCRIBE``s it (NTF-D35). A single reader
task routes each message to that user's local connections' queues.
"""

import asyncio
import contextlib
import json
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from redis.asyncio.client import PubSub

logger = logging.getLogger("notification.fanout")

KIND_NOTIFICATION = "notification"
KIND_UNREAD_COUNT = "unread_count"


def user_channel(user_id: object) -> str:
    return f"nt:user:{user_id}"


def notification_message(
    *,
    id: str,
    seq: int,
    type: str,
    title: str,
    body: str,
    created_at: str,
    origin_client_id: str | None,
    scope: str,
    unread_count: int,
) -> dict[str, Any]:
    return {
        "kind": KIND_NOTIFICATION,
        "id": id,
        "seq": seq,
        "type": type,
        "title": title,
        "body": body,
        "created_at": created_at,
        "origin_client_id": origin_client_id,
        "scope": scope,
        "unread_count": unread_count,
    }


def unread_count_message(count: int) -> dict[str, Any]:
    return {"kind": KIND_UNREAD_COUNT, "count": count}


async def publish(redis: "Redis", user_id: object, message: Mapping[str, Any]) -> bool:
    """Best effort: a failure is logged and swallowed (C16) — the next connect replays."""
    try:
        await redis.publish(user_channel(user_id), json.dumps(message, default=str))
    except Exception as exc:
        logger.warning(
            "fan-out publish failed", extra={"user_id": str(user_id), "error": repr(exc)}
        )
        return False
    return True


class Listener(Protocol):
    def offer(self, message: dict[str, Any]) -> None: ...

    def abort(self, reason: str) -> None: ...


class FanoutHub:
    """One ``PubSub`` per process; ``nt:user:{id}`` subscribed while a local stream needs it."""

    def __init__(self, redis: "Redis", *, subscribe_timeout: float = 2.0) -> None:
        self.redis = redis
        self.subscribe_timeout = subscribe_timeout
        self._pubsub: PubSub | None = None
        self._listeners: dict[str, set[Listener]] = {}
        self._confirmed: dict[str, asyncio.Event] = {}
        self._reader: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    @property
    def channels(self) -> set[str]:
        return set(self._listeners)

    def connections(self) -> int:
        return sum(len(v) for v in self._listeners.values())

    async def subscribe(self, user_id: object, listener: Listener) -> None:
        """Returns once Redis has confirmed the subscription, so a ``PUBLISH``
        made after this returns reaches ``listener`` (NTF-10)."""
        channel = user_channel(user_id)
        async with self._lock:
            listeners = self._listeners.setdefault(channel, set())
            listeners.add(listener)
            if len(listeners) > 1:
                return
            confirmed = self._confirmed[channel] = asyncio.Event()
            try:
                if self._pubsub is None:
                    self._pubsub = self.redis.pubsub()
                await self._pubsub.subscribe(channel)
                if self._reader is None or self._reader.done():
                    self._reader = asyncio.create_task(self._read(), name="fanout-reader")
            except BaseException:
                self._drop(channel, listener)
                raise
        try:
            await asyncio.wait_for(confirmed.wait(), self.subscribe_timeout)
        except BaseException:
            await self.unsubscribe(user_id, listener)
            raise

    async def unsubscribe(self, user_id: object, listener: Listener) -> None:
        channel = user_channel(user_id)
        async with self._lock:
            if not self._drop(channel, listener) or self._pubsub is None:
                return
            try:
                await self._pubsub.unsubscribe(channel)
            except Exception as exc:  # the reader resets the connection
                logger.warning("unsubscribe failed", extra={"error": repr(exc)})

    def _drop(self, channel: str, listener: Listener) -> bool:
        """Removes ``listener``; True if that was the channel's last one."""
        listeners = self._listeners.get(channel)
        if listeners is None:
            return False
        listeners.discard(listener)
        if listeners:
            return False
        del self._listeners[channel]
        self._confirmed.pop(channel, None)
        return True

    async def _read(self) -> None:
        assert self._pubsub is not None
        pubsub = self._pubsub
        try:
            while True:
                message = await pubsub.get_message(timeout=1.0)
                if message is None:
                    continue
                self._route(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Redis went away: every local stream closes and its client
            # reconnects and catches up by seq — lossless (C16, C18).
            logger.warning(
                "fan-out reader failed; closing local streams", extra={"error": repr(exc)}
            )
            await self._reset("fan-out connection lost")

    def _route(self, message: Mapping[str, Any]) -> None:
        channel = message.get("channel")
        if not isinstance(channel, str):
            return
        kind = message.get("type")
        if kind == "subscribe":
            event = self._confirmed.get(channel)
            if event is not None:
                event.set()
            return
        if kind != "message":
            return
        try:
            data = json.loads(message["data"])
        except (TypeError, ValueError):
            logger.warning("malformed fan-out message", extra={"channel": channel})
            return
        if not isinstance(data, dict):
            return
        for listener in list(self._listeners.get(channel, ())):
            listener.offer(data)

    async def _reset(self, reason: str) -> None:
        async with self._lock:
            listeners = [lst for group in self._listeners.values() for lst in group]
            self._listeners.clear()
            self._confirmed.clear()
            pubsub, self._pubsub = self._pubsub, None
            self._reader = None
        for listener in listeners:
            listener.abort(reason)
        if pubsub is not None:
            with contextlib.suppress(Exception):
                await pubsub.aclose()

    async def close(self) -> None:
        """Shutdown: stop reading and end every local stream with a retry hint."""
        reader = self._reader
        if reader is not None:
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await reader
        await self._reset("shutting down")
