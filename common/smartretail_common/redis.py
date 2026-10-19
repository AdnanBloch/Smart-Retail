"""Redis access (RFC 0001 §3.1.1.8): fast, disposable, never the source of truth.

Only the mechanics live here; every key is named by its owning service
(R8), as RFC 0001 §3.1.3.2 tabulates. ``redis`` is the ``redis`` extra.
"""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger = logging.getLogger("smartretail.redis")


def create_redis(url: str) -> "Redis":
    """A client with short timeouts: a slow Redis must not slow a request down."""
    from redis.asyncio import Redis

    client: Redis = Redis.from_url(
        url, decode_responses=True, socket_timeout=2.0, socket_connect_timeout=2.0
    )
    return client


async def allow(redis: "Redis", key: str, limit: int, window_seconds: int) -> bool:
    """Fixed-window rate limit: True while ``key`` has been hit at most ``limit``
    times in the current window. Fails **open** — a Redis outage never blocks a
    caller (RFC 0001 §3.1.1.8: rate limits have no source of truth to fall back to).
    The caller puts the window into ``key`` (``rl:{scope}:{id}:{window_start}``)."""
    try:
        pipe = redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, window_seconds, nx=True)
        count, _ = await pipe.execute()
    except Exception:  # any Redis failure: allow
        logger.warning("rate limit unavailable; allowing", extra={"key": key})
        return True
    return int(count) <= limit
