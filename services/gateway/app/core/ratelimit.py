"""Edge rate limiting (RFC 0002 GW-7, GW-D11 to GW-D13, GW-21).

A fixed-window counter in Redis, shared by every gateway replica:

    MULTI; SET rl:{scope}:{id} 0 EX w NX; INCR; PTTL; EXEC

The key's TTL *is* the window, so keys need no timestamp and expire on their
own. Rejected requests still count, so hammering does not shorten the wait.
Per-IP classes run before authentication, per-subject classes after.

A Redis error, or a reply slower than ``gateway_ratelimit_redis_timeout_ms``,
**fails open**: a limiter outage must not become a platform outage (GW-D12).
It is logged as ``failed_open`` so the gap is visible.

Not built yet: ``login_email`` (GW-8), which needs the login body read before
forwarding; ``login_ip`` still bounds login attempts per address.
"""

import asyncio
import logging
import math
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from app.core.config import GatewaySettings
from smartretail_common.errors import DomainError

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger = logging.getLogger("gateway.ratelimit")


class RateFamily(StrEnum):
    """What a route rule names; resolved to classes by caller kind (RFC 0002 rate-limit classes)."""

    LOGIN = "login"
    ORDER_CREATE = "order_create"
    SEARCH = "search"
    CHAT = "chat"
    GENERATE = "generate"
    NOTIF_ACK = "notif_ack"


@dataclass(frozen=True)
class RateClass:
    scope: str
    limit: int
    window_seconds: int


@dataclass(frozen=True)
class Decision:
    rate: RateClass
    count: int
    reset_seconds: int

    @property
    def allowed(self) -> bool:
        return self.count <= self.rate.limit

    @property
    def remaining(self) -> int:
        return max(self.rate.limit - self.count, 0)

    def headers(self) -> dict[str, str]:
        return {
            "RateLimit-Limit": str(self.rate.limit),
            "RateLimit-Remaining": str(self.remaining),
            "RateLimit-Reset": str(self.reset_seconds),
        }


class RateLimited(DomainError):
    status = 429
    type_slug = "rate-limited"
    title = "Too many requests"

    def __init__(self, decision: Decision) -> None:
        rate = decision.rate
        super().__init__(
            f"Limit of {rate.limit} requests per {rate.window_seconds} seconds exceeded "
            f"for this route. Retry after {decision.reset_seconds} seconds.",
            # `scope` names the class, never the identifier (no IP, no subject).
            extras={
                "scope": rate.scope,
                "limit": rate.limit,
                "window_seconds": rate.window_seconds,
                "retry_after": decision.reset_seconds,
            },
            headers={"Retry-After": str(decision.reset_seconds), **decision.headers()},
        )


def classes(settings: GatewaySettings) -> dict[str, RateClass]:
    s = settings
    return {
        c.scope: c
        for c in (
            RateClass("default", s.rl_default_per_ip_per_min, 60),
            RateClass("login_ip", s.rl_login_per_ip_per_min, 60),
            RateClass("order_create", s.rl_order_create_per_user_per_min, 60),
            RateClass("search_anon", s.rl_search_anon_per_ip_per_min, 60),
            RateClass("search_user", s.rl_search_per_user_per_min, 60),
            RateClass("chat_anon", s.rl_chat_anon_per_ip_per_min, 60),
            RateClass("chat_user", s.rl_chat_per_user_per_min, 60),
            RateClass("generate", s.rl_generate_per_merchant_per_hour, 3600),
            RateClass("notif_ack", s.rl_notif_ack_per_client_per_min, 60),
        )
    }


# Family → (class for an anonymous caller, by IP; class for a verified caller, by subject).
_FAMILIES: dict[RateFamily, tuple[str | None, str | None]] = {
    RateFamily.LOGIN: ("login_ip", None),
    RateFamily.ORDER_CREATE: (None, "order_create"),
    RateFamily.SEARCH: ("search_anon", "search_user"),
    RateFamily.CHAT: ("chat_anon", "chat_user"),
    RateFamily.GENERATE: (None, "generate"),
    RateFamily.NOTIF_ACK: (None, "notif_ack"),
}


def create_limiter_redis(settings: GatewaySettings) -> "Redis":
    """A bounded pool, so a burst waits briefly for a warm connection.

    Opening a connection costs tens of milliseconds (DNS and TCP), nearly the
    whole 50 ms budget, while a counter on a warm connection takes under one.
    With an unbounded pool a burst opens one new connection per concurrent
    request and every one of them fails open; a bounded, pre-warmed pool
    (``RateLimiter.warm``) makes them queue for a connection instead.
    """
    from redis.asyncio import BlockingConnectionPool, Redis

    pool = BlockingConnectionPool.from_url(
        settings.redis_url,
        max_connections=settings.gateway_ratelimit_redis_pool_size,
        timeout=settings.gateway_ratelimit_redis_timeout_ms / 1000,
        socket_timeout=2.0,
        socket_connect_timeout=2.0,
        decode_responses=True,
    )
    return Redis(connection_pool=pool)


class RateLimiter:
    def __init__(self, settings: GatewaySettings, redis: "Redis | None") -> None:
        self.enabled = settings.gateway_ratelimit_enabled and redis is not None
        self.redis = redis
        self.timeout = settings.gateway_ratelimit_redis_timeout_ms / 1000
        self.pool_size = settings.gateway_ratelimit_redis_pool_size
        self.classes = classes(settings)

    async def warm(self) -> None:
        """Open the pool's connections at start-up, outside any request budget.
        A Redis that is down here is not an error: limits fail open until it is back."""
        if not self.enabled or self.redis is None:
            return
        redis = self.redis
        results = await asyncio.gather(
            *(redis.ping() for _ in range(self.pool_size)), return_exceptions=True
        )
        if any(isinstance(r, BaseException) for r in results):
            logger.warning("rate limit redis unavailable at start-up; failing open")

    async def hit(self, scope: str, identifier: str) -> Decision | None:
        """Count one request; ``None`` when the limiter is off or failed open."""
        if not self.enabled or self.redis is None:
            return None
        rate = self.classes[scope]
        key = f"rl:{scope}:{identifier}"
        try:
            async with asyncio.timeout(self.timeout):
                pipe = self.redis.pipeline(transaction=True)
                pipe.set(key, 0, ex=rate.window_seconds, nx=True)
                pipe.incr(key)
                pipe.pttl(key)
                _, count, pttl = await pipe.execute()
        except Exception:  # any Redis failure or timeout: allow (GW-D12)
            logger.warning("rate limit failed_open", extra={"scope": scope, "failed_open": True})
            return None
        reset = math.ceil(max(int(pttl), 0) / 1000) or rate.window_seconds
        return Decision(rate, int(count), reset)

    async def before_auth(
        self, client_ip: str, family: RateFamily | None, *, has_token: bool, exempt: bool
    ) -> list[Decision]:
        """Stage 5: ``default`` and the anonymous class of the route's family."""
        scopes = [] if exempt else ["default"]
        anon = _FAMILIES[family][0] if family is not None else None
        if anon is not None and (family == RateFamily.LOGIN or not has_token):
            scopes.append(anon)
        return await self._apply(scopes, client_ip)

    async def after_auth(
        self, subject: str | None, family: RateFamily | None, *, client_id: str | None = None
    ) -> list[Decision]:
        """Stage 8: the per-subject class of the route's family."""
        if family is None or subject is None:
            return []
        scope = _FAMILIES[family][1]
        if scope is None:
            return []
        identifier = (
            f"{subject}:{client_id}" if family == RateFamily.NOTIF_ACK and client_id else subject
        )
        return await self._apply([scope], identifier)

    async def _apply(self, scopes: list[str], identifier: str) -> list[Decision]:
        decisions = []
        for scope in scopes:
            decision = await self.hit(scope, identifier)
            if decision is None:
                continue
            if not decision.allowed:
                raise RateLimited(decision)
            decisions.append(decision)
        return decisions


def tightest(decisions: list[Decision]) -> Decision | None:
    """GW-21: advertise the class closest to its limit."""
    return min(decisions, key=lambda d: (d.remaining, d.rate.limit), default=None)
