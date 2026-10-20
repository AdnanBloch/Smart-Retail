"""GW-7 / GW-T13: fixed-window rate limits against a real Redis.

Threshold, isolation, recovery, headers, the stream exemption and fail-open.
Uses the test Redis (`make test-db`, port 56379, db 15) and skips without it.
"""

import asyncio
import os
import uuid

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from app.core.config import GatewaySettings
from app.core.ratelimit import (
    Decision,
    RateClass,
    RateLimiter,
    create_limiter_redis,
    tightest,
)
from main import create_application
from smartretail_common.auth import JwtVerifier
from tests.conftest import KEY, problem, token

REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://127.0.0.1:56379/15")


@pytest.fixture
async def redis():
    from redis.asyncio import Redis

    client = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        await client.ping()
    except Exception:
        await client.aclose()
        pytest.skip("test Redis not reachable; run `make test-db`")
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


def limited_settings(**overrides) -> GatewaySettings:
    values = {
        "jwt_public_key_path": None,
        "redis_url": REDIS_URL,
        "gateway_ratelimit_redis_timeout_ms": 500,
        "rl_default_per_ip_per_min": 1000,
        "rl_order_create_per_user_per_min": 3,
        "rl_login_per_ip_per_min": 2,
        "rl_search_anon_per_ip_per_min": 2,
        "rl_search_per_user_per_min": 4,
        "rl_notif_ack_per_client_per_min": 1,
    }
    return GatewaySettings(**{**values, **overrides})


@pytest.fixture
async def make_client(upstreams):
    apps = []

    async def build(settings: GatewaySettings) -> AsyncClient:
        verifier = JwtVerifier(
            issuer=settings.jwt_issuer,
            audience=settings.jwt_audience,
            static_keys={None: KEY.public_key},
        )
        app = create_application(
            settings,
            verifier=verifier,
            transports={n: httpx.MockTransport(u) for n, u in upstreams.items()},
        )
        apps.append(app)
        return AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://gateway",
        )

    yield build
    for app in apps:
        for c in app.state.clients.values():
            await c.aclose()
        if app.state.limiter.redis is not None:
            await app.state.limiter.redis.aclose(close_connection_pool=True)


def customer(sub: str | None = None) -> dict[str, str]:
    return token("customer", sub=sub or str(uuid.uuid4()))


async def test_order_create_allows_the_limit_then_429s(redis, make_client, upstreams) -> None:
    client = await make_client(limited_settings())
    headers = customer()
    ok = [await client.post("/api/v1/orders", json={}, headers=headers) for _ in range(3)]
    assert [r.status_code for r in ok] == [200] * 3
    assert [r.headers["ratelimit-remaining"] for r in ok] == ["2", "1", "0"]
    assert ok[0].headers["ratelimit-limit"] == "3"

    r = await client.post("/api/v1/orders", json={}, headers=headers)
    assert r.status_code == 429
    body = problem(r)
    assert body["type"].endswith("/rate-limited") and body["scope"] == "order_create"
    assert body["limit"] == 3 and body["window_seconds"] == 60
    assert 0 < int(r.headers["retry-after"]) <= 60 == body["window_seconds"]
    assert r.headers["ratelimit-remaining"] == "0"
    # The upstream never saw the rejected request.
    assert len(upstreams["order"].requests) == 3


async def test_subjects_are_counted_independently(redis, make_client) -> None:
    client = await make_client(limited_settings())
    a, b = customer(), customer()
    for _ in range(3):
        assert (await client.post("/api/v1/orders", json={}, headers=a)).status_code == 200
    assert (await client.post("/api/v1/orders", json={}, headers=a)).status_code == 429
    assert (await client.post("/api/v1/orders", json={}, headers=b)).status_code == 200


async def test_a_new_window_resets_the_count(redis, make_client) -> None:
    client = await make_client(limited_settings())
    sub = str(uuid.uuid4())
    for _ in range(3):
        await client.post("/api/v1/orders", json={}, headers=customer(sub))
    assert (await client.post("/api/v1/orders", json={}, headers=customer(sub))).status_code == 429
    # Rejected requests still count, so the key carries the whole window...
    assert int(await redis.get(f"rl:order_create:{sub}")) == 4
    await redis.pexpire(f"rl:order_create:{sub}", 1)  # ...until it expires.
    await asyncio.sleep(0.05)
    assert (await client.post("/api/v1/orders", json={}, headers=customer(sub))).status_code == 200


async def test_concurrent_burst_admits_exactly_the_limit(redis, make_client) -> None:
    """The mentor's burst: 20 orders at once from one user, 3 get through."""
    client = await make_client(limited_settings())
    headers = customer()
    results = await asyncio.gather(
        *(client.post("/api/v1/orders", json={}, headers=headers) for _ in range(20))
    )
    statuses = sorted(r.status_code for r in results)
    assert statuses == [200] * 3 + [429] * 17


async def test_other_order_routes_are_not_order_create_limited(redis, make_client) -> None:
    client = await make_client(limited_settings())
    headers = customer()
    for _ in range(6):
        assert (await client.get("/api/v1/orders", headers=headers)).status_code == 200


async def test_default_per_ip_class_and_auth_order(redis, make_client) -> None:
    client = await make_client(limited_settings(rl_default_per_ip_per_min=2))
    assert (await client.get("/api/v1/products")).status_code == 200
    assert (await client.get("/api/v1/products")).status_code == 200
    r = await client.get("/api/v1/products")
    assert r.status_code == 429 and problem(r)["scope"] == "default"
    # Per-IP runs before auth: a bad token is not even looked at.
    bad = await client.get("/api/v1/users/me", headers={"Authorization": "Bearer junk"})
    assert bad.status_code == 429


async def test_the_notification_stream_is_exempt(redis, make_client) -> None:
    client = await make_client(limited_settings(rl_default_per_ip_per_min=1))
    for _ in range(3):
        assert (await client.get("/api/v1/notifications/stream")).status_code == 200


async def test_login_is_limited_per_ip(redis, make_client) -> None:
    client = await make_client(limited_settings())
    codes = [(await client.post("/api/v1/auth/login", json={})).status_code for _ in range(3)]
    assert codes == [200, 200, 429]


async def test_search_uses_the_anon_or_the_user_class(redis, make_client) -> None:
    client = await make_client(limited_settings())
    anon = [(await client.post("/api/v1/search/semantic", json={})).status_code for _ in range(3)]
    assert anon == [200, 200, 429]
    user = customer()
    signed = [
        (await client.post("/api/v1/search/semantic", json={}, headers=user)).status_code
        for _ in range(5)
    ]
    assert signed == [200] * 4 + [429]


async def test_notif_ack_counts_per_subject_and_client_id(redis, make_client) -> None:
    client = await make_client(limited_settings())
    sub = str(uuid.uuid4())
    tab_a = {**customer(sub), "X-Client-Id": "tab-aaaaaaaa"}
    tab_b = {**customer(sub), "X-Client-Id": "tab-bbbbbbbb"}
    assert (await client.post("/api/v1/notifications/ack", json={}, headers=tab_a)).is_success
    assert (await client.post("/api/v1/notifications/ack", json={}, headers=tab_b)).is_success
    r = await client.post("/api/v1/notifications/ack", json={}, headers=tab_a)
    assert r.status_code == 429


async def test_redis_down_fails_open(make_client, capsys) -> None:
    client = await make_client(
        limited_settings(
            redis_url="redis://127.0.0.1:1/0",
            gateway_ratelimit_redis_timeout_ms=50,
            rl_order_create_per_user_per_min=1,
        )
    )
    headers = customer()
    results = [await client.post("/api/v1/orders", json={}, headers=headers) for _ in range(3)]
    assert [r.status_code for r in results] == [200] * 3
    assert "ratelimit-limit" not in results[0].headers
    # The platform logger writes JSON lines to stdout (RFC 0001 logging).
    assert '"failed_open": true' in capsys.readouterr().out


async def test_disabled_limiter_never_touches_redis() -> None:
    limiter = RateLimiter(limited_settings(gateway_ratelimit_enabled=False), None)
    assert await limiter.hit("default", "1.2.3.4") is None
    assert await limiter.after_auth("sub", None) == []


def test_tightest_picks_the_class_closest_to_its_limit() -> None:
    loose = Decision(RateClass("default", 100, 60), 10, 30)
    tight = Decision(RateClass("order_create", 30, 60), 29, 30)
    assert tightest([loose, tight]) is tight
    assert tightest([]) is None
    over = Decision(RateClass("x", 1, 60), 5, 0)
    assert over.remaining == 0 and not over.allowed


async def test_a_burst_through_the_bounded_pool_is_counted_exactly(redis) -> None:
    """60 concurrent hits share a pool of 20 warm connections: they queue for a
    connection instead of dialling 60, and every hit is counted exactly once.
    (The budget is generous here so a loaded CI machine cannot make it flaky; in
    the stack, cold connections were what broke the 50 ms budget, and `warm`
    removes them.)"""
    settings = limited_settings(gateway_ratelimit_redis_timeout_ms=2000)
    limiter = RateLimiter(settings, create_limiter_redis(settings))
    try:
        await limiter.warm()
        sub = str(uuid.uuid4())
        decisions = await asyncio.gather(*(limiter.hit("order_create", sub) for _ in range(60)))
        assert None not in decisions  # nothing failed open
        assert sorted(d.count for d in decisions) == list(range(1, 61))
        assert sum(d.allowed for d in decisions) == 3
    finally:
        await limiter.redis.aclose(close_connection_pool=True)


async def test_warm_tolerates_redis_being_down(caplog) -> None:
    settings = limited_settings(
        redis_url="redis://127.0.0.1:1/0", gateway_ratelimit_redis_pool_size=2
    )
    limiter = RateLimiter(settings, create_limiter_redis(settings))
    try:
        await limiter.warm()  # does not raise
        assert await limiter.hit("default", "1.2.3.4") is None
    finally:
        await limiter.redis.aclose(close_connection_pool=True)
    # No app here, so the platform JSON logger is not installed: caplog sees it.
    assert any("failing open" in r.getMessage() for r in caplog.records)


async def test_warm_is_a_no_op_when_disabled() -> None:
    await RateLimiter(limited_settings(gateway_ratelimit_enabled=False), None).warm()
