"""The fixed-window limiter: counts per window, and fails open (RFC 0001 §3.1.1.8)."""

import uuid

import pytest

from smartretail_common.redis import allow, create_redis
from smartretail_common.testing import redis_reachable
from smartretail_common.testing import test_redis_url as redis_url


@pytest.fixture
async def redis():
    if not redis_reachable():
        pytest.skip("test Redis not reachable; run `make test-db`")
    client = create_redis(redis_url())
    yield client
    await client.aclose()


async def test_the_limit_holds_within_a_window(redis) -> None:
    key = f"rl:test:{uuid.uuid4()}:0"
    results = [await allow(redis, key, 3, 60) for _ in range(5)]
    assert results == [True, True, True, False, False]
    assert 0 < await redis.ttl(key) <= 60  # the window expires on its own


async def test_a_redis_outage_allows_the_caller() -> None:
    dead = create_redis("redis://127.0.0.1:1/0")
    assert await allow(dead, "rl:test:x:0", 1, 60) is True
    await dead.aclose()


def test_reachability_probe_handles_a_dead_port() -> None:
    assert redis_reachable("redis://127.0.0.1:1/0") is False
