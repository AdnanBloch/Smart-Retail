"""``FanoutHub`` against the test Redis (C17, NTF-10, NTF-D35): what it routes, what it
drops, and that a failed or unconfirmed subscribe leaves no listener behind."""

import asyncio
import json
import uuid

import pytest

from app.v1.stream.connection import claim_slot, conns_key, release_slot
from app.v1.stream.fanout import FanoutHub, user_channel
from smartretail_common.redis import create_redis


class Recorder:
    def __init__(self) -> None:
        self.messages: list[dict] = []
        self.aborted: str | None = None

    def offer(self, message: dict) -> None:
        self.messages.append(message)

    def abort(self, reason: str) -> None:
        self.aborted = reason


@pytest.fixture
async def redis(redis_url):
    client = create_redis(redis_url)
    yield client
    await client.aclose()


@pytest.fixture
async def hub(redis):
    h = FanoutHub(redis)
    yield h
    await h.close()


async def _wait_for(predicate, timeout: float = 3.0) -> None:  # noqa: ASYNC109
    for _ in range(int(timeout / 0.02)):
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not met")


async def test_malformed_messages_are_dropped_and_the_reader_keeps_going(hub, redis) -> None:
    user = uuid.uuid4()
    listener = Recorder()
    await hub.subscribe(user, listener)
    channel = user_channel(user)
    for raw in ("not json", "[1, 2]", '"a string"', "42"):
        await redis.publish(channel, raw)
    await redis.publish(channel, json.dumps({"kind": "unread_count", "count": 3}))
    await _wait_for(lambda: listener.messages)
    assert listener.messages == [{"kind": "unread_count", "count": 3}]
    assert listener.aborted is None


async def test_one_subscription_serves_every_local_listener(hub, redis) -> None:
    user = uuid.uuid4()
    first, second = Recorder(), Recorder()
    await hub.subscribe(user, first)
    await hub.subscribe(user, second)
    assert hub.channels == {user_channel(user)}
    await redis.publish(user_channel(user), json.dumps({"kind": "unread_count", "count": 1}))
    await _wait_for(lambda: first.messages and second.messages)

    await hub.unsubscribe(user, first)
    assert hub.channels == {user_channel(user)}  # still needed by `second`
    await hub.unsubscribe(user, second)
    assert hub.channels == set()
    await hub.unsubscribe(user, second)  # a repeated unsubscribe is harmless


async def test_messages_for_other_users_are_not_delivered(hub, redis) -> None:
    mine, theirs = uuid.uuid4(), uuid.uuid4()
    listener = Recorder()
    await hub.subscribe(mine, listener)
    await redis.publish(user_channel(theirs), json.dumps({"kind": "unread_count", "count": 9}))
    await redis.publish(user_channel(mine), json.dumps({"kind": "unread_count", "count": 1}))
    await _wait_for(lambda: listener.messages)
    assert listener.messages == [{"kind": "unread_count", "count": 1}]


def test_route_ignores_frames_without_a_channel_or_of_other_kinds(redis) -> None:
    hub = FanoutHub(redis)
    listener = Recorder()
    hub._listeners["nt:user:x"] = {listener}
    for message in (
        {"channel": None, "type": "message", "data": "{}"},
        {"channel": "nt:user:x", "type": "unsubscribe", "data": 0},
        {"channel": "nt:user:x", "type": "pong", "data": ""},
    ):
        hub._route(message)
    assert listener.messages == []


async def test_a_failed_subscribe_leaves_no_listener() -> None:
    dead = create_redis("redis://127.0.0.1:1/0")
    hub = FanoutHub(dead)
    try:
        with pytest.raises(Exception):  # noqa: B017 - whatever the client raises
            await hub.subscribe(uuid.uuid4(), Recorder())
        assert hub.channels == set()
    finally:
        await hub.close()
        await dead.aclose()


async def test_an_unconfirmed_subscribe_is_undone(redis) -> None:
    """NTF-10: subscribe returns only once Redis confirmed it; a confirmation that
    never comes in time must not leave a half-subscribed listener."""
    hub = FanoutHub(redis, subscribe_timeout=0)
    try:
        with pytest.raises(TimeoutError):
            await hub.subscribe(uuid.uuid4(), Recorder())
        assert hub.channels == set()
    finally:
        await hub.close()


async def test_an_unsubscribe_failure_is_logged_not_raised(hub, monkeypatch, caplog) -> None:
    user = uuid.uuid4()
    listener = Recorder()
    await hub.subscribe(user, listener)

    async def broken(*channels):
        raise ConnectionError("pubsub gone")

    monkeypatch.setattr(hub._pubsub, "unsubscribe", broken)
    await hub.unsubscribe(user, listener)  # stream cleanup must never raise
    assert hub.channels == set()
    assert any("unsubscribe failed" in r.getMessage() for r in caplog.records)


async def test_a_release_racing_a_claim_never_loses_the_claim(redis) -> None:
    """The last release of a user's slots and a new stream's claim, at once: the
    count afterwards must be exactly the new claim, or the cap drifts (C15)."""
    user = uuid.uuid4()
    lost = 0
    for _ in range(200):
        assert await claim_slot(redis, user, limit=3, ttl_seconds=60)
        await asyncio.gather(
            release_slot(redis, user), claim_slot(redis, user, limit=3, ttl_seconds=60)
        )
        if int(await redis.get(conns_key(user)) or 0) != 1:
            lost += 1
        await redis.delete(conns_key(user))
    assert lost == 0


async def test_a_release_with_redis_down_is_logged_not_raised(caplog) -> None:
    """Stream cleanup runs in a ``finally``: it must not raise; the TTL reclaims the slot."""
    dead = create_redis("redis://127.0.0.1:1/0")
    try:
        await release_slot(dead, uuid.uuid4())
    finally:
        await dead.aclose()
    assert any("slot release failed" in r.getMessage() for r in caplog.records)
