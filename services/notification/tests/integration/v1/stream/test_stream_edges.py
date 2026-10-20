"""Stream refusals and races (C15, C19, C22; mentor review 2026-10-20 §8).

A stream that fails to open must give its slot back, or the user is locked out
by phantom streams until the TTL; a ticket is single-use even when redeemed
concurrently; and the per-user cap holds when every open races.
"""

import asyncio

from app.v1.stream import routes as stream_routes
from app.v1.stream.connection import conns_key
from app.v1.stream.tickets import ticket_key
from tests.integration.v1.stream.sse import SseStream, ticket

BASE = "/api/v1/notifications"
ROUNDS = 3


async def _slots(app, user_id: str) -> int:
    return int(await app.state.redis.get(conns_key(user_id)) or 0)


async def test_a_stored_ticket_that_is_not_ours_is_refused(app, client, callers) -> None:
    token = "t" * 43
    for raw in ("not json", '{"client_id": null}', '{"user_id": "u", "jwt_exp": "soon"}'):
        await app.state.redis.set(ticket_key(token), raw, ex=60)
        r = await client.get(f"{BASE}/stream", params={"ticket": token})
        assert r.status_code == 401, raw
        # GETDEL consumed it: a corrupt ticket is not retried either.
        assert await app.state.redis.get(ticket_key(token)) is None


async def test_slot_store_down_is_503(app, client, callers, monkeypatch) -> None:
    async def down(*args, **kwargs):
        raise ConnectionError("redis down")

    monkeypatch.setattr(stream_routes, "claim_slot", down)
    token = await ticket(client, callers.customer)
    r = await client.get(f"{BASE}/stream", params={"ticket": token})
    assert r.status_code == 503 and r.json()["type"].endswith("/dependency-unavailable")


async def test_a_failed_subscribe_gives_the_slot_back(app, client, callers, monkeypatch) -> None:
    async def down(user_id, listener):
        raise ConnectionError("pubsub down")

    monkeypatch.setattr(app.state.hub, "subscribe", down)
    for _ in range(4):  # more than the cap: each failure released its slot
        token = await ticket(client, callers.customer)
        r = await client.get(f"{BASE}/stream", params={"ticket": token})
        assert r.status_code == 503, r.text
    assert await _slots(app, callers.customer_id) == 0


async def test_a_database_failure_on_open_unsubscribes_and_gives_the_slot_back(
    app, client, callers, monkeypatch
) -> None:
    async def db_down(*args, **kwargs):
        raise ConnectionRefusedError("database down")

    monkeypatch.setattr(stream_routes, "open_stream", db_down)
    for _ in range(4):
        token = await ticket(client, callers.customer, "device-DBDOWN")
        r = await client.get(f"{BASE}/stream", params={"ticket": token})
        assert r.status_code == 503, r.text
    assert await _slots(app, callers.customer_id) == 0
    assert app.state.hub.channels == set()


async def _open_and_hold(client, token: str, statuses: list[int], release: asyncio.Event):
    """Opens a stream with ``token``, records the status, holds a 200 until ``release``."""
    async with client.stream("GET", f"{BASE}/stream", params={"ticket": token}) as response:
        statuses.append(response.status_code)
        if response.status_code != 200:
            await response.aread()
            return
        # Keep the reader referenced: a collected line iterator closes the stream.
        stream = SseStream(response)
        hello = await stream.next()
        assert hello is not None and hello.event == "hello"
        await release.wait()
        del stream


async def _settled(statuses: list[int], expected: int) -> None:
    for _ in range(250):
        if len(statuses) == expected:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"only {len(statuses)} of {expected} opens answered")


async def test_concurrent_redemption_of_one_ticket_opens_one_stream(cluster, callers) -> None:
    _, client = await cluster.replica()
    for _ in range(ROUNDS):
        token = await ticket(client, callers.customer, "device-ONCE")
        release, statuses = asyncio.Event(), []
        tasks = [
            asyncio.create_task(_open_and_hold(client, token, statuses, release)) for _ in range(4)
        ]
        await _settled(statuses, 4)
        release.set()
        await asyncio.gather(*tasks)
        assert sorted(statuses) == [200, 401, 401, 401]


async def test_concurrent_opens_never_exceed_the_cap(cluster, callers, settings) -> None:
    server, client = await cluster.replica()
    cap = settings.stream_max_per_user
    for _ in range(ROUNDS):
        tokens = [await ticket(client, callers.customer, f"device-CAP{i:02d}") for i in range(6)]
        release, statuses = asyncio.Event(), []
        tasks = [asyncio.create_task(_open_and_hold(client, t, statuses, release)) for t in tokens]
        await _settled(statuses, len(tokens))
        assert sorted(statuses) == [200] * cap + [429] * (len(tokens) - cap)
        assert await _slots(server.app, callers.customer_id) == cap
        release.set()
        await asyncio.gather(*tasks)
        for _ in range(100):  # cleanup runs as each response closes
            if await _slots(server.app, callers.customer_id) == 0:
                break
            await asyncio.sleep(0.02)
        assert await _slots(server.app, callers.customer_id) == 0
