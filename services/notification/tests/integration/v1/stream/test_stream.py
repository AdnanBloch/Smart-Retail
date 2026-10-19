"""The SSE stream against real servers (C15-C22, G5a-G5c, NTF-10, NTF-12).

Each test runs one or two uvicorn servers on ephemeral ports over the shared
test Postgres and Redis; "two replicas" are two app instances, each with its
own fan-out hub.
"""

import asyncio
import uuid

import httpx
import pytest
from sqlalchemy import select

from app.v1.notifications.consumers.after_commit import after_commit
from app.v1.notifications.recording_repo import RecordingRepo
from app.v1.stream.models import StreamCursor
from smartretail_common.redis import create_redis
from smartretail_common.testing import truncate_all
from tests.integration.conftest import (
    TABLES,
    close_app,
    confirmed,
    envelope,
    make_app,
    seed,
    shipment,
)
from tests.integration.v1.stream.sse import LiveServer, open_stream, ticket

BASE = "/api/v1/notifications"
DEVICE_A = "device-AAAA-1"
DEVICE_B = "device-BBBB-2"


class Cluster:
    """Replicas of the service, started on demand, all stopped at teardown."""

    def __init__(self, settings, keypair) -> None:
        self.settings = settings
        self.keypair = keypair
        self.servers: list[LiveServer] = []
        self.clients: list[httpx.AsyncClient] = []

    async def replica(self) -> tuple[LiveServer, httpx.AsyncClient]:
        server = await LiveServer(make_app(self.settings, self.keypair)).start()
        client = httpx.AsyncClient(base_url=server.url, timeout=10)
        self.servers.append(server)
        self.clients.append(client)
        return server, client

    async def close(self) -> None:
        for client in self.clients:
            await client.aclose()
        for server in self.servers:
            await server.stop()
            await close_app(server.app)  # idempotent after the lifespan's own shutdown


@pytest.fixture
async def cluster(settings, keypair):
    c = Cluster(settings, keypair)
    yield c
    await c.close()
    await truncate_all(settings.database_url, TABLES)


async def consume(app, env) -> None:
    """What the consumer process does: record in one transaction, then the after-commit step."""
    async with app.state.db.sessionmaker() as session, session.begin():
        await RecordingRepo(session).record(env)

    async def no_dispatch(_):
        return None

    await after_commit(app.state.db.sessionmaker, app.state.redis, no_dispatch)(env)


async def notify(app, user_id: str, n: int = 1) -> None:
    for i in range(n):
        await consume(app, envelope("order.confirmed", confirmed(user_id, f"SR-L{i}")))


# --- opening ---------------------------------------------------------------


async def test_stream_headers_and_cold_start(cluster, callers) -> None:
    """G5c: an unseen client_id replays nothing and learns the count from hello."""
    server, client = await cluster.replica()
    await seed(server.app, callers.customer_id, 120)
    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        headers = stream.response.headers
        assert headers["content-type"].startswith("text/event-stream")
        assert headers["cache-control"] == "no-cache"
        assert headers["x-accel-buffering"] == "no"
        hello, frames = await stream.opening()
        assert hello.data["cold_start"] is True and hello.data["resumed_from"] is None
        assert hello.data["unread_count"] == 120 and hello.data["client_id"] == DEVICE_A
        assert hello.data["server_seq"] > 0 and hello.retry is not None
        assert [f.event for f in frames] == []  # zero notification frames

        await notify(server.app, callers.customer_id)  # live from this point
        live, before = await stream.until("notification")
        assert before == [] and live.data["type"] == "order_confirmed"
        assert live.id == str(live.data["seq"]) and live.data["origin"] is False
        count, _ = await stream.until("unread_count")
        assert count.data == {"count": 121}

    async with server.app.state.db.sessionmaker() as session:
        cursor = await session.scalar(
            select(StreamCursor).where(StreamCursor.client_id == DEVICE_A)
        )
    assert cursor is not None and cursor.cursor_seq == hello.data["server_seq"]


async def test_resume_with_last_event_id_replays_exactly_the_gap(cluster, callers) -> None:
    """G5b: drop, miss M, reconnect with Last-Event-ID → exactly M frames, in order."""
    server, client = await cluster.replica()
    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        await stream.opening()
        await notify(server.app, callers.customer_id, 2)
        first, _ = await stream.until("notification")
        second, _ = await stream.until("notification")
    last_seen = int(second.id)  # type: ignore[arg-type]
    await notify(server.app, callers.customer_id, 3)

    async with open_stream(
        client, callers.customer, client_id=DEVICE_A, last_event_id=last_seen
    ) as stream:
        hello, frames = await stream.opening()
        assert hello.data["cold_start"] is False and hello.data["resumed_from"] == last_seen
        seqs = [int(f.id) for f in frames if f.event == "notification"]  # type: ignore[arg-type]
        assert len(seqs) == 3 and seqs == sorted(seqs) and seqs[0] > last_seen
        assert int(first.id) < last_seen  # type: ignore[arg-type]


async def test_warm_resume_through_the_server_cursor_without_last_event_id(
    cluster, callers
) -> None:
    """The page-reload case: ack to N, miss M, reconnect with no header → M frames."""
    server, client = await cluster.replica()
    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        await stream.opening()
        await notify(server.app, callers.customer_id, 2)
        await stream.until("notification")
        latest, _ = await stream.until("notification")
    r = await client.post(
        f"{BASE}/ack",
        json={"client_id": DEVICE_A, "up_to_seq": int(latest.id), "ids": [latest.data["id"]]},  # type: ignore[arg-type]
        headers=callers.customer,
    )
    assert r.status_code == 204
    await notify(server.app, callers.customer_id, 2)

    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        hello, frames = await stream.opening()
        assert hello.data["resumed_from"] == int(latest.id)  # type: ignore[arg-type]
        assert len([f for f in frames if f.event == "notification"]) == 2


async def test_catch_up_is_bounded_and_sends_the_oldest_fifty(cluster, callers) -> None:
    """C22: 60 missed → 50 frames ascending + catchup_truncated{omitted: 10}; the
    list pages the rest from the last seq sent."""
    server, client = await cluster.replica()
    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        await stream.opening()  # cold start: cursor at 0
    await seed(server.app, callers.customer_id, 60)

    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        hello, frames = await stream.opening()
        assert hello.data["resumed_from"] == 0
        assert frames[0].event == "catchup_truncated"
        notifications = [f for f in frames if f.event == "notification"]
        seqs = [n.data["seq"] for n in notifications]
        assert len(seqs) == 50 and seqs == sorted(seqs)
        assert frames[0].data == {"omitted_count": 10, "oldest_seq_sent": seqs[0]}
    rest = await client.get(
        BASE, params={"since_seq": seqs[-1], "limit": 100}, headers=callers.customer
    )
    assert rest.json()["total"] == 10


async def test_catch_up_has_an_age_bound(cluster, callers) -> None:
    server, client = await cluster.replica()
    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        await stream.opening()
    await seed(server.app, callers.customer_id, 2, age_hours=25)
    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        _, frames = await stream.opening()
        assert [f.event for f in frames] == []
    listed = await client.get(BASE, headers=callers.customer)
    assert listed.json()["total"] == 2  # still in the durable list


async def test_ephemeral_stream_has_no_cursor(cluster, callers) -> None:
    """NTF-12: no X-Client-Id → no cursor row; Last-Event-ID is the only floor."""
    server, client = await cluster.replica()
    ids = await seed(server.app, callers.customer_id, 3)
    async with open_stream(client, callers.customer) as stream:
        hello, frames = await stream.opening()
        assert hello.data["client_id"] is None and hello.data["cold_start"] is True
        assert frames == []
    async with server.app.state.db.sessionmaker() as session:
        first = await session.scalar(
            select(StreamCursor.id).where(StreamCursor.user_id == uuid.UUID(callers.customer_id))
        )
    assert first is None

    listed = (await client.get(BASE, params={"since_seq": 0}, headers=callers.customer)).json()
    first_seq = listed["items"][0]["seq"]
    async with open_stream(client, callers.customer, last_event_id=first_seq) as stream:
        hello, frames = await stream.opening()
        assert hello.data["resumed_from"] == first_seq
        assert [f.data["id"] for f in frames if f.event == "notification"] == [
            str(i) for i in ids[1:]
        ]


async def test_a_spoofed_client_id_only_ever_reaches_your_own_rows(cluster, callers) -> None:
    """I7: another user's client_id string gets the caller's own cursor and channel."""
    server, client = await cluster.replica()
    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        await stream.opening()
    async with open_stream(client, callers.other, client_id=DEVICE_A) as stream:
        await stream.opening()
        await notify(server.app, callers.customer_id)
        await notify(server.app, callers.other_id)
        live, _ = await stream.until("notification")
        listed = (await client.get(BASE, headers=callers.other)).json()
        assert live.data["id"] == listed["items"][0]["id"]
    async with server.app.state.db.sessionmaker() as session:
        owners = set(
            await session.scalars(
                select(StreamCursor.user_id).where(StreamCursor.client_id == DEVICE_A)
            )
        )
    assert owners == {uuid.UUID(callers.customer_id), uuid.UUID(callers.other_id)}


# --- refusals ----------------------------------------------------------------


async def test_a_ticket_works_once(cluster, callers) -> None:
    server, client = await cluster.replica()
    token = await ticket(client, callers.customer, DEVICE_A)
    async with client.stream("GET", f"{BASE}/stream", params={"ticket": token}) as response:
        assert response.status_code == 200
    again = await client.get(f"{BASE}/stream", params={"ticket": token})
    assert again.status_code == 401
    assert again.json()["type"].endswith("/stream-ticket-invalid")
    for bad in ("nope", "x" * 43):
        assert (await client.get(f"{BASE}/stream", params={"ticket": bad})).status_code == 401
    assert (await client.get(f"{BASE}/stream")).status_code == 422


async def test_an_expired_ticket_is_refused(cluster, callers, settings) -> None:
    settings.stream_ticket_ttl_seconds = 1
    server, client = await cluster.replica()
    token = await ticket(client, callers.customer, DEVICE_A)
    await asyncio.sleep(1.5)
    assert (await client.get(f"{BASE}/stream", params={"ticket": token})).status_code == 401


async def test_a_ticket_outliving_its_token_is_refused(cluster, callers, keypair) -> None:
    server, client = await cluster.replica()
    headers = callers.headers(callers.customer_id, ttl=1)
    token = await ticket(client, headers, DEVICE_A)
    await asyncio.sleep(1.2)
    assert (await client.get(f"{BASE}/stream", params={"ticket": token})).status_code == 401


async def test_a_fourth_stream_is_refused(cluster, callers) -> None:
    server, client = await cluster.replica()
    async with (
        open_stream(client, callers.customer, client_id="device-0001") as s1,
        open_stream(client, callers.customer, client_id="device-0002") as s2,
        open_stream(client, callers.customer, client_id="device-0003") as s3,
    ):
        for s in (s1, s2, s3):
            await s.opening()
        token = await ticket(client, callers.customer, "device-0004")
        refused = await client.get(f"{BASE}/stream", params={"ticket": token})
        assert refused.status_code == 429
        assert refused.json()["type"].endswith("/too-many-streams")
        # Another user is unaffected.
        async with open_stream(client, callers.other, client_id="device-0005") as other:
            await other.opening()
    # Closing streams frees their slots.
    await asyncio.sleep(0.2)
    async with open_stream(client, callers.customer, client_id="device-0004") as s4:
        await s4.opening()


async def test_stream_ticket_needs_redis(app, client, callers) -> None:
    real = app.state.redis
    app.state.redis = create_redis("redis://127.0.0.1:1/0")
    try:
        r = await client.post(f"{BASE}/stream-ticket", headers=callers.customer)
        assert r.status_code == 503
        r = await client.get(f"{BASE}/stream", params={"ticket": "a" * 43})
        assert r.status_code == 503
        # The list and the count do not touch Redis (§3.1.1.5).
        assert (
            await client.get(f"{BASE}/unread-count", headers=callers.customer)
        ).status_code == 200
    finally:
        await app.state.redis.aclose()
        app.state.redis = real


# --- live delivery -------------------------------------------------------------


async def test_fan_out_across_replicas(cluster, callers) -> None:
    """G5a: the stream is held by replica A; the event is consumed by replica B."""
    a, client_a = await cluster.replica()
    b, client_b = await cluster.replica()
    async with (
        open_stream(client_a, callers.customer, client_id=DEVICE_A) as on_a,
        open_stream(client_a, callers.customer, client_id=DEVICE_B) as also_on_a,
    ):
        await on_a.opening()
        await also_on_a.opening()
        assert len(a.app.state.hub.channels) == 1  # one subscription for both streams

        await consume(
            b.app, envelope("shipment.status_changed", shipment(callers.customer_id, "shipped"))
        )
        for stream in (on_a, also_on_a):
            frame, _ = await stream.until("notification", timeout=3)
            assert frame.data["type"] == "shipment_update"
            count, _ = await stream.until("unread_count")
            assert count.data == {"count": 1}

        # Mark-read through B moves every device's badge (NTF-11).
        r = await client_b.post(f"{BASE}/{frame.data['id']}/read", headers=callers.customer)
        assert r.status_code == 200
        count, _ = await on_a.until("unread_count")
        assert count.data == {"count": 0}
    await asyncio.sleep(0.2)
    assert a.app.state.hub.channels == set()  # unsubscribed with the last stream


async def test_origin_flag_marks_the_originating_device(cluster, callers) -> None:
    server, client = await cluster.replica()
    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        await stream.opening()
        env = envelope("order.confirmed", confirmed(callers.customer_id), client_id=DEVICE_A)
        await consume(server.app, env)
        frame, _ = await stream.until("notification")
        assert frame.data["origin"] is True


async def test_heartbeat_keeps_an_idle_stream_alive(cluster, callers, settings) -> None:
    settings.stream_heartbeat_seconds = 0.3
    server, client = await cluster.replica()
    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        await stream.opening()
        keepalive = await stream.next()
        assert keepalive is not None and keepalive.comments == ["keepalive"]
        ping = await stream.next()
        assert ping is not None and ping.event == "ping"
        ttl = await server.app.state.redis.ttl(f"nt:conns:{callers.customer_id}")
        assert 0 < ttl <= settings.stream_conn_count_ttl_seconds


async def test_the_stream_closes_at_token_expiry_with_an_error_frame(cluster, callers) -> None:
    server, client = await cluster.replica()
    headers = callers.headers(callers.customer_id, ttl=2)
    async with open_stream(client, headers, client_id=DEVICE_A) as stream:
        await stream.opening()
        error, _ = await stream.until("error", timeout=5)
        assert error.data["type"].endswith("/stream-token-expired")
        assert error.data["status"] == 401
        assert await stream.next() is None  # closed


async def test_a_slow_consumer_is_disconnected_and_recovers_by_seq(
    cluster, callers, settings
) -> None:
    settings.stream_queue_max = 2
    server, client = await cluster.replica()
    redis = server.app.state.redis
    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        await stream.opening()
        pipe = redis.pipeline()
        for i in range(300):
            pipe.publish(f"nt:user:{callers.customer_id}", f'{{"kind":"unread_count","count":{i}}}')
        await pipe.execute()
        error, _ = await stream.until("error", timeout=5)
        assert error.data["type"].endswith("/stream-slow-consumer")
        assert await stream.next() is None


async def test_shutdown_drains_streams_with_a_retry_hint(cluster, callers) -> None:
    server, client = await cluster.replica()
    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        await stream.opening()
        await server.app.state.hub.close()
        hint = await stream.next()
        assert hint is not None and hint.retry is not None and hint.event is None
        assert await stream.next() is None


async def test_a_lost_fanout_connection_closes_local_streams(cluster, callers) -> None:
    server, client = await cluster.replica()
    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        await stream.opening()
        hub = server.app.state.hub
        await hub._pubsub.connection.disconnect()  # the reader's next read fails
        hint = await stream.next()
        assert hint is not None and hint.retry is not None
    # The hub recovers on the next stream.
    async with open_stream(client, callers.customer, client_id=DEVICE_A) as stream:
        await stream.opening()
        await notify(server.app, callers.customer_id)
        frame, _ = await stream.until("notification")
        assert frame.data["type"] == "order_confirmed"
