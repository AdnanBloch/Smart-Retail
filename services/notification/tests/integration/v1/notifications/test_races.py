"""Races on one user's rows (mentor review 2026-10-20 §8; RFC 0007 I3, I5, I8).

Mark-read and read-all are single guarded ``UPDATE``s (``read_at IS NULL``),
the ack is a guarded ``UPDATE`` plus a ``greatest()`` upsert, and the ack rate
limit is one atomic ``INCR``. Each test fires the conflicting calls at once,
over several rounds, and checks that every row is counted exactly once.
"""

import asyncio
import time
import uuid
from datetime import datetime

from sqlalchemy import select

from app.v1.notifications.models import Notification
from app.v1.stream.cursor_repo import CursorRepo
from app.v1.stream.models import StreamCursor
from tests.integration.conftest import confirmed, envelope, seed
from tests.integration.v1.stream.conftest import consume

BASE = "/api/v1/notifications"
ROUNDS = 5


async def _in_app(app, user_id: str) -> list[Notification]:
    async with app.state.db.sessionmaker() as session:
        return list(
            await session.scalars(
                select(Notification)
                .where(Notification.user_id == uuid.UUID(user_id), Notification.channel == "in_app")
                .order_by(Notification.seq)
            )
        )


async def _unread(client, headers) -> int:
    return int((await client.get(f"{BASE}/unread-count", headers=headers)).json()["count"])


async def _drain(pubsub) -> list[dict]:
    import json

    out = []
    while True:
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.3)
        if message is None:
            return out
        out.append(json.loads(message["data"]))


async def test_concurrent_mark_read_writes_read_at_once_and_publishes_once(
    app, client, callers
) -> None:
    [nid] = await seed(app, callers.customer_id, 1)
    pubsub = app.state.redis.pubsub()
    await pubsub.subscribe(f"nt:user:{callers.customer_id}")
    assert (await pubsub.get_message(timeout=2.0))["type"] == "subscribe"  # confirmed
    try:
        responses = await asyncio.gather(
            *(client.post(f"{BASE}/{nid}/read", headers=callers.customer) for _ in range(6))
        )
        published = await _drain(pubsub)
    finally:
        await pubsub.aclose()
    assert {r.status_code for r in responses} == {200}
    # I8: every caller sees the one read_at that was written.
    assert len({r.json()["read_at"] for r in responses}) == 1
    # Only the call that changed the row moves the badges (NTF-11).
    assert published == [{"kind": "unread_count", "count": 0}]


async def test_concurrent_read_all_counts_each_row_once(app, client, callers) -> None:
    for _ in range(ROUNDS):
        await seed(app, callers.customer_id, 7)
        responses = await asyncio.gather(
            *(client.post(f"{BASE}/read-all", headers=callers.customer) for _ in range(4))
        )
        assert sum(r.json()["updated_count"] for r in responses) == 7
        assert await _unread(client, callers.customer) == 0


async def test_mark_read_racing_read_all_counts_the_row_once(app, client, callers) -> None:
    for _ in range(ROUNDS):
        ids = await seed(app, callers.customer_id, 3)
        one, everything = await asyncio.gather(
            client.post(f"{BASE}/{ids[0]}/read", headers=callers.customer),
            client.post(f"{BASE}/read-all", headers=callers.customer),
        )
        assert one.status_code == 200 and everything.status_code == 200
        # Whoever got ids[0] first, read-all updated the other two and maybe it.
        assert everything.json()["updated_count"] in {2, 3}
        stored = next(r for r in await _in_app(app, callers.customer_id) if r.id == ids[0])
        # The single-row call reports the one read_at stored, whoever wrote it (I8).
        assert datetime.fromisoformat(one.json()["read_at"]) == stored.read_at
        assert await _unread(client, callers.customer) == 0


async def test_read_all_racing_a_new_notification_loses_nothing(app, client, callers) -> None:
    """A row recorded while read-all runs is either included in its count or stays
    unread — never silently marked read without being counted, never lost."""
    for i in range(ROUNDS * 2):
        await seed(app, callers.customer_id, 3)
        recorded = envelope("order.confirmed", confirmed(callers.customer_id, f"SR-RACE-{i}"))
        everything, _ = await asyncio.gather(
            client.post(f"{BASE}/read-all", headers=callers.customer),
            consume(app, recorded),
        )
        updated = everything.json()["updated_count"]
        rows = await _in_app(app, callers.customer_id)
        newest = next(r for r in rows if r.payload["event_id"] == str(recorded.event_id))
        unread = await _unread(client, callers.customer)
        if newest.read_at is None:
            assert updated == 3 and unread == 1
        else:
            assert updated == 4 and unread == 0
        await client.post(f"{BASE}/read-all", headers=callers.customer)  # settle the round
    assert len(await _in_app(app, callers.customer_id)) == ROUNDS * 2 * 4


async def test_concurrent_acks_stamp_once_and_the_cursor_reaches_the_max(
    app, client, callers
) -> None:
    """I3: first writer wins on delivered_at; I5: the cursor only moves up, whatever
    order concurrent acks commit in."""
    ids = await seed(app, callers.customer_id, 6)
    rows = await _in_app(app, callers.customer_id)
    seqs = [r.seq for r in rows]
    devices = [f"device-RACE-{i}" for i in range(3)]
    calls = [
        client.post(
            f"{BASE}/ack",
            json={"client_id": device, "ids": [str(ids[0])]},
            headers=callers.customer,
        )
        for device in devices
    ] + [
        client.post(
            f"{BASE}/ack",
            json={"client_id": "device-CURSOR", "up_to_seq": seq},
            headers=callers.customer,
        )
        for seq in seqs
    ]
    responses = await asyncio.gather(*calls)
    assert {r.status_code for r in responses} == {204}

    stamped = next(r for r in await _in_app(app, callers.customer_id) if r.id == ids[0])
    assert stamped.delivered_at is not None and stamped.delivered_client_id in devices
    async with app.state.db.sessionmaker() as session:
        cursors = list(
            await session.scalars(
                select(StreamCursor).where(StreamCursor.client_id == "device-CURSOR")
            )
        )
    assert [c.cursor_seq for c in cursors] == [max(seqs)]


async def test_concurrent_cold_starts_of_one_device_make_one_cursor(app, callers) -> None:
    user = uuid.UUID(callers.customer_id)

    async def cold_start(at_seq: int) -> None:
        async with app.state.db.sessionmaker() as session:
            await CursorRepo(session).cold_start(user, "device-COLD", at_seq, "ua")
            await session.commit()

    await asyncio.gather(*(cold_start(10 + i) for i in range(6)))
    async with app.state.db.sessionmaker() as session:
        cursors = list(
            await session.scalars(select(StreamCursor).where(StreamCursor.user_id == user))
        )
    # The first insert wins; later cold starts only touch it, never move it.
    assert len(cursors) == 1 and 10 <= cursors[0].cursor_seq <= 15


async def test_concurrent_acks_respect_the_rate_limit_exactly(client, callers, settings) -> None:
    settings.ack_rate_limit_per_minute = 5
    if time.time() % 60 > 55:  # keep the burst inside one fixed window
        await asyncio.sleep(60 - time.time() % 60 + 0.1)
    body = {"client_id": "device-BURST"}
    responses = await asyncio.gather(
        *(client.post(f"{BASE}/ack", json=body, headers=callers.customer) for _ in range(12))
    )
    codes = sorted(r.status_code for r in responses)
    assert codes == [204] * 5 + [429] * 7
    refused = next(r for r in responses if r.status_code == 429)
    assert refused.headers["retry-after"] == "60"
    assert refused.json()["type"].endswith("/rate-limited")
