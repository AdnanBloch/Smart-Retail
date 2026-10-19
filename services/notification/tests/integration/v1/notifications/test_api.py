"""``/api/v1/notifications``: the owner's list, read state, the admin view and
the ack (RFC 0007 §3.1.2.1, §3.1.2.2)."""

import json
import uuid

import pytest
from sqlalchemy import select, text

from app.v1.notifications.models import Notification
from app.v1.stream.models import StreamCursor
from smartretail_common.redis import create_redis
from tests.integration.conftest import seed

BASE = "/api/v1/notifications"


async def test_every_route_needs_a_token(client) -> None:
    for method, path in [
        ("GET", BASE),
        ("GET", f"{BASE}/unread-count"),
        ("POST", f"{BASE}/read-all"),
        ("POST", f"{BASE}/ack"),
        ("POST", f"{BASE}/stream-ticket"),
        ("GET", f"{BASE}/admin"),
        ("POST", f"{BASE}/{uuid.uuid4()}/read"),
    ]:
        r = await client.request(method, path)
        assert r.status_code == 401, (method, path, r.text)
        assert r.headers["content-type"] == "application/problem+json"


async def test_list_is_own_in_app_rows_newest_first(app, client, callers) -> None:
    ids = await seed(app, callers.customer_id, 3)
    await seed(app, callers.other_id, 2)

    r = await client.get(BASE, headers=callers.customer)
    assert r.status_code == 200, r.text
    page = r.json()
    assert page["total"] == 3 and page["limit"] == 20 and page["offset"] == 0
    assert [i["id"] for i in page["items"]] == [str(i) for i in reversed(ids)]
    item = page["items"][0]
    assert set(item) == {
        "id",
        "seq",
        "type",
        "title",
        "body",
        "created_at",
        "read_at",
        "delivered_at",
    }
    assert item["type"] == "order_confirmed"


async def test_list_filters_and_paging(app, client, callers) -> None:
    ids = await seed(app, callers.customer_id, 5)
    r = await client.post(f"{BASE}/{ids[0]}/read", headers=callers.customer)
    assert r.status_code == 200

    unread = (await client.get(BASE, params={"read": "false"}, headers=callers.customer)).json()
    assert unread["total"] == 4
    assert (
        await client.get(BASE, params={"unread_only": "true"}, headers=callers.customer)
    ).json()["total"] == 4
    read = (await client.get(BASE, params={"read": "true"}, headers=callers.customer)).json()
    assert [i["id"] for i in read["items"]] == [str(ids[0])]
    assert (await client.get(BASE, params={"type": "welcome"}, headers=callers.customer)).json()[
        "total"
    ] == 0

    # NTF-16: since_seq pages forward in ascending seq.
    seqs = [i["seq"] for i in (await client.get(BASE, headers=callers.customer)).json()["items"]]
    floor = sorted(seqs)[1]
    forward = (
        await client.get(BASE, params={"since_seq": floor, "limit": 2}, headers=callers.customer)
    ).json()
    assert [i["seq"] for i in forward["items"]] == sorted(seqs)[2:4]
    assert forward["total"] == 3

    clamped = (await client.get(BASE, params={"limit": 500}, headers=callers.customer)).json()
    assert clamped["limit"] == 100


async def test_list_rejects_bad_filters(client, callers) -> None:
    r = await client.get(
        BASE, params={"read": "true", "unread_only": "true"}, headers=callers.customer
    )
    assert r.status_code == 400
    assert r.json()["type"].endswith("/conflicting-read-filters")
    assert (
        await client.get(BASE, params={"type": "nope"}, headers=callers.customer)
    ).status_code == 422
    assert (
        await client.get(BASE, params={"since_seq": -1}, headers=callers.customer)
    ).status_code == 422


async def test_mark_read_is_idempotent_owner_only_and_publishes_the_count(
    app, client, callers, settings
) -> None:
    ids = await seed(app, callers.customer_id, 2)
    listener = create_redis(settings.redis_url)
    pubsub = listener.pubsub()
    await pubsub.subscribe(f"nt:user:{callers.customer_id}")
    try:
        first = await client.post(f"{BASE}/{ids[0]}/read", headers=callers.customer)
        assert first.status_code == 200
        assert first.json()["id"] == str(ids[0]) and first.json()["read_at"] is not None
        again = await client.post(f"{BASE}/{ids[0]}/read", headers=callers.customer)
        assert again.json()["read_at"] == first.json()["read_at"]  # I8

        message = None
        for _ in range(20):
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.2)
            if message is not None:
                break
        assert message is not None
        assert json.loads(message["data"]) == {"kind": "unread_count", "count": 1}
        # The repeat changed nothing, so it published nothing (NTF-11).
        assert await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.3) is None
    finally:
        await pubsub.aclose()
        await listener.aclose()

    # Another user's id and an unknown id are the same 404 (§3.1.2.2).
    assert (await client.post(f"{BASE}/{ids[1]}/read", headers=callers.other)).status_code == 404
    unknown = await client.post(f"{BASE}/{uuid.uuid4()}/read", headers=callers.customer)
    assert unknown.status_code == 404
    assert unknown.json()["type"].endswith("/notification-not-found")


async def test_email_rows_are_not_readable_through_the_owner_api(app, client, callers) -> None:
    await seed(app, callers.customer_id, 1)
    async with app.state.db.sessionmaker() as session:
        email_id = await session.scalar(
            select(Notification.id).where(Notification.channel == "email")
        )
    assert (
        await client.post(f"{BASE}/{email_id}/read", headers=callers.customer)
    ).status_code == 404


async def test_read_all_and_unread_count(app, client, callers) -> None:
    await seed(app, callers.customer_id, 3)
    await seed(app, callers.other_id, 1)
    count = await client.get(f"{BASE}/unread-count", headers=callers.customer)
    assert count.json() == {"count": 3}

    r = await client.post(f"{BASE}/read-all", headers=callers.customer)
    assert r.json() == {"updated_count": 3}
    assert (await client.post(f"{BASE}/read-all", headers=callers.customer)).json() == {
        "updated_count": 0
    }
    assert (await client.get(f"{BASE}/unread-count", headers=callers.customer)).json() == {
        "count": 0
    }
    assert (await client.get(f"{BASE}/unread-count", headers=callers.other)).json() == {"count": 1}


async def test_admin_view_is_admin_only(client, callers) -> None:
    for headers in (callers.customer, callers.merchant):
        r = await client.get(f"{BASE}/admin", headers=headers)
        assert r.status_code == 403


async def test_admin_view_lists_every_channel_with_attempts_and_filters(
    app, client, callers
) -> None:
    [in_app] = await seed(app, callers.customer_id, 1)
    await seed(app, callers.other_id, 1)
    async with app.state.db.sessionmaker() as session:
        email_id = await session.scalar(
            select(Notification.id).where(
                Notification.channel == "email",
                Notification.user_id == uuid.UUID(callers.customer_id),
            )
        )
        await session.execute(
            text(
                "INSERT INTO delivery_attempts (notification_id, channel, status, error, attempt)"
                " VALUES (:n, 'email', 'failed', 'boom', 1)"
            ),
            {"n": email_id},
        )
        await session.commit()

    everything = (await client.get(f"{BASE}/admin", headers=callers.admin)).json()
    assert everything["total"] == 4

    mine = (
        await client.get(
            f"{BASE}/admin", params={"user_id": callers.customer_id}, headers=callers.admin
        )
    ).json()
    assert {i["channel"] for i in mine["items"]} == {"in_app", "email"}
    email = next(i for i in mine["items"] if i["channel"] == "email")
    assert email["attempts"] == [
        {
            "attempt": 1,
            "channel": "email",
            "status": "failed",
            "error": "boom",
            "created_at": email["attempts"][0]["created_at"],
        }
    ]
    assert email["payload"] is None and email["delivery_scope"] == "all_devices"

    def ids(params):
        return client.get(f"{BASE}/admin", params=params, headers=callers.admin)

    failed = (await ids({"dispatch_status": "failed"})).json()
    assert [i["id"] for i in failed["items"]] == [str(email_id)]
    none = (await ids({"dispatch_status": "none"})).json()
    assert none["total"] == 1 and none["items"][0]["channel"] == "email"
    undelivered = (await ids({"undelivered": "true", "user_id": callers.customer_id})).json()
    assert [i["id"] for i in undelivered["items"]] == [str(in_app)]
    assert (await ids({"undelivered": "false"})).json()["total"] == 0
    assert (await ids({"channel": "in_app", "type": "order_confirmed"})).json()["total"] == 2
    assert (await ids({"delivery_scope": "origin_first"})).json()["total"] == 0
    assert (await ids({"correlation_id": "corr-1"})).json()["total"] == 4
    assert (await ids({"from": "2000-01-01T00:00:00Z", "to": "2100-01-01T00:00:00Z"})).json()[
        "total"
    ] == 4
    with_payload = (await ids({"include": "payload", "limit": 1})).json()
    assert with_payload["items"][0]["payload"]["event_type"] == "order.confirmed"
    assert (await ids({"dispatch_status": "weird"})).status_code == 422


# --- the ack (C23) -----------------------------------------------------------


async def _cursor(app, user_id: str, client_id: str) -> StreamCursor | None:
    async with app.state.db.sessionmaker() as session:
        return await session.scalar(
            select(StreamCursor).where(
                StreamCursor.user_id == uuid.UUID(user_id), StreamCursor.client_id == client_id
            )
        )


async def _row(app, notification_id) -> Notification:
    async with app.state.db.sessionmaker() as session:
        row = await session.get(Notification, notification_id)
        assert row is not None
        return row


async def test_ack_stamps_delivered_at_once_and_advances_the_cursor(app, client, callers) -> None:
    ids = await seed(app, callers.customer_id, 3)
    first, _, last = [await _row(app, i) for i in ids]
    body = {"client_id": "device-AAAA", "up_to_seq": last.seq, "ids": [str(ids[0]), str(ids[1])]}

    r = await client.post(f"{BASE}/ack", json=body, headers=callers.customer)
    assert r.status_code == 204 and r.content == b""
    stamped = await _row(app, ids[0])
    assert stamped.delivered_at is not None and stamped.delivered_client_id == "device-AAAA"
    assert (await _row(app, ids[2])).delivered_at is None  # never by range
    cursor = await _cursor(app, callers.customer_id, "device-AAAA")
    assert cursor is not None and cursor.cursor_seq == last.seq

    # Replay from another device: first writer wins; the cursor never goes down (G5e, I5).
    replay = {"client_id": "device-BBBB", "up_to_seq": first.seq, "ids": [str(ids[0])]}
    assert (
        await client.post(f"{BASE}/ack", json=replay, headers=callers.customer)
    ).status_code == 204
    again = await _row(app, ids[0])
    assert again.delivered_at == stamped.delivered_at and again.delivered_client_id == "device-AAAA"
    lower = {"client_id": "device-AAAA", "up_to_seq": first.seq}
    assert (
        await client.post(f"{BASE}/ack", json=lower, headers=callers.customer)
    ).status_code == 204
    assert (await _cursor(app, callers.customer_id, "device-AAAA")).cursor_seq == last.seq  # type: ignore[union-attr]


async def test_ack_of_a_foreign_or_unknown_id_is_204_and_touches_nothing(
    app, client, callers
) -> None:
    [theirs] = await seed(app, callers.other_id, 1)
    body = {"client_id": "device-AAAA", "ids": [str(theirs), str(uuid.uuid4())]}
    assert (
        await client.post(f"{BASE}/ack", json=body, headers=callers.customer)
    ).status_code == 204
    assert (await _row(app, theirs)).delivered_at is None
    assert await _cursor(app, callers.customer_id, "device-AAAA") is None  # no up_to_seq


async def test_ack_clamps_the_cursor_to_settled_rows(app, client, callers, settings) -> None:
    """The visibility trap: a high ack cannot move the cursor past rows still
    inside the window (I5)."""
    settings.seq_visibility_window_seconds = 3600
    ids = await seed(app, callers.customer_id, 2)
    newest = await _row(app, ids[1])
    body = {"client_id": "device-AAAA", "up_to_seq": newest.seq}
    assert (
        await client.post(f"{BASE}/ack", json=body, headers=callers.customer)
    ).status_code == 204
    assert (await _cursor(app, callers.customer_id, "device-AAAA")).cursor_seq == 0  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "body",
    [
        None,
        [],
        {},
        {"client_id": "short"},
        {"client_id": "device-AAAA", "up_to_seq": -1},
        {"client_id": "device-AAAA", "ids": ["nope"]},
        {"client_id": "device-AAAA", "unexpected": True},
    ],
)
async def test_a_malformed_ack_is_400(client, callers, body) -> None:
    r = await client.post(f"{BASE}/ack", json=body, headers=callers.customer)
    assert r.status_code == 400, r.text
    assert r.json()["type"].endswith("/malformed-ack")


async def test_too_many_ids_is_400(client, callers) -> None:
    body = {"client_id": "device-AAAA", "ids": [str(uuid.uuid4()) for _ in range(201)]}
    assert (
        await client.post(f"{BASE}/ack", json=body, headers=callers.customer)
    ).status_code == 400


async def test_ack_rate_limit_is_per_user_and_device(client, callers, settings) -> None:
    settings.ack_rate_limit_per_minute = 3
    body = {"client_id": "device-RATE1"}
    codes = [
        (await client.post(f"{BASE}/ack", json=body, headers=callers.customer)).status_code
        for _ in range(4)
    ]
    assert codes == [204, 204, 204, 429]
    other_device = {"client_id": "device-RATE2"}
    assert (
        await client.post(f"{BASE}/ack", json=other_device, headers=callers.customer)
    ).status_code == 204
    assert (await client.post(f"{BASE}/ack", json=body, headers=callers.other)).status_code == 204


async def test_ack_rate_limit_fails_open_without_redis(app, client, callers) -> None:
    real = app.state.redis
    app.state.redis = create_redis("redis://127.0.0.1:1/0")
    try:
        body = {"client_id": "device-AAAA"}
        assert (
            await client.post(f"{BASE}/ack", json=body, headers=callers.customer)
        ).status_code == 204
    finally:
        await app.state.redis.aclose()
        app.state.redis = real


async def test_health(client) -> None:
    assert (await client.get("/health")).json() == {"status": "ok"}
    ready = await client.get("/health/ready")
    assert ready.status_code == 200
    assert ready.json()["checks"] == {"database": "ok", "redis": "ok"}


async def test_readiness_fails_without_redis(app, client) -> None:
    real = app.state.redis
    dead = create_redis("redis://127.0.0.1:1/0")
    from app.health.routes import build_router

    app.router.routes = [r for r in app.router.routes if getattr(r, "path", "") != "/health/ready"]
    app.include_router(build_router(app.state.db, dead))
    try:
        ready = await client.get("/health/ready")
        assert ready.status_code == 503
        assert ready.json()["checks"]["redis"] == "failed"
    finally:
        await dead.aclose()
        app.state.redis = real
