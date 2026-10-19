"""Frame serialisation and the pure per-connection logic (no I/O)."""

import json
from datetime import UTC, datetime

from app.v1.stream import frames
from app.v1.stream.catchup import StreamMode, choose_floor
from app.v1.stream.connection import CLOSE, REASON_SLOW, Connection, render_live
from app.v1.stream.fanout import notification_message, unread_count_message
from app.v1.stream.routes import _last_event_id


def parse(chunk: str) -> dict:
    fields: dict = {}
    for line in chunk.strip("\n").split("\n"):
        key, _, value = line.partition(": ")
        fields[key] = value
    if "data" in fields:
        fields["data"] = json.loads(fields["data"])
    return fields


def test_frame_layout() -> None:
    assert frames.frame("ping") == "event: ping\ndata: {}\n\n"
    assert frames.frame("x", {"a": 1}, id=7, retry_ms=3000) == (
        'retry: 3000\nid: 7\nevent: x\ndata: {"a":1}\n\n'
    )
    assert frames.comment() == ": keepalive\n\n"


def test_hello_notification_and_counts() -> None:
    hello = parse(
        frames.hello(
            client_id="c" * 8, cold_start=True, unread_count=3, server_seq=9, resumed_from=None
        )
    )
    assert hello["event"] == "hello"
    assert hello["data"] == {
        "client_id": "cccccccc",
        "cold_start": True,
        "unread_count": 3,
        "server_seq": 9,
        "resumed_from": None,
    }
    created = datetime(2026, 10, 15, tzinfo=UTC)
    n = parse(
        frames.notification(
            id="abc", seq=42, type="welcome", title="t", body="b", created_at=created, origin=True
        )
    )
    assert n["id"] == "42" and n["event"] == "notification"
    assert n["data"]["created_at"] == created.isoformat() and n["data"]["origin"] is True
    assert parse(frames.unread_count(5))["data"] == {"count": 5}
    assert parse(frames.catchup_truncated(omitted_count=150, oldest_seq_sent=1))["data"] == {
        "omitted_count": 150,
        "oldest_seq_sent": 1,
    }


def test_error_frames_are_problem_details() -> None:
    expired = parse(frames.token_expired())["data"]
    assert expired["type"].endswith("/stream-token-expired") and expired["status"] == 401
    slow = parse(frames.slow_consumer())["data"]
    assert slow["type"].endswith("/stream-slow-consumer")
    assert "detail" not in parse(frames.error(slug="x", title="X", status=500))["data"]


def test_retry_hint_jitter() -> None:
    assert frames.retry_hint(3000, 0) == 3000
    assert frames.retry_hint(3000, 2000, rand=lambda lo, hi: hi) == 5000
    assert 3000 <= frames.retry_hint(3000, 2000) <= 5000


def _message(id: str = "n1", origin: str | None = None) -> dict:
    return notification_message(
        id=id,
        seq=5,
        type="order_confirmed",
        title="t",
        body="b",
        created_at="2026-10-15T00:00:00+00:00",
        origin_client_id=origin,
        scope="origin_first",
        unread_count=2,
    )


def test_render_live() -> None:
    chunks = render_live(_message(origin="client-AAA"), "client-AAA", set())
    assert [parse(c)["event"] for c in chunks] == ["notification", "unread_count"]
    assert parse(chunks[0])["data"]["origin"] is True
    # Already replayed by the catch-up: only the count (NTF-10).
    assert [parse(c)["event"] for c in render_live(_message(), None, {"n1"})] == ["unread_count"]
    # Broadcast (scoped delivery deferred): another device still gets the frame.
    assert (
        parse(render_live(_message(origin="client-AAA"), "client-BBB", set())[0])["data"]["origin"]
        is False
    )
    assert render_live(unread_count_message(4), None, set()) == [frames.unread_count(4)]
    assert render_live({"kind": "mystery"}, None, set()) == []


def test_choose_floor() -> None:
    assert choose_floor(client_id=None, cursor_seq=None, last_event_id=None) == (
        StreamMode.COLD,
        None,
    )
    assert choose_floor(client_id=None, cursor_seq=None, last_event_id=7) == (
        StreamMode.EPHEMERAL,
        7,
    )
    assert choose_floor(client_id="c", cursor_seq=None, last_event_id=7) == (StreamMode.COLD, None)
    assert choose_floor(client_id="c", cursor_seq=10, last_event_id=7) == (StreamMode.RESUME, 10)
    assert choose_floor(client_id="c", cursor_seq=10, last_event_id=12) == (StreamMode.RESUME, 12)
    assert choose_floor(client_id="c", cursor_seq=10, last_event_id=None) == (StreamMode.RESUME, 10)


def test_last_event_id_is_a_hint() -> None:
    assert _last_event_id(None) is None
    assert _last_event_id(" 84213 ") == 84213
    assert _last_event_id("abc") is None
    assert _last_event_id("-3") is None


def test_connection_closes_on_overflow_instead_of_buffering() -> None:
    conn = Connection("u", None, queue_max=2)
    conn.offer({"kind": "unread_count", "count": 1})
    conn.offer({"kind": "unread_count", "count": 2})
    conn.offer({"kind": "unread_count", "count": 3})  # overflow
    assert conn.closed == REASON_SLOW
    assert conn.queue.qsize() == 1
    assert conn.queue.get_nowait() == {"kind": CLOSE, "reason": REASON_SLOW}
    conn.offer({"kind": "unread_count", "count": 4})  # ignored once closed
    conn.abort("again")  # idempotent
    assert conn.queue.empty() and conn.closed == REASON_SLOW
