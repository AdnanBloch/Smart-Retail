"""SSE frame serialisers (RFC 0007 §3.1.1.4, the frame contract). No I/O.

Typed events: ``hello``, ``notification``, ``catchup_truncated``,
``unread_count``, ``ping``, ``error``; plus ``: keepalive`` comments.
"""

import json
import random
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from smartretail_common.errors import PROBLEM_TYPE_BASE


def _json(data: Mapping[str, Any]) -> str:
    return json.dumps(data, separators=(",", ":"), default=str)


def frame(
    event: str,
    data: Mapping[str, Any] | None = None,
    *,
    id: int | None = None,
    retry_ms: int | None = None,
) -> str:
    lines = []
    if retry_ms is not None:
        lines.append(f"retry: {retry_ms}")
    if id is not None:
        lines.append(f"id: {id}")
    lines.append(f"event: {event}")
    lines.append(f"data: {_json(data or {})}")
    return "\n".join(lines) + "\n\n"


def comment(text: str = "keepalive") -> str:
    return f": {text}\n\n"


def retry_hint(
    base_ms: int, jitter_ms: int, rand: Callable[[int, int], int] = random.randint
) -> int:
    """Jittered ``retry:`` so reconnects after a deploy spread out (NTF-D23)."""
    return base_ms + (rand(0, jitter_ms) if jitter_ms > 0 else 0)


def hello(
    *,
    client_id: str | None,
    cold_start: bool,
    unread_count: int,
    server_seq: int,
    resumed_from: int | None,
    retry_ms: int | None = None,
) -> str:
    return frame(
        "hello",
        {
            "client_id": client_id,
            "cold_start": cold_start,
            "unread_count": unread_count,
            "server_seq": server_seq,
            "resumed_from": resumed_from,
        },
        retry_ms=retry_ms,
    )


def notification(
    *,
    id: str,
    seq: int,
    type: str,
    title: str,
    body: str,
    created_at: datetime | str,
    origin: bool,
) -> str:
    created = created_at.isoformat() if isinstance(created_at, datetime) else created_at
    return frame(
        "notification",
        {
            "id": id,
            "seq": seq,
            "type": type,
            "title": title,
            "body": body,
            "created_at": created,
            "origin": origin,
        },
        id=seq,
    )


def catchup_truncated(*, omitted_count: int, oldest_seq_sent: int | None) -> str:
    return frame(
        "catchup_truncated",
        {"omitted_count": omitted_count, "oldest_seq_sent": oldest_seq_sent},
    )


def unread_count(count: int) -> str:
    return frame("unread_count", {"count": count})


def ping() -> str:
    return frame("ping")


def error(*, slug: str, title: str, status: int, detail: str | None = None) -> str:
    """An RFC 9457 problem detail, sent before the stream closes (A7)."""
    problem: dict[str, Any] = {"type": PROBLEM_TYPE_BASE + slug, "title": title, "status": status}
    if detail:
        problem["detail"] = detail
    return frame("error", problem)


def token_expired() -> str:
    return error(
        slug="stream-token-expired",
        title="The stream's token has expired",
        status=401,
        detail="Re-authenticate and open a new stream",
    )


def slow_consumer() -> str:
    return error(
        slug="stream-slow-consumer",
        title="The client fell too far behind",
        status=503,
        detail="Reconnect; missed notifications are replayed by seq",
    )
