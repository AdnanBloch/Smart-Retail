"""Server-Sent Events framing and the stream pump (E3, E5, AST-D19).

Both streaming endpoints (chat and generate) produce typed events through a
producer task; :func:`pump` turns its queue into SSE text, interleaving a
keep-alive comment while the producer is quiet and noticing a client
disconnect, which cancels the producer (so the provider stream stops costing).
"""

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import anyio

from smartretail_common.context import get_request_id
from smartretail_common.errors import PROBLEM_TYPE_BASE

EVENT_TYPES = ("status", "sources", "token", "usage", "done", "error")
KEEPALIVE = ": keepalive\n\n"
SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",
    "Connection": "keep-alive",
}


@dataclass(frozen=True)
class SseEvent:
    event: str
    data: dict[str, Any]

    def encode(self) -> str:
        return format_event(self.event, self.data)


def format_event(event: str, data: dict[str, Any]) -> str:
    if event not in EVENT_TYPES:
        raise ValueError(f"unknown SSE event type {event!r}")
    payload = json.dumps(data, separators=(",", ":"), ensure_ascii=False, default=str)
    return f"event: {event}\ndata: {payload}\n\n"


def problem_event(
    *, status: int, type_slug: str, title: str, detail: str | None, instance: str
) -> SseEvent:
    body: dict[str, Any] = {
        "type": PROBLEM_TYPE_BASE + type_slug,
        "title": title,
        "status": status,
        "instance": instance,
    }
    if detail:
        body["detail"] = detail
    if request_id := get_request_id():
        body["request_id"] = request_id
    return SseEvent("error", body)


Emit = Callable[[SseEvent], Awaitable[None]]
_END = object()


async def pump(
    produce: Callable[[Emit], Awaitable[None]],
    *,
    keepalive_seconds: float,
    is_disconnected: Callable[[], Awaitable[bool]] | None = None,
    poll_seconds: float = 0.25,
    on_close: Callable[[], None] | None = None,
) -> AsyncIterator[str]:
    """Runs ``produce`` in its own task and yields its events as SSE text."""
    queue: asyncio.Queue[Any] = asyncio.Queue()

    async def emit(event: SseEvent) -> None:
        await queue.put(event)

    async def run() -> None:
        try:
            await produce(emit)
        finally:
            queue.put_nowait(_END)

    task = asyncio.create_task(run())
    loop = asyncio.get_running_loop()
    last_sent = loop.time()
    try:
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=poll_seconds)
            except TimeoutError:
                if is_disconnected is not None and await is_disconnected():
                    return
                if loop.time() - last_sent >= keepalive_seconds:
                    last_sent = loop.time()
                    yield KEEPALIVE
                continue
            if item is _END:
                if not task.cancelled() and task.exception() is not None:
                    raise task.exception()  # type: ignore[misc]
                return
            last_sent = loop.time()
            yield item.encode()
    finally:
        if not task.done():
            task.cancel()
        # The producer persists the partial answer on cancellation; let it
        # finish even though this generator is itself being cancelled.
        with anyio.CancelScope(shield=True), contextlib.suppress(BaseException):
            await asyncio.wait({task}, timeout=10)
        if on_close is not None:
            on_close()
