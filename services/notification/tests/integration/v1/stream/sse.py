"""A real uvicorn server on an ephemeral port and a minimal SSE reader.

httpx's ASGITransport buffers a response body until it completes, so a
stream that never ends cannot be tested through it.
"""

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import httpx
import uvicorn


class LiveServer:
    def __init__(self, app: Any) -> None:
        self.app = app
        self.server = uvicorn.Server(
            uvicorn.Config(
                app,
                host="127.0.0.1",
                port=0,
                lifespan="on",
                ws="none",
                log_level="warning",
                timeout_graceful_shutdown=1,
            )
        )
        self.task: asyncio.Task[None] | None = None

    @property
    def url(self) -> str:
        port = self.server.servers[0].sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}"

    async def start(self) -> "LiveServer":
        self.task = asyncio.create_task(self.server.serve())
        for _ in range(500):
            if self.server.started:
                return self
            await asyncio.sleep(0.01)
        raise RuntimeError("server did not start")

    async def stop(self) -> None:
        self.server.should_exit = True
        if self.task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(self.task, 10)


@dataclass
class Event:
    event: str | None = None
    data: Any = None
    id: str | None = None
    retry: int | None = None
    comments: list[str] = field(default_factory=list)


class SseStream:
    def __init__(self, response: httpx.Response) -> None:
        self.response = response
        self._lines: AsyncIterator[str] = response.aiter_lines()

    async def next(self, timeout: float = 5.0) -> Event | None:  # noqa: ASYNC109
        """The next event (a comment-only block counts); ``None`` at end of stream."""
        event = Event()
        seen = False
        while True:
            try:
                line = await asyncio.wait_for(anext(self._lines), timeout)
            except StopAsyncIteration:
                return event if seen else None
            if line == "":
                if seen:
                    return event
                continue
            seen = True
            if line.startswith(":"):
                event.comments.append(line[1:].strip())
                continue
            key, _, value = line.partition(":")
            value = value[1:] if value.startswith(" ") else value
            if key == "event":
                event.event = value
            elif key == "data":
                event.data = json.loads(value)
            elif key == "id":
                event.id = value
            elif key == "retry":
                event.retry = int(value)

    async def until(
        self,
        name: str,
        timeout: float = 5.0,  # noqa: ASYNC109
    ) -> tuple[Event, list[Event]]:
        """Reads until an event named ``name``; returns it and everything before it."""
        before: list[Event] = []
        while True:
            event = await self.next(timeout)
            if event is None:
                raise AssertionError(f"stream ended before {name!r}; got {before}")
            if event.event == name:
                return event, before
            before.append(event)

    async def opening(self) -> tuple[Event, list[Event]]:
        """``hello`` and everything up to and including the connect-time ``unread_count``."""
        hello = await self.next()
        assert hello is not None and hello.event == "hello", hello
        _, frames = await self.until("unread_count")
        return hello, frames


async def ticket(
    client: httpx.AsyncClient, headers: dict[str, str], client_id: str | None = None
) -> str:
    extra = {"X-Client-Id": client_id} if client_id else {}
    r = await client.post("/api/v1/notifications/stream-ticket", headers={**headers, **extra})
    assert r.status_code == 200, r.text
    return str(r.json()["ticket"])


@contextlib.asynccontextmanager
async def open_stream(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    *,
    client_id: str | None = None,
    last_event_id: int | None = None,
) -> AsyncIterator[SseStream]:
    token = await ticket(client, headers, client_id)
    extra = {"Last-Event-ID": str(last_event_id)} if last_event_id is not None else {}
    async with client.stream(
        "GET", "/api/v1/notifications/stream", params={"ticket": token}, headers=extra
    ) as response:
        assert response.status_code == 200, await response.aread()
        yield SseStream(response)
