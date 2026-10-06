"""Per-request context: request id and authenticated subject.

The gateway generates ``X-Request-ID``; services adopt the inbound value (or
generate one when called directly) and echo it on every response. Until
tracing lands in Milestone 3 the value is a random UUID.
"""

import uuid
from contextvars import ContextVar

from starlette.types import ASGIApp, Message, Receive, Scope, Send

REQUEST_ID_HEADER = "x-request-id"

_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)
_user_id: ContextVar[str | None] = ContextVar("user_id", default=None)


def get_request_id() -> str | None:
    return _request_id.get()


def set_request_id(value: str) -> None:
    _request_id.set(value)


def get_user_id() -> str | None:
    return _user_id.get()


def set_user_id(value: str | None) -> None:
    _user_id.set(value)


def _valid_inbound(value: str) -> bool:
    return 0 < len(value) <= 128 and value.isprintable() and " " not in value


class RequestContextMiddleware:
    """Pure ASGI middleware so streamed bodies are never buffered.

    ``adopt_inbound`` is ``True`` for services behind the gateway and ``False``
    at the gateway itself, which always generates a fresh id (RFC 0002 §2.5).
    """

    def __init__(self, app: ASGIApp, *, adopt_inbound: bool = True) -> None:
        self.app = app
        self.adopt_inbound = adopt_inbound

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id: str | None = None
        if self.adopt_inbound:
            for name, value in scope.get("headers", []):
                if name == b"x-request-id":
                    decoded = value.decode("latin-1")
                    if _valid_inbound(decoded):
                        request_id = decoded
                    break
        if request_id is None:
            request_id = str(uuid.uuid4())

        # Not reset afterwards: each request runs in its own task, and the
        # outermost error middleware still needs the value to stamp a 500.
        _request_id.set(request_id)
        _user_id.set(None)
        scope.setdefault("state", {})["request_id"] = request_id

        async def send_with_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = [
                    (k, v) for k, v in message.get("headers", []) if k.lower() != b"x-request-id"
                ]
                headers.append((b"x-request-id", request_id.encode("latin-1")))
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_id)
