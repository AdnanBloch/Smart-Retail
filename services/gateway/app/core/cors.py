"""CORS at the edge (RFC 0002 GW-19).

Preflight is answered here and never proxied: an allowed origin gets 204
with CORS headers, a disallowed one gets 204 without them. CORS headers are
added to every other response, errors included.
"""

from starlette.types import ASGIApp, Message, Receive, Scope, Send

ALLOW_METHODS = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
ALLOW_HEADERS = (
    "Authorization, Content-Type, Idempotency-Key, X-Request-ID, X-Client-Id, "
    "Last-Event-ID, traceparent"
)
EXPOSE_HEADERS = (
    "X-Request-ID, traceparent, Location, Retry-After, "
    "RateLimit-Limit, RateLimit-Remaining, RateLimit-Reset"
)


class EdgeCORSMiddleware:
    def __init__(self, app: ASGIApp, *, origins: frozenset[str], max_age: int) -> None:
        self.app = app
        self.origins = origins
        self.max_age = max_age

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = dict(scope.get("headers", []))
        origin = headers.get(b"origin", b"").decode("latin-1")
        allowed = origin in self.origins

        if scope["method"] == "OPTIONS" and b"access-control-request-method" in headers:
            response_headers = [(b"vary", b"Origin")]
            if allowed:
                response_headers += [
                    (b"access-control-allow-origin", origin.encode("latin-1")),
                    (b"access-control-allow-methods", ALLOW_METHODS.encode()),
                    (b"access-control-allow-headers", ALLOW_HEADERS.encode()),
                    (b"access-control-max-age", str(self.max_age).encode()),
                ]
            await send({"type": "http.response.start", "status": 204, "headers": response_headers})
            await send({"type": "http.response.body", "body": b""})
            return

        async def send_with_cors(message: Message) -> None:
            if message["type"] == "http.response.start" and allowed:
                message["headers"] = [
                    *message.get("headers", []),
                    (b"access-control-allow-origin", origin.encode("latin-1")),
                    (b"access-control-expose-headers", EXPOSE_HEADERS.encode()),
                    (b"vary", b"Origin"),
                ]
            await send(message)

        await self.app(scope, receive, send_with_cors)
