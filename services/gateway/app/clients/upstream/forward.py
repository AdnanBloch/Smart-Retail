"""Streamed forwarding to upstreams (RFC 0002 GW-6, GW-11, GW-14).

One pooled client per upstream. Bodies stream in both directions and are
never re-serialised; the gateway never retries.
"""

from collections.abc import AsyncIterator

import httpx
from fastapi import Request
from starlette.background import BackgroundTask
from starlette.responses import Response, StreamingResponse

from app.clients.upstream.errors import (
    PayloadTooLarge,
    UpstreamBadResponse,
    UpstreamError,
    UpstreamTimeout,
    UpstreamUnavailable,
)
from smartretail_common.context import get_request_id

HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
    }
)
# Identity is never asserted by headers (GW-5): services verify the JWT.
STRIPPED_PREFIXES = ("x-user-", "x-internal-")
STRIPPED_REQUEST = (
    HOP_BY_HOP
    | {"host", "content-length", "x-request-id"}
    | {
        "x-forwarded-for",
        "x-forwarded-proto",
        "x-forwarded-host",
    }
)
STRIPPED_RESPONSE = HOP_BY_HOP | {"x-request-id"}


def request_headers(request: Request) -> dict[str, str]:
    headers = {
        k: v
        for k, v in request.headers.items()
        if k not in STRIPPED_REQUEST and not k.startswith(STRIPPED_PREFIXES)
    }
    client_host = request.client.host if request.client else ""
    headers["x-forwarded-for"] = client_host
    headers["x-forwarded-proto"] = request.url.scheme
    headers["x-forwarded-host"] = request.headers.get("host", "")
    if request_id := get_request_id():
        headers["x-request-id"] = request_id
    if "content-length" in request.headers:
        headers["content-length"] = request.headers["content-length"]
    return headers


async def read_body(request: Request, limit: int) -> bytes | AsyncIterator[bytes]:
    """Checks the size limit before any byte reaches the upstream.

    A declared Content-Length is checked up front and the body is streamed
    with a guard; a chunked body (no length) is buffered up to the limit.
    """
    declared = request.headers.get("content-length")
    if declared is not None:
        try:
            length = int(declared)
        except ValueError as exc:
            raise PayloadTooLarge(limit) from exc
        if length > limit:
            raise PayloadTooLarge(limit)

        async def guarded() -> AsyncIterator[bytes]:
            seen = 0
            async for chunk in request.stream():
                seen += len(chunk)
                if seen > length:
                    raise PayloadTooLarge(limit)
                yield chunk

        return guarded()

    buffer = bytearray()
    async for chunk in request.stream():
        buffer.extend(chunk)
        if len(buffer) > limit:
            raise PayloadTooLarge(limit)
    return bytes(buffer)


def make_client(
    base_url: str,
    *,
    connect: float,
    read: float,
    max_connections: int,
    keepalive: int,
    transport: httpx.AsyncBaseTransport | None = None,
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=base_url,
        transport=transport,
        timeout=httpx.Timeout(connect=connect, read=read, write=read, pool=connect),
        limits=httpx.Limits(max_connections=max_connections, max_keepalive_connections=keepalive),
        follow_redirects=False,
    )


async def forward(
    request: Request,
    *,
    upstream: str,
    client: httpx.AsyncClient | None,
    body_limit: int,
    read_timeout: float,
) -> Response:
    if client is None:
        raise UpstreamUnavailable(upstream)
    body = await read_body(request, body_limit)
    upstream_request = client.build_request(
        request.method,
        request.url.path,
        params=request.url.query.encode("latin-1") if request.url.query else None,
        headers=request_headers(request),
        content=body,
    )
    try:
        response = await client.send(upstream_request, stream=True)
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
        raise UpstreamUnavailable(upstream) from exc
    except httpx.ReadTimeout as exc:
        raise UpstreamTimeout(upstream, read_timeout) from exc
    except (httpx.RemoteProtocolError, httpx.ReadError, httpx.WriteError) as exc:
        raise UpstreamBadResponse(upstream) from exc

    content_type = response.headers.get("content-type", "")
    if response.status_code >= 500 and not content_type.startswith("application/problem+json"):
        await response.aclose()
        raise UpstreamError(upstream, response.status_code)

    headers = [
        (k, v) for k, v in response.headers.multi_items() if k.lower() not in STRIPPED_RESPONSE
    ]

    async def body_stream() -> AsyncIterator[bytes]:
        if response.is_stream_consumed:  # already buffered by the transport
            yield response.content
            return
        try:
            async for chunk in response.aiter_raw():
                yield chunk
        except httpx.HTTPError:
            # Headers are already sent; all we can do is end the stream.
            return

    streamed = StreamingResponse(
        body_stream(), status_code=response.status_code, background=BackgroundTask(response.aclose)
    )
    streamed.raw_headers = [(k.encode("latin-1"), v.encode("latin-1")) for k, v in headers]
    return streamed
