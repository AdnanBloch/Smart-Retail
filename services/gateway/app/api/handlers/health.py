"""Gateway health. Never proxied, exempt from auth (RFC 0002 GW-16)."""

import asyncio
from typing import Any

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(include_in_schema=False)


async def get_health() -> dict[str, str]:
    return {"status": "ok"}


async def _probe(client: httpx.AsyncClient | None) -> str:
    if client is None:
        return "not_configured"
    try:
        response = await client.get("/health", timeout=1.0)
        return "ok" if response.status_code == 200 else "unreachable"
    except httpx.HTTPError:
        return "unreachable"


async def get_ready(request: Request) -> JSONResponse:
    state = request.app.state
    checks = {
        "settings": "ok",
        "public_key": "ok" if state.verifier.has_keys else "failed",
        "route_table": "ok" if state.routes.rules else "failed",
    }
    names = list(state.upstream_names)
    statuses = await asyncio.gather(*(_probe(state.clients.get(n)) for n in names))
    ok = all(v == "ok" for v in checks.values())
    body: dict[str, Any] = {
        "status": "ready" if ok else "not_ready",
        "checks": checks,
        # Informational only: an unreachable upstream does not make the gateway unready.
        "informational": {"upstreams": dict(zip(names, statuses, strict=True))},
    }
    return JSONResponse(body, status_code=200 if ok else 503)
