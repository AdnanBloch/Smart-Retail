"""Liveness and readiness endpoints (RFC 0001 §3.1.1.1).

``/health`` checks nothing external. ``/health/ready`` runs each check and
returns 503 if any fails.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from smartretail_common.db import Database

logger = logging.getLogger(__name__)

Check = Callable[[], Awaitable[bool]]


def database_check(db: Database) -> Check:
    async def check() -> bool:
        return await db.ping()

    return check


async def _run(check: Check, seconds: float) -> bool:
    try:
        return bool(await asyncio.wait_for(check(), seconds))
    except Exception as exc:  # any failure means not ready
        logger.warning("readiness check failed", extra={"error": repr(exc)})
        return False


def health_router(checks: Mapping[str, Check], *, timeout: float = 2.0) -> APIRouter:
    router = APIRouter(tags=["operational"])

    @router.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/health/ready")
    async def ready() -> JSONResponse:
        names = list(checks)
        results = await asyncio.gather(*(_run(checks[n], timeout) for n in names))
        outcome = {n: "ok" if r else "failed" for n, r in zip(names, results, strict=True)}
        ok = all(results)
        return JSONResponse(
            {"status": "ready" if ok else "not_ready", "checks": outcome},
            status_code=200 if ok else 503,
        )

    return router
