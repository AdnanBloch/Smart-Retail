"""``/health`` and ``/health/ready``. Mounted at the root, outside any API version.

Readiness covers ``notification_db`` and Redis: without Redis no stream can
open (§3.1.1.5).
"""

from typing import TYPE_CHECKING

from fastapi import APIRouter

from smartretail_common.db import Database
from smartretail_common.health import Check, database_check, health_router

if TYPE_CHECKING:
    from redis.asyncio import Redis


def redis_check(redis: "Redis") -> Check:
    async def check() -> bool:
        return bool(await redis.ping())

    return check


def build_router(db: Database, redis: "Redis") -> APIRouter:
    return health_router({"database": database_check(db), "redis": redis_check(redis)})
