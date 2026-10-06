"""``/health`` and ``/health/ready``. Mounted at the root, outside ``/api/v1``."""

from fastapi import APIRouter

from smartretail_common.db import Database
from smartretail_common.health import database_check, health_router


def build_router(db: Database) -> APIRouter:
    return health_router({"database": database_check(db)})
