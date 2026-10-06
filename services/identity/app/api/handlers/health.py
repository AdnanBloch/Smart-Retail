"""``/health`` and ``/health/ready``. Mounted at the root, outside ``/api/v1``."""

from fastapi import APIRouter

from app.infrastructure.security import KeyRing
from smartretail_common.db import Database
from smartretail_common.health import database_check, health_router


def build_router(db: Database, keyring: KeyRing) -> APIRouter:
    async def signing_key_loaded() -> bool:
        return bool(keyring.active_kid)

    return health_router({"database": database_check(db), "signing_key": signing_key_loaded})
