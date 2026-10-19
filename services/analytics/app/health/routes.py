"""``/health`` and ``/health/ready``. Mounted at the root, outside any API version.

Ready means Mongo answers as a writable replica-set primary (ANL-1).
"""

from fastapi import APIRouter

from app.core.db import Mongo
from smartretail_common.health import Check, health_router


def mongo_check(mongo: Mongo) -> Check:
    async def check() -> bool:
        return await mongo.ping()

    return check


def build_router(mongo: Mongo) -> APIRouter:
    return health_router({"mongo": mongo_check(mongo)})
