"""Cross-entity FastAPI dependencies: the caller, the admin guard, settings, Redis.

Each entity's ``dependencies.py`` builds its repos.
"""

from typing import TYPE_CHECKING

from fastapi import Request

from app.core.config import Settings
from app.core.enums import CallerRole
from smartretail_common.auth import current_principal, require_roles

if TYPE_CHECKING:
    from redis.asyncio import Redis

__all__ = ["current_principal", "get_redis", "get_settings", "require_admin"]

require_admin = require_roles(CallerRole.ADMIN)


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_redis(request: Request) -> "Redis":
    return request.app.state.redis
