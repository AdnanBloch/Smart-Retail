"""Cross-entity FastAPI dependencies: role guards and the optional caller.

Each entity's ``dependencies.py`` builds its repos.
"""

from fastapi import Request

from app.core.config import Settings
from app.core.enums import CallerRole
from smartretail_common.auth import optional_principal, require_roles

__all__ = [
    "get_settings",
    "optional_principal",
    "require_admin",
    "require_catalog_writer",
    "require_merchant",
]

require_admin = require_roles(CallerRole.ADMIN)
require_merchant = require_roles(CallerRole.MERCHANT)
require_catalog_writer = require_roles(CallerRole.MERCHANT, CallerRole.ADMIN)


def get_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings
