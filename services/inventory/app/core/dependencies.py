"""Cross-entity FastAPI dependencies: role guards and the internal scope guard.

Each entity's ``dependencies.py`` builds its repos.
"""

from fastapi import Request

from app.core.config import Settings
from app.core.enums import CallerRole
from smartretail_common.auth import require_roles, require_scope

__all__ = ["get_settings", "require_internal", "require_stock_manager"]

# The scope an internal route requires on a service token (INVENTORY_SERVICE_SCOPE).
INTERNAL_SCOPE = "inventory:internal"

require_stock_manager = require_roles(CallerRole.MERCHANT, CallerRole.ADMIN)
require_internal = require_scope(INTERNAL_SCOPE)


def get_settings(request: Request) -> Settings:
    return request.app.state.settings
