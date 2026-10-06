"""Cross-entity FastAPI dependencies: role guards and the optional caller.

Each entity's ``dependencies.py`` builds its repos.
"""

from app.core.enums import CallerRole
from smartretail_common.auth import optional_principal, require_roles

__all__ = ["optional_principal", "require_admin", "require_catalog_writer", "require_merchant"]

require_admin = require_roles(CallerRole.ADMIN)
require_merchant = require_roles(CallerRole.MERCHANT)
require_catalog_writer = require_roles(CallerRole.MERCHANT, CallerRole.ADMIN)
