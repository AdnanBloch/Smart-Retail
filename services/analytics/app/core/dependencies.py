"""Cross-entity FastAPI dependencies: the role guards of RFC 0008 §3.1.2.2."""

from fastapi import Request

from app.core.config import Settings
from app.v1.metrics.enums import UserRole
from smartretail_common.auth import require_roles

__all__ = ["get_settings", "require_admin", "require_analyst"]

# overview, failures: admin only (their figures can name other merchants).
require_admin = require_roles(UserRole.ADMIN)
# orders, revenue, popularity, stockouts: a merchant (own scope) or an admin.
require_analyst = require_roles(UserRole.MERCHANT, UserRole.ADMIN)


def get_settings(request: Request) -> Settings:
    return request.app.state.settings
