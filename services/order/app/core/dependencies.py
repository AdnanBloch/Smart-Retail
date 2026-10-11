"""Cross-entity FastAPI dependencies: role guards, settings, the bearer token.

User-facing routes refuse service tokens (``require_roles`` does).
"""

from fastapi import Request

from app.core.config import Settings
from app.core.enums import CallerRole
from smartretail_common.auth import bearer_token, require_roles

__all__ = [
    "get_settings",
    "require_admin",
    "require_customer",
    "require_customer_or_admin",
    "require_merchant",
    "user_token",
]

require_customer = require_roles(CallerRole.CUSTOMER)
require_merchant = require_roles(CallerRole.MERCHANT)
require_admin = require_roles(CallerRole.ADMIN)
require_customer_or_admin = require_roles(CallerRole.CUSTOMER, CallerRole.ADMIN)


def get_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


def user_token(request: Request) -> str:
    """The caller's own token, forwarded to catalog for the snapshot (A13)."""
    token = bearer_token(request)
    assert token is not None  # the route's role guard already required one
    return token
