"""Cross-entity FastAPI dependencies: app state, role guards, client address.

Each entity's ``dependencies.py`` builds its repos from these.
"""

from fastapi import Request

from app.core.config import Settings
from app.core.enums import UserRole
from app.core.security import KeyRing, PasswordHasher
from smartretail_common.auth import current_principal, optional_principal, require_roles

__all__ = [
    "client_ip",
    "current_principal",
    "get_hasher",
    "get_keyring",
    "get_settings",
    "optional_principal",
    "require_admin",
    "require_merchant",
]

require_admin = require_roles(UserRole.ADMIN)
require_merchant = require_roles(UserRole.MERCHANT)


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_hasher(request: Request) -> PasswordHasher:
    return request.app.state.hasher


def get_keyring(request: Request) -> KeyRing:
    return request.app.state.keyring


def client_ip(request: Request) -> str | None:
    # identity is reachable only through the gateway, which sets this header.
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()[:64]
    return request.client.host if request.client else None
