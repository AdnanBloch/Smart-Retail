"""Cross-entity FastAPI dependencies: role guards, settings and shared clients.

Each entity's ``dependencies.py`` builds its repos.
"""

import hashlib
from typing import Any

from fastapi import Request

from app.core.clients.catalog import CatalogClient
from app.core.clients.inventory import InventoryClient
from app.core.clients.llm import LLMProvider
from app.core.config import Settings
from app.core.enums import CallerRole
from smartretail_common.auth import Principal, optional_principal, require_roles
from smartretail_common.errors import Forbidden

__all__ = [
    "Principal",
    "caller_ip_scope",
    "get_catalog",
    "get_inventory",
    "get_llm",
    "get_settings",
    "optional_principal",
    "require_admin",
    "require_customer",
    "require_merchant",
    "require_user",
]

require_admin = require_roles(CallerRole.ADMIN)
require_customer = require_roles(CallerRole.CUSTOMER)
require_merchant = require_roles(CallerRole.MERCHANT)
require_user = require_roles(CallerRole.CUSTOMER, CallerRole.MERCHANT, CallerRole.ADMIN)
require_merchant_or_admin = require_roles(CallerRole.MERCHANT, CallerRole.ADMIN)


async def optional_customer(request: Request) -> Principal | None:
    """Chat is for customers and anonymous callers; merchants and admins are
    not granted the customer surface (§3.1.2.2)."""
    principal = await optional_principal(request)
    if principal is not None and principal.role != CallerRole.CUSTOMER:
        raise Forbidden("The shopping assistant is for customers")
    return principal


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_catalog(request: Request) -> CatalogClient:
    return request.app.state.catalog


def get_inventory(request: Request) -> InventoryClient:
    return request.app.state.inventory


def get_llm(request: Request) -> LLMProvider:
    return request.app.state.llm


def get_state(request: Request, name: str) -> Any:
    return getattr(request.app.state, name)


def caller_ip_scope(request: Request) -> str:
    """``ip:<sha256>`` for anonymous budgets. The gateway sets ``X-Forwarded-For``
    to the client address; called directly, the socket peer is used."""
    forwarded = request.headers.get("x-forwarded-for", "")
    ip = forwarded.split(",")[0].strip() or (request.client.host if request.client else "unknown")
    return "ip:" + hashlib.sha256(ip.encode()).hexdigest()[:32]


def bearer(request: Request) -> str | None:
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    return token.strip() if scheme.lower() == "bearer" and token.strip() else None
