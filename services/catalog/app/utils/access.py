"""Ownership and visibility (RFC 0004 §3.1.2.2, I4).

Enforced here, in the service layer; the gateway's role check is never relied
on. A merchant's id is the subject of their token and is never taken from a
body, query or header.
"""

import uuid

from app.exception_handlers.exception_handlers import (
    ProductArchived,
    ProductNotFound,
    VersionConflict,
)
from app.models import Product
from app.models.enums import CallerRole, ProductStatus
from smartretail_common.auth import Principal
from smartretail_common.errors import Forbidden


def is_admin(principal: Principal | None) -> bool:
    return principal is not None and principal.role == CallerRole.ADMIN


def caller_merchant_id(principal: Principal | None) -> uuid.UUID | None:
    if principal is None or principal.role != CallerRole.MERCHANT or principal.merchant_id is None:
        return None
    return uuid.UUID(principal.merchant_id)


def is_owner(principal: Principal | None, product: Product) -> bool:
    merchant_id = caller_merchant_id(principal)
    return merchant_id is not None and merchant_id == product.merchant_id


def can_view(principal: Principal | None, product: Product) -> bool:
    return (
        product.status == ProductStatus.PUBLISHED
        or is_admin(principal)
        or is_owner(principal, product)
    )


def ensure_visible(principal: Principal | None, product: Product) -> None:
    # 404, not 403, so the existence of a non-published product is not confirmed.
    if not can_view(principal, product):
        raise ProductNotFound()


def ensure_can_manage(principal: Principal, product: Product) -> None:
    """Owner or admin. Others get 403 on a visible product and 404 otherwise."""
    if is_admin(principal) or is_owner(principal, product):
        return
    ensure_visible(principal, product)
    raise Forbidden("Only the owning merchant or an admin can do this")


def ensure_version(product: Product, version: int) -> None:
    if product.version != version:
        raise VersionConflict(product.version)


def ensure_mutable(product: Product) -> None:
    if product.status == ProductStatus.ARCHIVED:
        raise ProductArchived()
