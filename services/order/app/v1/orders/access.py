"""Ownership (RFC 0005 §3.1.2.2), decided against claims from the verified token,
never against a request field. Order isolation — customer sees own, merchant
sees own lines, admin sees all — is the highest-value test here."""

import uuid

from app.core.enums import CallerRole
from app.v1.orders.models import Order
from smartretail_common.auth import Principal


def is_admin(principal: Principal) -> bool:
    return principal.role == CallerRole.ADMIN


def caller_merchant_id(principal: Principal) -> uuid.UUID | None:
    if principal.role != CallerRole.MERCHANT or principal.merchant_id is None:
        return None
    return uuid.UUID(principal.merchant_id)


def can_view(principal: Principal, order: Order) -> bool:
    if is_admin(principal):
        return True
    return principal.role == CallerRole.CUSTOMER and order.customer_id == uuid.UUID(
        principal.subject
    )


def actor(principal: Principal) -> str:
    """``order_status_history.actor``: ``customer:<id>`` or ``admin:<id>``."""
    kind = "admin" if is_admin(principal) else "customer"
    return f"{kind}:{principal.subject}"
