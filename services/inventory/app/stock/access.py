"""Ownership (RFC 0006 §3.1.2.2): merchant M may manage variant V iff
``stock_items(V).merchant_id = M``. Checked on the owning row, never on a
request field; the gateway's role check is only an early rejection (A5)."""

import uuid

from app.core.enums import CallerRole
from app.stock.models import StockItem
from smartretail_common.auth import Principal
from smartretail_common.errors import Forbidden


def is_admin(principal: Principal) -> bool:
    return principal.role == CallerRole.ADMIN


def caller_merchant_id(principal: Principal) -> uuid.UUID | None:
    if principal.role != CallerRole.MERCHANT or principal.merchant_id is None:
        return None
    return uuid.UUID(principal.merchant_id)


def ensure_can_manage(principal: Principal, item: StockItem) -> None:
    if is_admin(principal) or caller_merchant_id(principal) == item.merchant_id:
        return
    raise Forbidden("Only the owning merchant or an admin can manage this stock")
