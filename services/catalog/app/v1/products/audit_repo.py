"""Audit trail writes (RFC 0004 CAT-5, S7).

Always called inside the transaction that makes the change, so a failed
change leaves no audit row and no change goes unaudited.
"""

import uuid
from typing import Any

from pydantic_core import to_jsonable_python
from sqlalchemy.ext.asyncio import AsyncSession

from app.v1.products.enums import ProductAuditAction
from app.v1.products.models import ProductAuditLog
from smartretail_common.context import get_request_id


def record(
    session: AsyncSession,
    *,
    product_id: uuid.UUID,
    actor_id: str,
    action: ProductAuditAction,
    changes: dict[str, Any],
) -> None:
    payload = to_jsonable_python(changes)
    # No column for it (CAT-Q7), so the correlation id rides in the payload.
    if request_id := get_request_id():
        payload["_correlation_id"] = request_id
    session.add(
        ProductAuditLog(
            product_id=product_id,
            actor_id=uuid.UUID(actor_id),
            action=action,
            changes=payload,
        )
    )
