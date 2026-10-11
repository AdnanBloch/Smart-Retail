"""The only writer of ``stock_movements`` (RFC 0006 I6, I9).

Always called inside the transaction that changes the quantities, so the
ledger and ``stock_items`` can never disagree about a committed change.
"""

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.stock.enums import ReferenceType, StockMovementReason
from app.stock.models import StockMovement
from smartretail_common.context import get_request_id


def record(
    session: AsyncSession,
    *,
    variant_id: uuid.UUID,
    reason: StockMovementReason,
    delta_on_hand: int = 0,
    delta_reserved: int = 0,
    reference_type: ReferenceType | None = None,
    reference_id: uuid.UUID | None = None,
    actor: str,
    correlation_id: str | None = None,
) -> None:
    session.add(
        StockMovement(
            variant_id=variant_id,
            delta_on_hand=delta_on_hand,
            delta_reserved=delta_reserved,
            reason=reason,
            reference_type=reference_type.value if reference_type else None,
            reference_id=reference_id,
            actor=actor,
            correlation_id=correlation_id or get_request_id(),
        )
    )
