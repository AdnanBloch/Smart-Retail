"""``/internal/v1/inventory/reservations`` models."""

import uuid
from datetime import datetime
from typing import Annotated

from pydantic import Field

from app.core.schemas import OutModel, StrictModel
from app.v1.reservations.enums import ReleaseReason, ReservationStatus
from app.v1.reservations.models import Reservation


class LineIn(StrictModel):
    variant_id: uuid.UUID
    quantity: Annotated[int, Field(gt=0)]


class ReserveIn(StrictModel):
    order_id: uuid.UUID
    # Upper bound is INVENTORY_MAX_LINES_PER_RESERVATION's default.
    lines: Annotated[list[LineIn], Field(min_length=1, max_length=100)]
    correlation_id: Annotated[str, Field(max_length=128)] | None = None


class ReservationOut(OutModel):
    id: uuid.UUID
    order_id: uuid.UUID
    variant_id: uuid.UUID
    quantity: int
    status: ReservationStatus
    expires_at: datetime


class ReserveOut(OutModel):
    order_id: uuid.UUID
    reservations: list[ReservationOut]

    @classmethod
    def from_reservations(cls, order_id: uuid.UUID, rows: list[Reservation]) -> "ReserveOut":
        return cls(order_id=order_id, reservations=[ReservationOut.model_validate(r) for r in rows])


__all__ = ["LineIn", "ReleaseReason", "ReservationOut", "ReserveIn", "ReserveOut"]
