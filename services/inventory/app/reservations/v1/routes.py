"""``/internal/v1/inventory/reservations`` — the server side of the order
workflow's inventory activities (RFC 0006 §3.1.2.1). Service token with
scope ``inventory:internal`` only; user tokens are refused, even an admin's."""

import uuid

from fastapi import APIRouter, Depends, Response, status

from app.core.dependencies import require_internal
from app.reservations.dependencies import reservation_repo
from app.reservations.enums import ReleaseReason
from app.reservations.repo import Line, ReservationRepo
from app.reservations.v1.schemas import ReservationOut, ReserveIn, ReserveOut
from smartretail_common.auth import Principal

router = APIRouter()


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=ReserveOut,
    responses={200: {"description": "Idempotent replay of an earlier reserve"}},
)
async def reserve(
    body: ReserveIn,
    response: Response,
    principal: Principal = Depends(require_internal),
    repo: ReservationRepo = Depends(reservation_repo),
) -> ReserveOut:
    result = await repo.reserve(
        body.order_id,
        [Line(line.variant_id, line.quantity) for line in body.lines],
        actor=principal.subject,
        correlation_id=body.correlation_id,
    )
    if not result.created:
        response.status_code = status.HTTP_200_OK
    return ReserveOut.from_reservations(body.order_id, result.reservations)


@router.delete("/{reservation_id}", response_model=ReservationOut)
async def release(
    reservation_id: uuid.UUID,
    reason: ReleaseReason = ReleaseReason.COMPENSATION,
    principal: Principal = Depends(require_internal),
    repo: ReservationRepo = Depends(reservation_repo),
) -> ReservationOut:
    reservation = await repo.release(reservation_id, reason=reason, actor=principal.subject)
    return ReservationOut.model_validate(reservation)


@router.post("/{reservation_id}/commit", response_model=ReservationOut)
async def commit(
    reservation_id: uuid.UUID,
    principal: Principal = Depends(require_internal),
    repo: ReservationRepo = Depends(reservation_repo),
) -> ReservationOut:
    return ReservationOut.model_validate(await repo.commit(reservation_id, actor=principal.subject))


@router.post("/{reservation_id}/reverse", response_model=ReservationOut)
async def reverse(
    reservation_id: uuid.UUID,
    principal: Principal = Depends(require_internal),
    repo: ReservationRepo = Depends(reservation_repo),
) -> ReservationOut:
    return ReservationOut.model_validate(
        await repo.reverse(reservation_id, actor=principal.subject)
    )
