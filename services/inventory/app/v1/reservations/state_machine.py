"""Reservation status transitions (RFC 0006 §3.1.1.3).

``transition()`` is the only code that assigns ``Reservation.status`` (I8).
Idempotent re-requests are resolved by the repo *before* calling it, so a
retried activity is a no-op rather than an illegal self-transition.
"""

from collections.abc import Mapping
from typing import Protocol

from app.v1.reservations.enums import ReservationStatus as R
from smartretail_common.errors import Conflict

RESERVATION_TRANSITIONS: Mapping[R, frozenset[R]] = {
    R.HELD: frozenset({R.COMMITTED, R.RELEASED, R.EXPIRED}),
    R.COMMITTED: frozenset(),  # terminal — never returns to held
    R.RELEASED: frozenset(),  # terminal
    R.EXPIRED: frozenset(),  # terminal
}


class IllegalTransition(Conflict):
    type_slug = "illegal-transition"
    title = "Illegal status transition"

    def __init__(self, current: object, target: object) -> None:
        super().__init__(
            f"Cannot move a reservation from {current} to {target}",
            extras={"from": str(current), "to": str(target)},
        )


class HasStatus(Protocol):
    status: R | str


def allowed(current: R | str, target: R) -> bool:
    return isinstance(current, R) and target in RESERVATION_TRANSITIONS[current]


def transition(entity: HasStatus, target: R) -> None:
    if not allowed(entity.status, target):
        raise IllegalTransition(entity.status, target)
    entity.status = target
