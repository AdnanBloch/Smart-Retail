"""Product status transitions (RFC 0004 §3.1.1.4).

``transition()`` is the only code that assigns ``Product.status`` (I7). The
Milestone 2 edges through ``publishing`` are added with that state.
"""

from typing import Protocol

from app.models.enums import ProductStatus as S
from smartretail_common.errors import Conflict

PRODUCT_TRANSITIONS: dict[S, frozenset[S]] = {
    S.DRAFT: frozenset({S.INACTIVE, S.ARCHIVED}),
    S.INACTIVE: frozenset({S.DRAFT, S.ARCHIVED}),
    S.PUBLISHED: frozenset({S.DRAFT, S.INACTIVE, S.ARCHIVED}),
    S.ARCHIVED: frozenset(),  # terminal
}

TERMINAL_STATES = frozenset(s for s, targets in PRODUCT_TRANSITIONS.items() if not targets)


class IllegalTransition(Conflict):
    type_slug = "illegal-transition"
    title = "Illegal status transition"

    def __init__(self, current: object, target: object) -> None:
        super().__init__(
            f"Cannot move a product from {current} to {target}",
            extras={"from": str(current), "to": str(target)},
        )


class HasStatus(Protocol):
    status: S | str


def allowed(current: S | str, target: S) -> bool:
    # A raw status that is not a member has no outgoing edges.
    return isinstance(current, S) and target in PRODUCT_TRANSITIONS[current]


def transition(entity: HasStatus, target: S) -> S | str:
    """Move ``entity`` to ``target`` or raise ``IllegalTransition``. Returns the old status."""
    current = entity.status
    if not allowed(current, target):
        raise IllegalTransition(current, target)
    entity.status = target
    return current
