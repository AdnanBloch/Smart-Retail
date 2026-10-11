"""Product status transitions (RFC 0004 §3.1.1.4).

``transition()`` is the only code that assigns ``Product.status`` (I7).
``published`` is reachable only from ``publishing``, and ``publishing`` only
by an accepted publish (T3/T4) — so no write can make a product public
without the workflow having run.
"""

from typing import Protocol

from app.products.enums import ProductStatus as S
from smartretail_common.errors import Conflict

PRODUCT_TRANSITIONS: dict[S, frozenset[S]] = {
    S.DRAFT: frozenset({S.INACTIVE, S.PUBLISHING, S.ARCHIVED}),  # T1, T3, T9
    S.INACTIVE: frozenset({S.DRAFT, S.PUBLISHING, S.ARCHIVED}),  # T2, T4, T10
    S.PUBLISHING: frozenset({S.PUBLISHED, S.DRAFT}),  # T5 mark_published, T6 compensation
    S.PUBLISHED: frozenset({S.DRAFT, S.INACTIVE, S.ARCHIVED}),  # T7, T8, T11
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
