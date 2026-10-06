"""Merchant status transitions (RFC 0003 §3.1.1.3).

Milestone 1 only drives the role-change edges; the admin status endpoint that
uses the full table arrives in Milestone 3.
"""

from app.models.enums import MerchantStatus as S

MERCHANT_TRANSITIONS: dict[S, frozenset[S]] = {
    S.PENDING: frozenset({S.APPROVED, S.SUSPENDED}),
    S.APPROVED: frozenset({S.SUSPENDED}),
    S.SUSPENDED: frozenset({S.APPROVED, S.PENDING}),
}


def can_transition(current: S | str, target: S) -> bool:
    return isinstance(current, S) and target in MERCHANT_TRANSITIONS[current]
