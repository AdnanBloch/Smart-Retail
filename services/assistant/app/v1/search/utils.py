"""The ``index_state`` transition table (AST-7) and RRF arithmetic (D7). No I/O."""

from collections.abc import Hashable, Sequence
from typing import TypeVar

from app.v1.search.enums import IndexStatus

S = IndexStatus
# None is "(new)": no row yet. (new) → removed records a tombstone for an
# unpublish of a product never indexed, so a later, older publish is stale.
TRANSITIONS: dict[IndexStatus | None, frozenset[IndexStatus]] = {
    None: frozenset({S.PENDING, S.REMOVED}),
    S.PENDING: frozenset({S.INDEXED, S.FAILED, S.REMOVED}),
    S.INDEXED: frozenset({S.PENDING, S.REMOVED}),
    S.FAILED: frozenset({S.PENDING, S.REMOVED}),
    S.REMOVED: frozenset({S.PENDING}),
}


class IllegalTransition(ValueError):
    def __init__(self, current: IndexStatus | None, target: IndexStatus) -> None:
        super().__init__(f"index_state cannot move from {current} to {target}")


def check_transition(current: IndexStatus | None, target: IndexStatus) -> None:
    if target not in TRANSITIONS[current]:
        raise IllegalTransition(current, target)


def path_to_indexed(current: IndexStatus | None) -> list[IndexStatus]:
    """The legal path to ``indexed`` from ``current``, through ``pending``."""
    steps = [] if current == S.PENDING else [S.PENDING]
    steps.append(S.INDEXED)
    state = current
    for step in steps:
        check_transition(state, step)
        state = step
    return steps


def path_to_removed(current: IndexStatus | None) -> list[IndexStatus]:
    if current == S.REMOVED:
        return []  # removal is idempotent
    check_transition(current, S.REMOVED)
    return [S.REMOVED]


K = TypeVar("K", bound=Hashable)


def rrf(rankings: Sequence[tuple[Sequence[K], float]], *, k: int = 60) -> dict[K, float]:
    """Reciprocal Rank Fusion: ``score(d) = Σ w_i / (k + rank_i(d))``, ranks from 1.

    ``rankings`` is a list of (ordered ids, weight). An empty list contributes nothing.
    """
    scores: dict[K, float] = {}
    for ordered, weight in rankings:
        for rank, key in enumerate(ordered, start=1):
            scores[key] = scores.get(key, 0.0) + weight / (k + rank)
    return scores
