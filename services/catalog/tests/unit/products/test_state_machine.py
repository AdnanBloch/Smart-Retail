import itertools
from dataclasses import dataclass

import pytest

from app.products.enums import ProductStatus as S
from app.products.state_machine import (
    PRODUCT_TRANSITIONS,
    TERMINAL_STATES,
    IllegalTransition,
    allowed,
    transition,
)


@dataclass
class Entity:
    status: S | str


def test_table_is_total() -> None:
    assert set(PRODUCT_TRANSITIONS) == set(S)
    for targets in PRODUCT_TRANSITIONS.values():
        assert targets <= set(S)


def test_terminal_set_is_archived_only() -> None:
    assert {S.ARCHIVED} == TERMINAL_STATES


def test_reachability_from_draft() -> None:
    seen, frontier = {S.DRAFT}, [S.DRAFT]
    while frontier:
        for nxt in PRODUCT_TRANSITIONS[frontier.pop()]:
            if nxt not in seen:
                seen.add(nxt)
                frontier.append(nxt)
    assert seen == set(S)


def test_published_only_through_publishing() -> None:
    """I8: no edge reaches `published` except the workflow's T5."""
    sources = {a for a, targets in PRODUCT_TRANSITIONS.items() if S.PUBLISHED in targets}
    assert sources == {S.PUBLISHING}
    # `publishing` is entered only from draft or inactive (T3/T4) and left
    # only by the workflow (T5 mark_published, T6 compensation).
    assert {a for a, t in PRODUCT_TRANSITIONS.items() if S.PUBLISHING in t} == {
        S.DRAFT,
        S.INACTIVE,
    }
    assert PRODUCT_TRANSITIONS[S.PUBLISHING] == {S.PUBLISHED, S.DRAFT}


def test_edge_count() -> None:
    # 11 legal edges of the 25 ordered pairs over five states (RFC 0004 §3.1.1.3).
    assert len(LEGAL) == 11 and len(ILLEGAL) == 14


LEGAL = [(a, b) for a, targets in PRODUCT_TRANSITIONS.items() for b in targets]
ILLEGAL = [(a, b) for a, b in itertools.product(S, S) if (a, b) not in LEGAL]


@pytest.mark.parametrize(("current", "target"), LEGAL)
def test_legal_transitions(current, target) -> None:
    entity = Entity(current)
    assert transition(entity, target) == current
    assert entity.status == target


@pytest.mark.parametrize(("current", "target"), ILLEGAL)
def test_illegal_transitions_leave_entity_unchanged(current, target) -> None:
    entity = Entity(current)
    with pytest.raises(IllegalTransition):
        transition(entity, target)
    assert entity.status == current


def test_unknown_raw_status_has_no_edges() -> None:
    assert not any(allowed("mystery", target) for target in S)
    with pytest.raises(IllegalTransition):
        transition(Entity("mystery"), S.DRAFT)
