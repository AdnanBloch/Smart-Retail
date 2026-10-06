import itertools
from dataclasses import dataclass

import pytest

from app.models.enums import ProductStatus as S
from app.utils.state_machine import (
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
    # `published` becomes reachable through `publishing` in Milestone 2.
    seen, frontier = {S.DRAFT}, [S.DRAFT]
    while frontier:
        for nxt in PRODUCT_TRANSITIONS[frontier.pop()]:
            if nxt not in seen:
                seen.add(nxt)
                frontier.append(nxt)
    assert seen == set(S) - {S.PUBLISHED}


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
