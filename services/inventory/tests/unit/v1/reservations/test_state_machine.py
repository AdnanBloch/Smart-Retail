"""RESERVATION_TRANSITIONS: 3 legal edges of 16 ordered pairs; the other 13 are refused."""

import itertools
import uuid

import pytest

from app.v1.reservations.enums import ReservationStatus as R
from app.v1.reservations.repo import Line, normalise, stockout_trigger
from app.v1.reservations.state_machine import (
    RESERVATION_TRANSITIONS,
    IllegalTransition,
    allowed,
    transition,
)
from app.v1.stock.enums import StockoutTrigger

LEGAL = {(R.HELD, R.COMMITTED), (R.HELD, R.RELEASED), (R.HELD, R.EXPIRED)}
PAIRS = list(itertools.product(R, R))


class Row:
    def __init__(self, status: R | str) -> None:
        self.status = status


def test_table_is_total_and_has_exactly_the_legal_edges() -> None:
    assert set(RESERVATION_TRANSITIONS) == set(R)
    edges = {(a, b) for a, targets in RESERVATION_TRANSITIONS.items() for b in targets}
    assert edges == LEGAL
    assert len(PAIRS) - len(LEGAL) == 13


@pytest.mark.parametrize(("current", "target"), sorted(LEGAL))
def test_legal_edges(current: R, target: R) -> None:
    row = Row(current)
    transition(row, target)
    assert row.status == target


@pytest.mark.parametrize(("current", "target"), [p for p in PAIRS if p not in LEGAL])
def test_illegal_pairs_are_refused(current: R, target: R) -> None:
    row = Row(current)
    with pytest.raises(IllegalTransition) as err:
        transition(row, target)
    assert err.value.status == 409 and row.status == current


def test_a_raw_status_has_no_edges() -> None:
    assert not allowed("frozen-by-a-newer-build", R.COMMITTED)


def test_normalise_merges_and_sorts() -> None:
    a, b = sorted([uuid.uuid4(), uuid.uuid4()])
    merged = normalise([Line(b, 1), Line(a, 2), Line(b, 3)])
    assert merged == [Line(a, 2), Line(b, 4)]


@pytest.mark.parametrize(
    ("before", "after", "threshold", "expected"),
    [
        (3, 0, 5, StockoutTrigger.DEPLETED),
        (8, 5, 5, StockoutTrigger.LOW_STOCK),
        (8, 6, 5, None),
        (4, 2, 5, None),  # already at or below: edge-triggered, no repeat
    ],
)
def test_stockout_is_edge_triggered(before, after, threshold, expected) -> None:
    assert stockout_trigger(before, after, threshold) == expected
