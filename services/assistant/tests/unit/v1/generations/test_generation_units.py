from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.core.config import Settings
from app.v1.generations.budget_repo import cost_of, next_reset, scope_kind, utc_today
from app.v1.generations.bulk_repo import request_hash
from app.v1.generations.enums import BulkJobStatus as B
from app.v1.generations.utils import (
    TERMINAL,
    TRANSITIONS,
    IllegalJobTransition,
    check_transition,
    final_status,
)


def test_bulk_transition_table():
    assert set(TRANSITIONS) == set(B)
    assert {B.COMPLETED, B.PARTIALLY_FAILED, B.FAILED, B.CANCELLED} == TERMINAL
    for terminal in TERMINAL:
        assert TRANSITIONS[terminal] == frozenset()
    check_transition(B.QUEUED, B.RUNNING)
    check_transition(B.RUNNING, B.CANCELLED)
    with pytest.raises(IllegalJobTransition):
        check_transition(B.QUEUED, B.COMPLETED)
    with pytest.raises(IllegalJobTransition):
        check_transition(B.CANCELLED, B.RUNNING)
    assert final_status(3, 0) == B.COMPLETED
    assert final_status(0, 3) == B.FAILED
    assert final_status(2, 1) == B.PARTIALLY_FAILED


def test_budget_arithmetic():
    late = datetime(2026, 10, 15, 23, 30, tzinfo=UTC)
    assert str(utc_today(late)) == "2026-10-15"
    assert next_reset(late) == datetime(2026, 10, 16, tzinfo=UTC)
    assert scope_kind("user:abc") == "user" and scope_kind("global") == "global"
    settings = Settings(database_url="postgresql+asyncpg://x/y")
    assert cost_of(settings, "claude-sonnet-5-5", 1_000_000, 100_000) == Decimal("3.000000")
    assert cost_of(settings, "claude-haiku-5-5", 1000, 1000) == Decimal("0.000600")
    assert cost_of(settings, "scripted", 10**6, 10**6) == Decimal("0")


def test_request_hash_is_canonical():
    assert request_hash({"a": 1, "b": [2]}) == request_hash({"b": [2], "a": 1})
    assert request_hash({"a": 1}) != request_hash({"a": 2})
