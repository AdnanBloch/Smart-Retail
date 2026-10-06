"""Shared pure helpers."""

from app.core.utils import diff


def test_diff_only_changed_fields() -> None:
    assert diff({"a": 1, "b": 2}, {"a": 1, "b": 3}) == {"b": {"from": 2, "to": 3}}
