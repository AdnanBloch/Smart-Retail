"""Small pure helpers shared by the repos."""

from typing import Any


def diff(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Before/after of the fields that actually changed."""
    return {
        key: {"from": before.get(key), "to": value}
        for key, value in after.items()
        if before.get(key) != value
    }
