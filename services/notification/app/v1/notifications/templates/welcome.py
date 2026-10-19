"""``welcome`` ← ``identity.user.registered``."""

from collections.abc import Mapping
from typing import Any

TITLE = "Welcome to SmartRetail"


def render(payload: Mapping[str, Any]) -> tuple[str, str]:
    name = str(payload["full_name"]).strip()
    greeting = f"Hi {name}, your" if name else "Your"
    return TITLE, f"{greeting} {payload['role']} account is ready."
