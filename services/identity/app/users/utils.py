"""Password policy (RFC 0003 IDN-2): 12-128 characters after NFKC, not the email."""

from app.core.config import Settings
from app.core.security import normalise_password
from smartretail_common.errors import Unprocessable


def check_password_policy(settings: Settings, password: str, email: str, *, field: str) -> None:
    normalised = normalise_password(password)
    problems = []
    if len(normalised) < settings.password_min_length:
        problems.append(f"must be at least {settings.password_min_length} characters")
    if len(normalised) > settings.password_max_length:
        problems.append(f"must be at most {settings.password_max_length} characters")
    if normalised.casefold() == email.casefold():
        problems.append("must not equal the email address")
    if problems:
        raise Unprocessable(
            "Password does not meet the policy",
            violations=[
                {"field": field, "message": m, "code": "password_policy"} for m in problems
            ],
        )
