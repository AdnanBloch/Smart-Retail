"""Password policy (RFC 0003 IDN-2)."""

import pytest

from app.users.utils import check_password_policy
from smartretail_common.errors import Unprocessable


@pytest.mark.parametrize(
    ("password", "ok"),
    [("a" * 11, False), ("a" * 12, True), ("a" * 128, True), ("a" * 129, False)],
)
def test_password_length_policy(settings, password, ok) -> None:
    if ok:
        check_password_policy(settings, password, "x@y.z", field="password")
    else:
        with pytest.raises(Unprocessable):
            check_password_policy(settings, password, "x@y.z", field="password")


def test_password_must_not_equal_email(settings) -> None:
    with pytest.raises(Unprocessable):
        check_password_policy(
            settings, "Longer.Email@Example.com", "longer.email@example.com", field="p"
        )
