"""identity problem types (RFC 0003 §3.1.2.3)."""

from smartretail_common.errors import Conflict, Forbidden, NotFound, Unauthorized


class InvalidCredentials(Unauthorized):
    type_slug = "invalid-credentials"
    title = "Invalid email or password"


class InvalidRefreshToken(Unauthorized):
    type_slug = "invalid-refresh-token"
    title = "Invalid refresh token"


class AccountInactive(Forbidden):
    type_slug = "account-inactive"
    title = "Account is inactive"


class MerchantSuspended(Forbidden):
    type_slug = "merchant-suspended"
    title = "Merchant account is suspended"


class UserNotFound(NotFound):
    type_slug = "user-not-found"
    title = "User not found"


class MerchantNotFound(NotFound):
    type_slug = "merchant-not-found"
    title = "Merchant not found"


class EmailTaken(Conflict):
    type_slug = "email-taken"
    title = "Email is already registered"


class LastAdmin(Conflict):
    type_slug = "last-admin"
    title = "Cannot remove the last active admin"


class OAuthError(Exception):
    """RFC 6749 §5.2 error for ``POST /auth/token``: an OAuth body, not a problem detail."""

    def __init__(self, status: int, error: str) -> None:
        super().__init__(error)
        self.status = status
        self.error = error
