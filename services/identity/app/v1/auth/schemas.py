"""``/api/v1/auth`` request and response models."""

from typing import Annotated, Literal

from pydantic import EmailStr, Field, field_validator

from app.core.enums import SELF_REGISTERABLE_ROLES, UserRole
from app.core.schemas import FullName, Password, StrictModel
from app.v1.auth.token_repo import IssuedTokens


class RegisterIn(StrictModel):
    email: EmailStr
    password: Password
    full_name: FullName
    role: UserRole = UserRole.CUSTOMER
    business_name: Annotated[str, Field(min_length=1, max_length=200)] | None = None

    @field_validator("role")
    @classmethod
    def _not_admin(cls, role: UserRole) -> UserRole:
        if role not in SELF_REGISTERABLE_ROLES:
            raise ValueError("role must be customer or merchant")
        return role


class LoginIn(StrictModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=1024)


class RefreshIn(StrictModel):
    refresh_token: str = Field(min_length=1, max_length=256)


class TokenPair(StrictModel):
    access_token: str
    refresh_token: str
    token_type: Literal["Bearer"] = "Bearer"
    expires_in: int

    @classmethod
    def from_issued(cls, tokens: IssuedTokens) -> "TokenPair":
        return cls(
            access_token=tokens.access_token,
            refresh_token=tokens.refresh_token,
            expires_in=tokens.expires_in,
        )


class ServiceToken(StrictModel):
    """No refresh token: a client re-presents its credentials instead."""

    access_token: str
    token_type: Literal["Bearer"] = "Bearer"
    expires_in: int
    scope: str
