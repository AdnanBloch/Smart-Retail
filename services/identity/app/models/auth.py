from typing import Literal

from pydantic import EmailStr, Field

from app.models.response import StrictModel


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
