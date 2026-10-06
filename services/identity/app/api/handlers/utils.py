"""FastAPI dependencies wiring the service layer to request state."""

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.configs.env import Settings
from app.infrastructure.db import get_db
from app.infrastructure.security import KeyRing, PasswordHasher
from app.models.enums import UserRole
from app.services.authentication_repo import AuthenticationRepo
from app.services.merchant_repo import MerchantRepo
from app.services.registration_repo import RegistrationRepo
from app.services.session_repo import SessionRepo
from app.services.token_repo import TokenRepo
from app.services.user_repo import UserRepo
from smartretail_common.auth import current_principal, optional_principal, require_roles

__all__ = [
    "current_principal",
    "optional_principal",
    "require_admin",
    "require_merchant",
]

require_admin = require_roles(UserRole.ADMIN)
require_merchant = require_roles(UserRole.MERCHANT)


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_hasher(request: Request) -> PasswordHasher:
    return request.app.state.hasher


def get_keyring(request: Request) -> KeyRing:
    return request.app.state.keyring


def get_token_repo(
    settings: Settings = Depends(get_settings), keyring: KeyRing = Depends(get_keyring)
) -> TokenRepo:
    return TokenRepo(settings, keyring)


def registration_repo(
    session: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
    hasher: PasswordHasher = Depends(get_hasher),
) -> RegistrationRepo:
    return RegistrationRepo(session, settings, hasher)


def authentication_repo(
    session: AsyncSession = Depends(get_db),
    hasher: PasswordHasher = Depends(get_hasher),
    issuer: TokenRepo = Depends(get_token_repo),
) -> AuthenticationRepo:
    return AuthenticationRepo(session, hasher, issuer)


def session_repo(
    session: AsyncSession = Depends(get_db), issuer: TokenRepo = Depends(get_token_repo)
) -> SessionRepo:
    return SessionRepo(session, issuer)


def user_repo(
    session: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
    hasher: PasswordHasher = Depends(get_hasher),
) -> UserRepo:
    return UserRepo(session, settings, hasher)


def merchant_repo(session: AsyncSession = Depends(get_db)) -> MerchantRepo:
    return MerchantRepo(session)


def client_ip(request: Request) -> str | None:
    # identity is reachable only through the gateway, which sets this header.
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()[:64]
    return request.client.host if request.client else None
