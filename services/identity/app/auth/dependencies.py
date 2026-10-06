"""Builds the auth repos for a request."""

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.authentication_repo import AuthenticationRepo
from app.auth.registration_repo import RegistrationRepo
from app.auth.session_repo import SessionRepo
from app.auth.token_repo import TokenRepo
from app.core.config import Settings
from app.core.db import get_db
from app.core.dependencies import get_hasher, get_keyring, get_settings
from app.core.security import KeyRing, PasswordHasher


def token_repo(
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
    issuer: TokenRepo = Depends(token_repo),
) -> AuthenticationRepo:
    return AuthenticationRepo(session, hasher, issuer)


def session_repo(
    session: AsyncSession = Depends(get_db), issuer: TokenRepo = Depends(token_repo)
) -> SessionRepo:
    return SessionRepo(session, issuer)
