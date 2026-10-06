import json
import uuid

from fastapi import APIRouter, Depends, Request, Response, status

from app.api.handlers.utils import (
    authentication_repo,
    client_ip,
    current_principal,
    get_keyring,
    get_settings,
    registration_repo,
    session_repo,
)
from app.configs.env import Settings
from app.infrastructure.security import KeyRing
from app.models.auth import LoginIn, RefreshIn, TokenPair
from app.models.user import RegisterIn, UserOut
from app.services.authentication_repo import AuthenticationRepo
from app.services.registration_repo import RegistrationRepo
from app.services.session_repo import SessionRepo
from smartretail_common.auth import Principal

router = APIRouter()


async def register(
    body: RegisterIn,
    response: Response,
    repo: RegistrationRepo = Depends(registration_repo),
) -> UserOut:
    user = await repo.register(body)
    response.headers["Location"] = f"/api/v1/users/{user.id}"
    return user


async def login(
    body: LoginIn,
    request: Request,
    repo: AuthenticationRepo = Depends(authentication_repo),
) -> TokenPair:
    return await repo.login(
        body, user_agent=request.headers.get("user-agent"), ip=client_ip(request)
    )


async def refresh(
    body: RefreshIn,
    request: Request,
    repo: SessionRepo = Depends(session_repo),
) -> TokenPair:
    return await repo.refresh(
        body.refresh_token, user_agent=request.headers.get("user-agent"), ip=client_ip(request)
    )


async def logout(
    body: RefreshIn,
    principal: Principal = Depends(current_principal),
    repo: SessionRepo = Depends(session_repo),
) -> Response:
    await repo.logout(uuid.UUID(principal.subject), body.refresh_token)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


async def jwks(
    request: Request,
    keyring: KeyRing = Depends(get_keyring),
    settings: Settings = Depends(get_settings),
) -> Response:
    """Public signing keys. Served from memory, so it works while Postgres is down."""
    headers = {
        "ETag": keyring.jwks_etag,
        "Cache-Control": (
            f"public, max-age={settings.jwks_max_age_seconds}, "
            "stale-while-revalidate=60, stale-if-error=86400"
        ),
    }
    if request.headers.get("if-none-match") == keyring.jwks_etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED, headers=headers)
    return Response(
        content=json.dumps(keyring.jwks),
        media_type="application/jwk-set+json",
        headers=headers,
    )
