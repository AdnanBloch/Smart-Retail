"""``/api/v1/auth``: registration, login, refresh, logout, JWKS (RFC 0003 §3.1.2)."""

import json
import uuid

from fastapi import APIRouter, Depends, Form, Request, Response, status
from fastapi.responses import JSONResponse

from app.auth.authentication_repo import AuthenticationRepo
from app.auth.client_credentials_repo import ClientCredentialsRepo
from app.auth.dependencies import (
    authentication_repo,
    client_credentials_repo,
    registration_repo,
    session_repo,
)
from app.auth.registration_repo import RegistrationRepo
from app.auth.session_repo import SessionRepo
from app.auth.v1.schemas import LoginIn, RefreshIn, RegisterIn, ServiceToken, TokenPair
from app.core.config import Settings
from app.core.dependencies import client_ip, current_principal, get_keyring, get_settings
from app.core.errors import OAuthError
from app.core.security import KeyRing
from app.users.v1.schemas import UserOut
from smartretail_common.auth import Principal

router = APIRouter()


@router.post("/register", status_code=status.HTTP_201_CREATED, response_model=UserOut)
async def register(
    body: RegisterIn,
    response: Response,
    repo: RegistrationRepo = Depends(registration_repo),
) -> UserOut:
    user = await repo.register(
        email=body.email,
        password=body.password,
        full_name=body.full_name,
        role=body.role,
        business_name=body.business_name,
    )
    response.headers["Location"] = f"/api/v1/users/{user.id}"
    return UserOut.model_validate(user)


@router.post("/login", response_model=TokenPair)
async def login(
    body: LoginIn,
    request: Request,
    repo: AuthenticationRepo = Depends(authentication_repo),
) -> TokenPair:
    tokens = await repo.login(
        body.email,
        body.password,
        user_agent=request.headers.get("user-agent"),
        ip=client_ip(request),
    )
    return TokenPair.from_issued(tokens)


@router.post("/refresh", response_model=TokenPair)
async def refresh(
    body: RefreshIn,
    request: Request,
    repo: SessionRepo = Depends(session_repo),
) -> TokenPair:
    tokens = await repo.refresh(
        body.refresh_token, user_agent=request.headers.get("user-agent"), ip=client_ip(request)
    )
    return TokenPair.from_issued(tokens)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    body: RefreshIn,
    principal: Principal = Depends(current_principal),
    repo: SessionRepo = Depends(session_repo),
) -> Response:
    await repo.logout(uuid.UUID(principal.subject), body.refresh_token)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/jwks", responses={304: {"description": "Not modified"}})
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


@router.post(
    "/token",
    response_model=ServiceToken,
    responses={400: {"description": "RFC 6749 error"}, 401: {"description": "invalid_client"}},
)
async def service_token(
    grant_type: str = Form(...),
    client_id: str = Form(..., max_length=128),
    client_secret: str = Form(..., max_length=256),
    scope: str | None = Form(None, max_length=1024),
    repo: ClientCredentialsRepo = Depends(client_credentials_repo),
) -> ServiceToken | JSONResponse:
    """Client-credentials grant (RFC 6749 §4.4). Internal: the gateway never routes it."""
    try:
        issued = await repo.issue(
            grant_type=grant_type, client_id=client_id, client_secret=client_secret, scope=scope
        )
    except OAuthError as exc:
        headers = {"WWW-Authenticate": "Basic"} if exc.status == 401 else None
        return JSONResponse({"error": exc.error}, status_code=exc.status, headers=headers)
    return ServiceToken(
        access_token=issued.access_token, expires_in=issued.expires_in, scope=issued.scope
    )
