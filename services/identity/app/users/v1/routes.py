"""``/api/v1/users``: self-service and admin user management (RFC 0003 §3.1.2)."""

import uuid

from fastapi import APIRouter, Depends, Query, Response, status

from app.core.dependencies import current_principal, require_admin
from app.core.enums import UserRole
from app.users.dependencies import user_repo
from app.users.repo import UserRepo
from app.users.v1.schemas import (
    AdminUserPatch,
    PasswordChangeIn,
    UserDetailOut,
    UserMePatch,
    UserOut,
)
from smartretail_common.auth import Principal
from smartretail_common.pagination import Page, PageParams, page_params

router = APIRouter()


@router.get("/me", response_model=UserOut)
async def get_me(
    principal: Principal = Depends(current_principal),
    repo: UserRepo = Depends(user_repo),
) -> UserOut:
    return UserOut.model_validate(await repo.get_me(principal))


@router.patch("/me", response_model=UserOut)
async def update_me(
    body: UserMePatch,
    principal: Principal = Depends(current_principal),
    repo: UserRepo = Depends(user_repo),
) -> UserOut:
    user = await repo.update_me(principal, body.model_dump(exclude_unset=True))
    return UserOut.model_validate(user)


@router.post("/me/password", status_code=status.HTTP_204_NO_CONTENT)
async def change_password(
    body: PasswordChangeIn,
    principal: Principal = Depends(current_principal),
    repo: UserRepo = Depends(user_repo),
) -> Response:
    await repo.change_password(
        principal, current_password=body.current_password, new_password=body.new_password
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("", response_model=Page[UserOut], dependencies=[Depends(require_admin)])
async def list_users(
    role: UserRole | None = None,
    is_active: bool | None = None,
    q: str | None = Query(None, max_length=200),
    page: PageParams = Depends(page_params),
    repo: UserRepo = Depends(user_repo),
) -> Page[UserOut]:
    users = await repo.list(page, role=role, is_active=is_active, q=q)
    return users.to_page(UserOut.model_validate)


@router.get("/{user_id}", response_model=UserDetailOut, dependencies=[Depends(require_admin)])
async def get_user(user_id: uuid.UUID, repo: UserRepo = Depends(user_repo)) -> UserDetailOut:
    return UserDetailOut.model_validate(await repo.get(user_id))


@router.patch("/{user_id}", response_model=UserDetailOut, dependencies=[Depends(require_admin)])
async def update_user(
    user_id: uuid.UUID, body: AdminUserPatch, repo: UserRepo = Depends(user_repo)
) -> UserDetailOut:
    user = await repo.admin_update(user_id, role=body.role, is_active=body.is_active)
    return UserDetailOut.model_validate(user)
