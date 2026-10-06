import uuid

from fastapi import APIRouter, Depends, Query, Response, status

from app.api.handlers.utils import current_principal, user_repo
from app.models.enums import UserRole
from app.models.user import AdminUserPatch, PasswordChangeIn, UserDetailOut, UserMePatch, UserOut
from app.services.user_repo import UserRepo
from smartretail_common.auth import Principal
from smartretail_common.pagination import Page, PageParams, page_params

router = APIRouter()


async def get_me(
    principal: Principal = Depends(current_principal),
    repo: UserRepo = Depends(user_repo),
) -> UserOut:
    return await repo.get_me(principal)


async def update_me(
    body: UserMePatch,
    principal: Principal = Depends(current_principal),
    repo: UserRepo = Depends(user_repo),
) -> UserOut:
    return await repo.update_me(principal, body)


async def change_password(
    body: PasswordChangeIn,
    principal: Principal = Depends(current_principal),
    repo: UserRepo = Depends(user_repo),
) -> Response:
    await repo.change_password(principal, body)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


async def list_users(
    role: UserRole | None = None,
    is_active: bool | None = None,
    q: str | None = Query(None, max_length=200),
    page: PageParams = Depends(page_params),
    repo: UserRepo = Depends(user_repo),
) -> Page[UserOut]:
    return await repo.list(page, role=role, is_active=is_active, q=q)


async def get_user(user_id: uuid.UUID, repo: UserRepo = Depends(user_repo)) -> UserDetailOut:
    return await repo.get(user_id)


async def update_user(
    user_id: uuid.UUID, body: AdminUserPatch, repo: UserRepo = Depends(user_repo)
) -> UserDetailOut:
    return await repo.admin_update(user_id, body)
