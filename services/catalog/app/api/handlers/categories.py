import uuid
from typing import Literal

from fastapi import APIRouter, Depends, Response, status

from app.api.handlers.utils import category_repo
from app.models.category import (
    CategoryCreate,
    CategoryDetail,
    CategoryNode,
    CategoryOut,
    CategoryPatch,
)
from app.services.category_repo import CategoryRepo
from smartretail_common.pagination import Page, PageParams, page_params

router = APIRouter()


async def list_categories(
    view: Literal["tree", "flat"] = "tree",
    parent_id: uuid.UUID | None = None,
    page: PageParams = Depends(page_params),
    repo: CategoryRepo = Depends(category_repo),
) -> list[CategoryNode] | Page[CategoryOut]:
    if view == "tree":
        return await repo.tree()
    return await repo.flat(page, parent_id)


async def get_category(
    category_id: uuid.UUID, repo: CategoryRepo = Depends(category_repo)
) -> CategoryDetail:
    return await repo.get(category_id)


async def create_category(
    body: CategoryCreate, response: Response, repo: CategoryRepo = Depends(category_repo)
) -> CategoryOut:
    category = await repo.create(body)
    response.headers["Location"] = f"/api/v1/categories/{category.id}"
    return category


async def update_category(
    category_id: uuid.UUID,
    body: CategoryPatch,
    repo: CategoryRepo = Depends(category_repo),
) -> CategoryOut:
    return await repo.update(category_id, body)


async def delete_category(
    category_id: uuid.UUID, repo: CategoryRepo = Depends(category_repo)
) -> Response:
    await repo.delete(category_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
