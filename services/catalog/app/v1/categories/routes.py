"""``/api/v1/categories``: the category tree (RFC 0004 §3.1.2). Admin-only writes."""

import uuid
from typing import Literal

from fastapi import APIRouter, Depends, Response, status

from app.core.dependencies import require_admin
from app.v1.categories.dependencies import category_repo
from app.v1.categories.repo import CategoryRepo
from app.v1.categories.schemas import (
    CategoryCreate,
    CategoryDetail,
    CategoryNode,
    CategoryOut,
    CategoryPatch,
)
from smartretail_common.pagination import Page, PageParams, page_params

router = APIRouter()


@router.get("", response_model=list[CategoryNode] | Page[CategoryOut])
async def list_categories(
    view: Literal["tree", "flat"] = "tree",
    parent_id: uuid.UUID | None = None,
    page: PageParams = Depends(page_params),
    repo: CategoryRepo = Depends(category_repo),
) -> list[CategoryNode] | Page[CategoryOut]:
    if view == "tree":
        return [CategoryNode.from_tree(t) for t in await repo.tree()]
    categories = await repo.flat(page, parent_id)
    return categories.to_page(CategoryOut.model_validate)


@router.get("/{category_id}", response_model=CategoryDetail)
async def get_category(
    category_id: uuid.UUID, repo: CategoryRepo = Depends(category_repo)
) -> CategoryDetail:
    return CategoryDetail.from_result(await repo.get(category_id))


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=CategoryOut,
    dependencies=[Depends(require_admin)],
)
async def create_category(
    body: CategoryCreate, response: Response, repo: CategoryRepo = Depends(category_repo)
) -> CategoryOut:
    category = await repo.create(name=body.name, slug=body.slug, parent_id=body.parent_id)
    response.headers["Location"] = f"/api/v1/categories/{category.id}"
    return CategoryOut.model_validate(category)


@router.patch("/{category_id}", response_model=CategoryOut, dependencies=[Depends(require_admin)])
async def update_category(
    category_id: uuid.UUID,
    body: CategoryPatch,
    repo: CategoryRepo = Depends(category_repo),
) -> CategoryOut:
    category = await repo.update(category_id, body.model_dump(exclude_unset=True))
    return CategoryOut.model_validate(category)


@router.delete(
    "/{category_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_admin)]
)
async def delete_category(
    category_id: uuid.UUID, repo: CategoryRepo = Depends(category_repo)
) -> Response:
    await repo.delete(category_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
