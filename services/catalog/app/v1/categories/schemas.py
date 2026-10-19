"""``/api/v1/categories`` request and response models."""

import uuid
from datetime import datetime
from typing import Annotated

from pydantic import Field

from app.core.schemas import OutModel, StrictModel
from app.v1.categories.repo import CategoryTree, CategoryWithPath

Slug = Annotated[str, Field(min_length=1, max_length=100, pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")]
Name = Annotated[str, Field(min_length=1, max_length=200)]


class CategoryCreate(StrictModel):
    name: Name
    slug: Slug
    parent_id: uuid.UUID | None = None


class CategoryPatch(StrictModel):
    name: Name | None = None
    slug: Slug | None = None
    parent_id: uuid.UUID | None = None


class CategoryRef(OutModel):
    id: uuid.UUID
    name: str
    slug: str


class CategoryOut(CategoryRef):
    parent_id: uuid.UUID | None
    created_at: datetime
    updated_at: datetime


class CategoryDetail(CategoryOut):
    path: list[CategoryRef] = Field(description="Ancestors, root first")

    @classmethod
    def from_result(cls, result: CategoryWithPath) -> "CategoryDetail":
        return cls.model_validate(
            {
                **CategoryOut.model_validate(result.category).model_dump(),
                "path": [CategoryRef.model_validate(c) for c in result.path],
            }
        )


class CategoryNode(CategoryRef):
    children: list["CategoryNode"] = Field(default_factory=list)

    @classmethod
    def from_tree(cls, tree: CategoryTree) -> "CategoryNode":
        node = cls.model_validate(tree.category)
        node.children = [cls.from_tree(child) for child in tree.children]
        return node
