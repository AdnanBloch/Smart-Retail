import uuid
from datetime import datetime
from typing import Annotated

from pydantic import Field
from sqlalchemy import ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.response import OutModel, StrictModel
from smartretail_common.db import Base, Timestamps, UUIDPrimaryKey


class Category(UUIDPrimaryKey, Timestamps, Base):
    __tablename__ = "categories"

    name: Mapped[str] = mapped_column(Text, nullable=False)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    parent_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("categories.id"), nullable=True
    )


# --- request / response models ------------------------------------------

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


class CategoryNode(CategoryRef):
    children: list["CategoryNode"] = Field(default_factory=list)
