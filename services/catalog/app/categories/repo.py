"""Category tree management (RFC 0004 §3.1.2). Admin-only writes, public reads."""

import uuid
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import exists, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.categories.models import Category
from app.core.errors import CategoryInUse, CategoryNotFound, DuplicateSlug, unknown_reference
from app.products.models import Product
from smartretail_common.pagination import PageParams, Slice


@dataclass
class CategoryTree:
    category: Category
    children: list["CategoryTree"] = field(default_factory=list)


@dataclass(frozen=True)
class CategoryWithPath:
    category: Category
    path: list[Category]
    """Ancestors, root first."""


class CategoryRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def _all(self) -> dict[uuid.UUID, Category]:
        rows = await self.session.scalars(select(Category).order_by(Category.name))
        return {c.id: c for c in rows}

    async def _get(self, category_id: uuid.UUID) -> Category:
        category = await self.session.get(Category, category_id)
        if category is None:
            raise CategoryNotFound()
        return category

    async def tree(self) -> list[CategoryTree]:
        """Every category, nested under its parent; siblings ordered by name."""
        categories = await self._all()
        nodes = {cid: CategoryTree(c) for cid, c in categories.items()}
        roots: list[CategoryTree] = []
        for cid, category in categories.items():
            parent = nodes.get(category.parent_id) if category.parent_id else None
            (parent.children if parent else roots).append(nodes[cid])
        return roots

    async def flat(self, page: PageParams, parent_id: uuid.UUID | None) -> Slice[Category]:
        filters = [Category.parent_id == parent_id] if parent_id else []
        total = await self.session.scalar(
            select(func.count()).select_from(Category).where(*filters)
        )
        rows = await self.session.scalars(
            select(Category)
            .where(*filters)
            .order_by(Category.name)
            .limit(page.limit)
            .offset(page.offset)
        )
        return Slice(items=list(rows), total=total or 0, params=page)

    async def get(self, category_id: uuid.UUID) -> CategoryWithPath:
        categories = await self._all()
        category = categories.get(category_id)
        if category is None:
            raise CategoryNotFound()
        path: list[Category] = []
        parent_id = category.parent_id
        while parent_id is not None and parent_id in categories and len(path) < len(categories):
            parent = categories[parent_id]
            path.insert(0, parent)
            parent_id = parent.parent_id
        return CategoryWithPath(category, path)

    async def create(self, *, name: str, slug: str, parent_id: uuid.UUID | None) -> Category:
        if parent_id is not None and await self.session.get(Category, parent_id) is None:
            raise unknown_reference("parent_id", "Parent category does not exist")
        category = Category(name=name, slug=slug, parent_id=parent_id)
        self.session.add(category)
        await self._commit()
        return category

    async def update(self, category_id: uuid.UUID, changes: dict[str, Any]) -> Category:
        """Applies the fields the caller sent. ``None`` clears ``parent_id`` only."""
        category = await self._get(category_id)
        if "parent_id" in changes and changes["parent_id"] is not None:
            await self._ensure_no_cycle(category_id, changes["parent_id"])
        for name, value in changes.items():
            if value is None and name != "parent_id":
                continue
            setattr(category, name, value)
        await self._commit()
        return category

    async def delete(self, category_id: uuid.UUID) -> None:
        category = await self._get(category_id)
        in_use = await self.session.scalar(
            select(
                exists().where(Product.category_id == category_id)
                | exists().where(Category.parent_id == category_id)
            )
        )
        if in_use:
            raise CategoryInUse()
        await self.session.delete(category)
        await self.session.commit()

    async def _ensure_no_cycle(self, category_id: uuid.UUID, parent_id: uuid.UUID) -> None:
        categories = await self._all()
        if parent_id not in categories:
            raise unknown_reference("parent_id", "Parent category does not exist")
        cursor: uuid.UUID | None = parent_id
        while cursor is not None:
            if cursor == category_id:
                raise unknown_reference("parent_id", "A category cannot be its own ancestor")
            cursor = categories[cursor].parent_id if cursor in categories else None

    async def _commit(self) -> None:
        try:
            await self.session.commit()
        except IntegrityError as exc:
            await self.session.rollback()
            if "slug" in str(exc.orig):
                raise DuplicateSlug() from exc
            raise
