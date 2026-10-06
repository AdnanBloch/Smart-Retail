"""Category tree management (RFC 0004 §3.1.2). Admin-only writes, public reads."""

import uuid

from sqlalchemy import exists, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.exception_handlers.exception_handlers import (
    CategoryInUse,
    CategoryNotFound,
    DuplicateSlug,
    unknown_reference,
)
from app.models import Category, Product
from app.models.category import (
    CategoryCreate,
    CategoryDetail,
    CategoryNode,
    CategoryOut,
    CategoryPatch,
    CategoryRef,
)
from smartretail_common.pagination import Page, PageParams


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

    async def tree(self) -> list[CategoryNode]:
        categories = await self._all()
        nodes = {cid: CategoryNode.model_validate(c) for cid, c in categories.items()}
        roots: list[CategoryNode] = []
        for cid, category in categories.items():
            parent = nodes.get(category.parent_id) if category.parent_id else None
            (parent.children if parent else roots).append(nodes[cid])
        return roots

    async def flat(self, page: PageParams, parent_id: uuid.UUID | None) -> Page[CategoryOut]:
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
        return Page[CategoryOut](
            items=[CategoryOut.model_validate(c) for c in rows],
            total=total or 0,
            limit=page.limit,
            offset=page.offset,
        )

    async def get(self, category_id: uuid.UUID) -> CategoryDetail:
        categories = await self._all()
        category = categories.get(category_id)
        if category is None:
            raise CategoryNotFound()
        path: list[CategoryRef] = []
        parent_id = category.parent_id
        while parent_id is not None and parent_id in categories and len(path) < len(categories):
            parent = categories[parent_id]
            path.insert(0, CategoryRef.model_validate(parent))
            parent_id = parent.parent_id
        return CategoryDetail.model_validate(
            {**CategoryOut.model_validate(category).model_dump(), "path": path}
        )

    async def create(self, data: CategoryCreate) -> CategoryOut:
        if data.parent_id is not None and await self.session.get(Category, data.parent_id) is None:
            raise unknown_reference("parent_id", "Parent category does not exist")
        category = Category(name=data.name, slug=data.slug, parent_id=data.parent_id)
        self.session.add(category)
        await self._commit()
        return CategoryOut.model_validate(category)

    async def update(self, category_id: uuid.UUID, data: CategoryPatch) -> CategoryOut:
        category = await self._get(category_id)
        changes = data.model_dump(exclude_unset=True)
        if "parent_id" in changes and changes["parent_id"] is not None:
            await self._ensure_no_cycle(category_id, changes["parent_id"])
        for field, value in changes.items():
            if value is None and field != "parent_id":
                continue
            setattr(category, field, value)
        await self._commit()
        return CategoryOut.model_validate(category)

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
