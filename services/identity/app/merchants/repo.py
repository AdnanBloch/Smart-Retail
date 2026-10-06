"""Merchant profiles (RFC 0003 §3.1.2, IDN-14)."""

import uuid
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import MerchantStatus, UserRole
from app.core.errors import MerchantNotFound
from app.merchants.models import MerchantProfile
from smartretail_common.auth import Principal
from smartretail_common.pagination import PageParams, Slice


class MerchantRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def _get(self, merchant_id: uuid.UUID | str) -> MerchantProfile:
        try:
            uid = uuid.UUID(str(merchant_id))
        except ValueError as exc:
            raise MerchantNotFound() from exc
        profile = await self.session.get(MerchantProfile, uid)
        if profile is None:
            raise MerchantNotFound()
        return profile

    async def get_public(self, merchant_id: uuid.UUID, viewer: Principal | None) -> MerchantProfile:
        """Only approved profiles are public; the owner and admins see any status."""
        profile = await self._get(merchant_id)
        privileged = viewer is not None and (
            viewer.role == UserRole.ADMIN or viewer.subject == str(profile.user_id)
        )
        if profile.status != MerchantStatus.APPROVED and not privileged:
            raise MerchantNotFound()
        return profile

    async def get_me(self, principal: Principal) -> MerchantProfile:
        return await self._get(principal.subject)

    async def update_me(self, principal: Principal, changes: dict[str, Any]) -> MerchantProfile:
        """Applies the fields the caller sent; the API layer decides which are allowed."""
        profile = await self._get(principal.subject)
        for field, value in changes.items():
            setattr(profile, field, value)
        await self.session.commit()
        await self.session.refresh(profile)
        return profile

    async def list(
        self, page: PageParams, *, status: MerchantStatus | None, q: str | None
    ) -> Slice[MerchantProfile]:
        filters = []
        if status is not None:
            filters.append(MerchantProfile.status == status)
        if q:
            pattern = f"%{q}%"
            filters.append(
                or_(
                    MerchantProfile.business_name.ilike(pattern),
                    MerchantProfile.slug.ilike(pattern),
                )
            )
        total = await self.session.scalar(
            select(func.count()).select_from(MerchantProfile).where(*filters)
        )
        rows = await self.session.scalars(
            select(MerchantProfile)
            .where(*filters)
            .order_by(MerchantProfile.created_at.desc(), MerchantProfile.user_id)
            .limit(page.limit)
            .offset(page.offset)
        )
        return Slice(items=list(rows), total=total or 0, params=page)
