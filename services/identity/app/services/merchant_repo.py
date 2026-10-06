"""Merchant profiles (RFC 0003 §3.1.2, IDN-14)."""

import uuid

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.exception_handlers.exception_handlers import MerchantNotFound
from app.models import MerchantProfile
from app.models.enums import MerchantStatus, UserRole
from app.models.merchant_profile import MerchantMePatch, MerchantOut
from smartretail_common.auth import Principal
from smartretail_common.pagination import Page, PageParams


def _out(profile: MerchantProfile) -> MerchantOut:
    return MerchantOut(
        merchant_id=profile.user_id,
        business_name=profile.business_name,
        slug=profile.slug,
        description=profile.description,
        status=profile.status,
    )


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

    async def get_public(self, merchant_id: uuid.UUID, viewer: Principal | None) -> MerchantOut:
        """Only approved profiles are public; the owner and admins see any status."""
        profile = await self._get(merchant_id)
        privileged = viewer is not None and (
            viewer.role == UserRole.ADMIN or viewer.subject == str(profile.user_id)
        )
        if profile.status != MerchantStatus.APPROVED and not privileged:
            raise MerchantNotFound()
        return _out(profile)

    async def get_me(self, principal: Principal) -> MerchantOut:
        return _out(await self._get(principal.subject))

    async def update_me(self, principal: Principal, data: MerchantMePatch) -> MerchantOut:
        profile = await self._get(principal.subject)
        for field, value in data.model_dump(exclude_unset=True).items():
            setattr(profile, field, value)
        await self.session.commit()
        await self.session.refresh(profile)
        return _out(profile)

    async def list(
        self, page: PageParams, *, status: MerchantStatus | None, q: str | None
    ) -> Page[MerchantOut]:
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
        return Page[MerchantOut](
            items=[_out(p) for p in rows], total=total or 0, limit=page.limit, offset=page.offset
        )
