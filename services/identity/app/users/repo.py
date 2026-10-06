"""Self-service and admin user management (RFC 0003 §3.1.2, §3.1.4.4)."""

import uuid
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.session_repo import revoke_all_sessions
from app.core.config import Settings
from app.core.enums import MerchantStatus, UserRole
from app.core.errors import InvalidCredentials, LastAdmin, UserNotFound
from app.core.security import PasswordHasher
from app.merchants.models import MerchantProfile
from app.merchants.utils import unique_slug
from app.users.models import User
from app.users.utils import check_password_policy
from smartretail_common.auth import Principal
from smartretail_common.pagination import PageParams, Slice


class UserRepo:
    def __init__(self, session: AsyncSession, settings: Settings, hasher: PasswordHasher) -> None:
        self.session = session
        self.settings = settings
        self.hasher = hasher

    async def _get(self, user_id: uuid.UUID | str, *, for_update: bool = False) -> User:
        try:
            uid = uuid.UUID(str(user_id))
        except ValueError as exc:
            raise UserNotFound() from exc
        stmt = select(User).where(User.id == uid)
        if for_update:
            stmt = stmt.with_for_update()
        user = await self.session.scalar(stmt)
        if user is None:
            raise UserNotFound()
        return user

    # --- self ------------------------------------------------------------

    async def get_me(self, principal: Principal) -> User:
        return await self._get(principal.subject)

    async def update_me(self, principal: Principal, changes: dict[str, Any]) -> User:
        """Applies the fields the caller sent; the API layer decides which are allowed."""
        user = await self._get(principal.subject)
        for field, value in changes.items():
            setattr(user, field, value)
        await self.session.commit()
        await self.session.refresh(user)
        return user

    async def change_password(
        self, principal: Principal, *, current_password: str, new_password: str
    ) -> None:
        user = await self._get(principal.subject)
        if not await self.hasher.verify(user.password_hash, current_password):
            raise InvalidCredentials()
        check_password_policy(self.settings, new_password, user.email, field="new_password")
        user.password_hash = await self.hasher.hash(new_password)
        current_sid = uuid.UUID(principal.session_id) if principal.session_id else None
        await revoke_all_sessions(self.session, user.id, except_id=current_sid)
        await self.session.commit()

    # --- admin -----------------------------------------------------------

    async def list(
        self, page: PageParams, *, role: UserRole | None, is_active: bool | None, q: str | None
    ) -> Slice[User]:
        filters = []
        if role is not None:
            filters.append(User.role == role)
        if is_active is not None:
            filters.append(User.is_active.is_(is_active))
        if q:
            pattern = f"%{q}%"
            filters.append(or_(User.email.ilike(pattern), User.full_name.ilike(pattern)))
        total = await self.session.scalar(select(func.count()).select_from(User).where(*filters))
        rows = await self.session.scalars(
            select(User)
            .where(*filters)
            .order_by(User.created_at.desc(), User.id)
            .limit(page.limit)
            .offset(page.offset)
        )
        return Slice(items=list(rows), total=total or 0, params=page)

    async def get(self, user_id: uuid.UUID) -> User:
        return await self._get(user_id)

    async def admin_update(
        self, user_id: uuid.UUID, *, role: UserRole | None = None, is_active: bool | None = None
    ) -> User:
        user = await self._get(user_id, for_update=True)
        changes: dict[str, Any] = {
            k: v for k, v in {"role": role, "is_active": is_active}.items() if v is not None
        }
        new_role = changes.get("role", user.role)
        new_active = changes.get("is_active", user.is_active)
        role_changed = new_role != user.role
        deactivated = user.is_active and not new_active

        # I10: never demote or deactivate the last active admin. Lock every
        # active admin row so two concurrent demotions cannot both pass.
        if user.role == UserRole.ADMIN and user.is_active and (role_changed or deactivated):
            admins = await self.session.scalars(
                select(User.id)
                .where(User.role == UserRole.ADMIN, User.is_active.is_(True))
                .with_for_update()
            )
            if not set(admins) - {user.id}:
                raise LastAdmin()

        if role_changed:
            await self._apply_role_change(user, UserRole(new_role))
        user.is_active = new_active
        if role_changed or deactivated:
            await revoke_all_sessions(self.session, user.id)
        await self.session.commit()
        await self.session.refresh(user, ["merchant_profile", "updated_at"])
        return user

    async def _apply_role_change(self, user: User, new_role: UserRole) -> None:
        profile = user.merchant_profile
        if new_role == UserRole.MERCHANT:
            if profile is None:
                self.session.add(
                    MerchantProfile(
                        user_id=user.id,
                        business_name=user.full_name,
                        slug=await unique_slug(self.session, user.full_name),
                        status=MerchantStatus.PENDING,
                    )
                )
            elif profile.status == MerchantStatus.SUSPENDED:
                profile.status = MerchantStatus.PENDING
        elif user.role == UserRole.MERCHANT and profile is not None:
            # IDN-7: moving away from merchant suspends the profile; the
            # status_changed event joins this transaction in Milestone 3.
            profile.status = MerchantStatus.SUSPENDED
        user.role = new_role
