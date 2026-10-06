"""Registration (RFC 0003 §3.1.4.1). A merchant's profile is created in the
same transaction as the user (I5)."""

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.configs.env import Settings
from app.exception_handlers.exception_handlers import EmailTaken
from app.infrastructure.security import PasswordHasher
from app.models import MerchantProfile, User
from app.models.enums import MerchantStatus, UserRole
from app.models.user import RegisterIn, UserOut
from app.utils.helpers import check_password_policy, unique_slug


class RegistrationRepo:
    def __init__(self, session: AsyncSession, settings: Settings, hasher: PasswordHasher) -> None:
        self.session = session
        self.settings = settings
        self.hasher = hasher

    async def register(self, data: RegisterIn) -> UserOut:
        check_password_policy(self.settings, data.password, data.email, field="password")
        password_hash = await self.hasher.hash(data.password)

        existing = await self.session.scalar(select(User.id).where(User.email == data.email))
        if existing is not None:
            raise EmailTaken()

        user = User(
            email=data.email,
            password_hash=password_hash,
            full_name=data.full_name,
            role=data.role,
        )
        self.session.add(user)
        try:
            await self.session.flush()
            if data.role == UserRole.MERCHANT:
                business_name = data.business_name or data.full_name
                self.session.add(
                    MerchantProfile(
                        user_id=user.id,
                        business_name=business_name,
                        slug=await unique_slug(self.session, business_name),
                        status=MerchantStatus.PENDING,
                    )
                )
            await self.session.commit()
        except IntegrityError as exc:
            await self.session.rollback()
            if "email" in str(exc.orig):
                raise EmailTaken() from exc
            raise
        await self.session.refresh(user)
        return UserOut.model_validate(user)
