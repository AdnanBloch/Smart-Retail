"""Registration (RFC 0003 §3.1.4.1). A merchant's profile is created in the
same transaction as the user (I5), and so is the ``identity.user.registered``
outbox row (A10, IDN-T4)."""

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.enums import MerchantStatus, UserRole
from app.core.errors import EmailTaken
from app.core.security import PasswordHasher
from app.v1.merchants.models import MerchantProfile
from app.v1.merchants.utils import unique_slug
from app.v1.users import events
from app.v1.users.models import User
from app.v1.users.utils import check_password_policy


class RegistrationRepo:
    def __init__(self, session: AsyncSession, settings: Settings, hasher: PasswordHasher) -> None:
        self.session = session
        self.settings = settings
        self.hasher = hasher

    async def register(
        self,
        *,
        email: str,
        password: str,
        full_name: str,
        role: UserRole,
        business_name: str | None = None,
        client_id: str | None = None,
    ) -> User:
        check_password_policy(self.settings, password, email, field="password")
        password_hash = await self.hasher.hash(password)

        existing = await self.session.scalar(select(User.id).where(User.email == email))
        if existing is not None:
            raise EmailTaken()

        user = User(
            email=email,
            password_hash=password_hash,
            full_name=full_name,
            role=role,
        )
        self.session.add(user)
        try:
            await self.session.flush()
            if role == UserRole.MERCHANT:
                display_name = business_name or full_name
                self.session.add(
                    MerchantProfile(
                        user_id=user.id,
                        business_name=display_name,
                        slug=await unique_slug(self.session, display_name),
                        status=MerchantStatus.PENDING,
                    )
                )
            events.user_registered(self.session, user, client_id=client_id)
            await self.session.commit()
        except IntegrityError as exc:
            await self.session.rollback()
            if "email" in str(exc.orig):
                raise EmailTaken() from exc
            raise
        await self.session.refresh(user)
        return user
