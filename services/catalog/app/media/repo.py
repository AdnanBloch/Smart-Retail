"""URL media (RFC 0004 §3.1.2). Writes bump the root product's version (CAT-1) and
are audited against the product."""

import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import MediaNotFound
from app.media.models import ProductMedia
from app.products import audit_repo
from app.products.enums import ProductAuditAction
from app.products.repo import bump, lock_for_change
from smartretail_common.auth import Principal


@dataclass(frozen=True)
class MediaWrite:
    """A media item after a write, with the product version that write produced."""

    media: ProductMedia
    product_version: int


class MediaRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(
        self,
        principal: Principal,
        product_id: uuid.UUID,
        *,
        url: str,
        alt_text: str | None,
        position: int,
        version: int,
    ) -> MediaWrite:
        product = await lock_for_change(self.session, principal, product_id, version)
        media = ProductMedia(
            id=uuid.uuid4(),
            product_id=product.id,
            url=url,
            alt_text=alt_text,
            position=position,
        )
        self.session.add(media)
        bump(product)
        await self.session.flush()
        audit_repo.record(
            self.session,
            product_id=product.id,
            actor_id=principal.subject,
            action=ProductAuditAction.MEDIA_ADDED,
            changes={"media_id": media.id, "url": media.url, "position": media.position},
        )
        await self.session.commit()
        return MediaWrite(media, product.version)

    async def remove(self, principal: Principal, media_id: uuid.UUID, version: int) -> None:
        media = await self.session.scalar(select(ProductMedia).where(ProductMedia.id == media_id))
        if media is None:
            raise MediaNotFound()
        product = await lock_for_change(self.session, principal, media.product_id, version)
        await self.session.delete(media)
        bump(product)
        audit_repo.record(
            self.session,
            product_id=product.id,
            actor_id=principal.subject,
            action=ProductAuditAction.MEDIA_REMOVED,
            changes={"media_id": media.id, "url": media.url},
        )
        await self.session.commit()
