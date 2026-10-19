"""Media: URL attach, binary upload, remove (RFC 0004 §3.1.2). Writes bump the root
product's version (CAT-1) and are audited against the product."""

import logging
import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.errors import MediaNotFound
from app.v1.media import processing
from app.v1.media.enums import MediaProcessingStatus
from app.v1.media.models import ProductMedia
from app.v1.media.storage import MediaStorage
from app.v1.products import audit_repo
from app.v1.products.enums import ProductAuditAction
from app.v1.products.models import Product
from app.v1.products.repo import after_published_change, bump, lock_for_change
from smartretail_common.auth import Principal

logger = logging.getLogger("catalog.media")


@dataclass(frozen=True)
class MediaWrite:
    """A media item after a write, with the product version that write produced."""

    media: ProductMedia
    product_version: int


@dataclass(frozen=True)
class Upload:
    data: bytes
    filename: str | None
    content_type: str | None


def stored_keys(media: ProductMedia) -> list[str]:
    keys = [media.storage_key] if media.storage_key else []
    return keys + [str(k) for k in (media.derived or {}).values()]


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
        """URL form: nothing stored by us, so nothing to process — ``ready`` at once."""
        product = await lock_for_change(self.session, principal, product_id, version)
        media = ProductMedia(
            id=uuid.uuid4(),
            product_id=product.id,
            url=url,
            alt_text=alt_text,
            position=position,
            processing_status=MediaProcessingStatus.READY,
        )
        return await self._added(principal, product, media)

    async def upload(
        self,
        principal: Principal,
        product_id: uuid.UUID,
        upload: Upload,
        *,
        alt_text: str | None,
        position: int,
        version: int,
        storage: MediaStorage,
        settings: Settings,
    ) -> MediaWrite:
        """Binary upload (B12): validate → store the original → insert ``pending``.

        The object write precedes the row insert, so a storage failure records
        nothing. Processing happens in the next publish run.
        """
        ext = processing.check_upload(
            upload.data,
            declared_type=upload.content_type,
            filename=upload.filename,
            allowed=settings.allowed_content_types,
            max_bytes=settings.media_max_bytes,
        )
        product = await lock_for_change(self.session, principal, product_id, version)
        media_id = uuid.uuid4()
        key = f"products/{product.id}/{media_id.hex}/original.{ext}"
        assert upload.content_type is not None  # check_upload refused anything else
        content_type = upload.content_type.split(";")[0].strip().lower()
        await storage.put(key, upload.data, content_type)
        media = ProductMedia(
            id=media_id,
            product_id=product.id,
            url=storage.location(key),
            alt_text=alt_text,
            position=position,
            storage_key=key,
            content_type=content_type,
            size_bytes=len(upload.data),
            processing_status=MediaProcessingStatus.PENDING,
        )
        return await self._added(principal, product, media)

    async def remove(self, principal: Principal, media_id: uuid.UUID, version: int) -> list[str]:
        """Removes the row; returns the object keys the caller deletes after commit."""
        media = await self.session.scalar(select(ProductMedia).where(ProductMedia.id == media_id))
        if media is None:
            raise MediaNotFound()
        product = await lock_for_change(self.session, principal, media.product_id, version)
        keys = stored_keys(media)
        await self.session.delete(media)
        bump(product)
        audit_repo.record(
            self.session,
            product_id=product.id,
            actor_id=principal.subject,
            action=ProductAuditAction.MEDIA_REMOVED,
            changes={"media_id": media.id, "url": media.url},
        )
        # Removing the last media of a published product is a 409.
        await after_published_change(self.session, product.id, ["media"])
        await self.session.commit()
        return keys

    async def _added(
        self, principal: Principal, product: Product, media: ProductMedia
    ) -> MediaWrite:
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
        await after_published_change(self.session, product.id, ["media"])
        await self.session.commit()
        return MediaWrite(media, product.version)


async def delete_objects(storage: MediaStorage, keys: list[str]) -> None:
    """Best effort, after commit: an orphaned object is harmless, a lost row is not."""
    if not keys:
        return
    try:
        await storage.delete(keys)
    except Exception:
        logger.warning("could not delete media objects", extra={"keys": keys})
