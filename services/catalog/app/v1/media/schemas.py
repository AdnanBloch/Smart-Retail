"""``/api/v1/media`` request and response models."""

import uuid
from datetime import datetime
from typing import Annotated, Any

from pydantic import AnyUrl, Field, UrlConstraints

from app.core.schemas import OutModel, StrictModel, Version
from app.v1.media.enums import MediaProcessingStatus
from app.v1.media.repo import MediaWrite
from smartretail_common.enums import widen_for_response

HttpsUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["https"], max_length=2048)]


class MediaCreate(StrictModel):
    url: HttpsUrl
    alt_text: Annotated[str, Field(max_length=500)] | None = None
    position: Annotated[int, Field(ge=0, le=10_000)] = 0
    version: Version


class MediaOut(OutModel):
    id: uuid.UUID
    product_id: uuid.UUID
    url: str
    alt_text: str | None
    position: int
    created_at: datetime
    processing_status: Annotated[
        MediaProcessingStatus | str, widen_for_response(MediaProcessingStatus)
    ]
    content_type: str | None = None
    size_bytes: int | None = None
    # size name → storage key; null for URL media and until processed.
    derived: dict[str, Any] | None = None


class MediaWritten(MediaOut):
    version: int = Field(description="The product's new version")

    @classmethod
    def from_write(cls, write: MediaWrite) -> "MediaWritten":
        fields = MediaOut.model_validate(write.media).model_dump()
        return cls.model_validate({**fields, "version": write.product_version})
