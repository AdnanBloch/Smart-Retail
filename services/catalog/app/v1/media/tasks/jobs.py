"""The Celery side of ``process_media``: runs on ``catalog-celery`` (queue ``catalog.media``)."""

import uuid

from app.core.celery_app import app
from app.core.config import get_settings
from app.v1.media.storage import MinioStorage
from app.v1.media.tasks.process import PROCESS_MEDIA_TASK, Result, derive_and_store
from smartretail_common.celery_app import TASK_DEFAULTS, run_async


async def process(media_id: str, storage_key: str, product_id: str) -> Result:
    settings = get_settings()
    return await derive_and_store(
        MinioStorage(settings),
        settings,
        media_id=uuid.UUID(media_id),
        product_id=uuid.UUID(product_id),
        storage_key=storage_key,
    )


@app.task(name=PROCESS_MEDIA_TASK, **TASK_DEFAULTS)
def process_media(media_id: str, storage_key: str, product_id: str) -> Result:
    return run_async(lambda: process(media_id, storage_key, product_id))
