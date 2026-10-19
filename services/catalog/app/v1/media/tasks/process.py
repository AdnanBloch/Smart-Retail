"""``process_media``: one image in, its derived sizes in object storage (RFC 0004 §3.1.1.4).

A one-shot, idempotent job, which is why it belongs on Celery rather than in
the publishing workflow (A12, ``0002-Q7``): the *workflow* decides that media
is processed and what happens if it cannot be; *this* is the job itself.

``derive_and_store`` is the job's body, shared by both runners:

* ``InlineMediaJobs`` runs it in the activity's process (tests, and the
  stack without the events profile);
* ``CeleryMediaJobs`` sends ``smartretail.catalog.process_media`` to the
  ``catalog.media`` queue and waits for the result over RabbitMQ (``rpc://``),
  heartbeating the activity meanwhile.

The result is data, never an exception, for the one business outcome —
``{"undecodable": reason}`` — so it crosses the broker as JSON; infrastructure
failures raise and are retried by Celery, then by Temporal.

Object keys are deterministic, so a re-run overwrites identical objects.
"""

import asyncio
import uuid
from collections.abc import Callable
from typing import Any, Protocol

import anyio

from app.core.config import Settings
from app.v1.media import processing
from app.v1.media.storage import MediaStorage

Heartbeat = Callable[[str], None]
Result = dict[str, Any]  # {"keys": {name: key}} | {"undecodable": reason}


async def derive_and_store(
    storage: MediaStorage,
    settings: Settings,
    *,
    media_id: uuid.UUID,
    product_id: uuid.UUID,
    storage_key: str,
    heartbeat: Heartbeat = lambda _: None,
) -> Result:
    original = await storage.get(storage_key)
    heartbeat("decoding")
    try:
        derived = await anyio.to_thread.run_sync(
            lambda: processing.derive(
                original, settings.derived_sizes, max_pixels=settings.media_max_pixels
            )
        )
    except processing.MediaUndecodable as exc:
        return {"undecodable": str(exc)}
    keys: dict[str, str] = {}
    for name, blob in derived.items():
        key = f"products/{product_id}/{media_id.hex}/{name}.webp"
        await storage.put(key, blob, "image/webp")
        keys[name] = key
        heartbeat(name)
    return {"keys": keys}


class MediaJobs(Protocol):
    async def run(
        self,
        *,
        media_id: uuid.UUID,
        product_id: uuid.UUID,
        storage_key: str,
        heartbeat: Heartbeat,
    ) -> Result: ...


class InlineMediaJobs:
    def __init__(self, storage: MediaStorage, settings: Settings) -> None:
        self.storage = storage
        self.settings = settings

    async def run(
        self,
        *,
        media_id: uuid.UUID,
        product_id: uuid.UUID,
        storage_key: str,
        heartbeat: Heartbeat,
    ) -> Result:
        return await derive_and_store(
            self.storage,
            self.settings,
            media_id=media_id,
            product_id=product_id,
            storage_key=storage_key,
            heartbeat=heartbeat,
        )


class MediaJobFailed(Exception):
    """The Celery job failed or did not answer in time; Temporal retries the activity."""


class CeleryMediaJobs:
    """Enqueue-and-await (RFC 0001 §3.1.1.4): the activity stays the unit Temporal
    tracks and retries; the work happens on ``catalog-celery``."""

    def __init__(self, app: Any, *, timeout: float, heartbeat_every: float = 5.0) -> None:
        self.app = app
        self.timeout = timeout
        self.heartbeat_every = heartbeat_every

    async def run(
        self,
        *,
        media_id: uuid.UUID,
        product_id: uuid.UUID,
        storage_key: str,
        heartbeat: Heartbeat,
    ) -> Result:
        pending = await asyncio.to_thread(
            self.app.send_task,
            PROCESS_MEDIA_TASK,
            args=[str(media_id), storage_key, str(product_id)],
        )
        waiting = asyncio.ensure_future(
            asyncio.to_thread(pending.get, timeout=self.timeout, propagate=True)
        )
        try:
            while True:
                done, _ = await asyncio.wait({waiting}, timeout=self.heartbeat_every)
                if done:
                    break
                heartbeat("queued on catalog.media")
            result: Result = waiting.result()
        except Exception as exc:  # a failed task, a timeout, the broker gone
            raise MediaJobFailed(f"{type(exc).__name__}: {exc}") from exc
        return result


PROCESS_MEDIA_TASK = "smartretail.catalog.process_media"
