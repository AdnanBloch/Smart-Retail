"""``process_media`` as a job (0002-Q7): the shared body, the inline and Celery
runners, the Celery task, and which runner the worker picks."""

import asyncio
import time
import uuid
from typing import Any

import pytest

from app.core.celery_app import app as celery_app
from app.core.config import Settings
from app.v1.media.storage import MemoryStorage
from app.v1.media.tasks import jobs
from app.v1.media.tasks.process import (
    PROCESS_MEDIA_TASK,
    CeleryMediaJobs,
    InlineMediaJobs,
    MediaJobFailed,
    derive_and_store,
)
from app.v1.products.workflows.worker import media_jobs
from tests.unit.v1.media.test_processing import image_bytes

SETTINGS = Settings(
    service_name="catalog",
    database_url="postgresql+asyncpg://x/y",
    media_derived_sizes="thumb:10,medium:20",
)
MEDIA, PRODUCT = uuid.uuid4(), uuid.uuid4()


def storage_with(data: bytes) -> MemoryStorage:
    storage = MemoryStorage()
    storage.objects["originals/x"] = (data, "image/png")
    return storage


async def test_derive_and_store_writes_deterministic_keys_and_heartbeats() -> None:
    storage = storage_with(image_bytes("PNG"))
    beats: list[str] = []
    result = await derive_and_store(
        storage,
        SETTINGS,
        media_id=MEDIA,
        product_id=PRODUCT,
        storage_key="originals/x",
        heartbeat=beats.append,
    )
    assert result == {
        "keys": {
            name: f"products/{PRODUCT}/{MEDIA.hex}/{name}.webp" for name in ("thumb", "medium")
        }
    }
    assert set(result["keys"].values()) <= set(storage.objects)
    assert beats == ["decoding", "thumb", "medium"]


async def test_an_undecodable_image_is_a_result_not_an_exception() -> None:
    result = await InlineMediaJobs(storage_with(b"not an image"), SETTINGS).run(
        media_id=MEDIA, product_id=PRODUCT, storage_key="originals/x", heartbeat=lambda _: None
    )
    assert "undecodable" in result


class Pending:
    def __init__(self, result: Any = None, delay: float = 0.0, error: Exception | None = None):
        self.result, self.delay, self.error = result, delay, error

    def get(self, timeout: float, propagate: bool) -> Any:
        time.sleep(self.delay)
        if self.error:
            raise self.error
        return self.result


class FakeCelery:
    def __init__(self, pending: Pending) -> None:
        self.pending = pending
        self.sent: list[tuple[str, list[str]]] = []

    def send_task(self, name: str, args: list[str]) -> Pending:
        self.sent.append((name, args))
        return self.pending


async def test_the_celery_runner_sends_the_task_and_heartbeats_while_waiting() -> None:
    fake = FakeCelery(Pending({"keys": {"thumb": "k"}}, delay=0.25))
    beats: list[str] = []
    runner = CeleryMediaJobs(fake, timeout=5, heartbeat_every=0.05)
    result = await runner.run(
        media_id=MEDIA, product_id=PRODUCT, storage_key="originals/x", heartbeat=beats.append
    )
    assert result == {"keys": {"thumb": "k"}}
    assert fake.sent == [(PROCESS_MEDIA_TASK, [str(MEDIA), "originals/x", str(PRODUCT)])]
    assert beats  # the activity stayed alive while the job ran


async def test_a_failed_or_lost_job_becomes_a_retryable_failure() -> None:
    runner = CeleryMediaJobs(FakeCelery(Pending(error=TimeoutError("no answer"))), timeout=1)
    with pytest.raises(MediaJobFailed, match="TimeoutError"):
        await runner.run(
            media_id=MEDIA, product_id=PRODUCT, storage_key="k", heartbeat=lambda _: None
        )


def test_the_task_runs_the_shared_body(monkeypatch) -> None:
    storage = storage_with(image_bytes("PNG"))
    monkeypatch.setattr(jobs, "MinioStorage", lambda _settings: storage)
    monkeypatch.setattr(jobs, "get_settings", lambda: SETTINGS)
    result = jobs.process_media.apply(args=[str(MEDIA), "originals/x", str(PRODUCT)]).get()
    assert set(result["keys"]) == {"thumb", "medium"}


def test_the_catalog_celery_app_returns_results_over_the_broker() -> None:
    conf = celery_app.conf
    assert conf.task_default_queue == "catalog.media"
    assert conf.result_backend == "rpc://" and conf.task_ignore_result is False
    assert conf.task_acks_late and conf.worker_prefetch_multiplier == 1


def test_the_worker_uses_celery_only_when_configured() -> None:
    assert media_jobs(SETTINGS) is None  # inline
    on = SETTINGS.model_copy(update={"catalog_media_via_celery": True})
    assert isinstance(media_jobs(on), CeleryMediaJobs)


def test_asyncio_loop_is_free_while_waiting() -> None:
    # The wait runs in a thread: other coroutines keep running meanwhile.
    async def main() -> int:
        runner = CeleryMediaJobs(FakeCelery(Pending({"keys": {}}, delay=0.2)), timeout=5)
        ticks = 0

        async def ticker() -> None:
            nonlocal ticks
            while True:
                ticks += 1
                await asyncio.sleep(0.01)

        task = asyncio.create_task(ticker())
        await runner.run(
            media_id=MEDIA, product_id=PRODUCT, storage_key="k", heartbeat=lambda _: None
        )
        task.cancel()
        return ticks

    assert asyncio.run(main()) > 5
