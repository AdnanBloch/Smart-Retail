"""Object storage for uploaded media (RFC 0004 B12): MinIO, S3-compatible.

The client is synchronous, so every call runs in a worker thread. Failures
surface as ``StorageUnavailable`` (503); the caller records nothing when a
write fails, because the object write precedes the row insert.
"""

import io
import logging
from collections.abc import Callable, Iterable
from datetime import timedelta
from typing import Protocol

import anyio
import urllib3
from minio import Minio
from minio.error import MinioException

from app.core.config import Settings
from app.core.errors import StorageUnavailable

logger = logging.getLogger("catalog.storage")


class MediaStorage(Protocol):
    bucket: str

    async def put(self, key: str, data: bytes, content_type: str) -> None: ...

    async def get(self, key: str) -> bytes: ...

    async def delete(self, keys: Iterable[str]) -> None: ...

    def location(self, key: str) -> str: ...


class MinioStorage:
    def __init__(self, settings: Settings) -> None:
        self.bucket = settings.minio_media_bucket
        # Short timeouts and one retry: an unreachable store is a fast 503.
        http = urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=2, read=10),
            retries=urllib3.Retry(total=1, backoff_factor=0.2),
        )
        self._client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
            http_client=http,
        )

    async def put(self, key: str, data: bytes, content_type: str) -> None:
        def _put() -> None:
            self._client.put_object(
                self.bucket, key, io.BytesIO(data), len(data), content_type=content_type
            )

        await self._run(_put)

    async def get(self, key: str) -> bytes:
        def _get() -> bytes:
            response = self._client.get_object(self.bucket, key)
            try:
                return response.read()
            finally:
                response.close()
                response.release_conn()

        return await self._run(_get)

    async def delete(self, keys: Iterable[str]) -> None:
        for key in keys:

            def _remove(k: str = key) -> None:
                self._client.remove_object(self.bucket, k)

            await self._run(_remove)

    def location(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def presign(self, key: str, ttl_seconds: int) -> str:  # pragma: no cover - needs MinIO
        return self._client.presigned_get_object(
            self.bucket, key, expires=timedelta(seconds=ttl_seconds)
        )

    async def _run[T](self, fn: Callable[[], T]) -> T:
        try:
            return await anyio.to_thread.run_sync(fn)
        except (MinioException, urllib3.exceptions.HTTPError, OSError) as exc:
            logger.warning("object storage call failed", extra={"error": repr(exc)})
            raise StorageUnavailable("Media storage is unavailable; retry shortly") from exc


class MemoryStorage:
    """In-process storage for tests and for running without MinIO."""

    def __init__(self, bucket: str = "product-media") -> None:
        self.bucket = bucket
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.fail = False

    async def put(self, key: str, data: bytes, content_type: str) -> None:
        self._check()
        self.objects[key] = (data, content_type)

    async def get(self, key: str) -> bytes:
        self._check()
        return self.objects[key][0]

    async def delete(self, keys: Iterable[str]) -> None:
        self._check()
        for key in keys:
            self.objects.pop(key, None)

    def location(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def _check(self) -> None:
        if self.fail:
            raise StorageUnavailable("Media storage is unavailable; retry shortly")
