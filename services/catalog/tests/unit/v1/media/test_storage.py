"""``MinioStorage`` against a stand-in client: what it sends, and that every
transport failure surfaces as ``StorageUnavailable`` (503), never a raw error."""

import pytest
import urllib3
from minio.error import MinioException

from app.core.config import Settings
from app.core.errors import StorageUnavailable
from app.v1.media.storage import MinioStorage


class FakeObject:
    def __init__(self, data: bytes) -> None:
        self.data, self.closed, self.released = data, False, False

    def read(self) -> bytes:
        return self.data

    def close(self) -> None:
        self.closed = True

    def release_conn(self) -> None:
        self.released = True


class FakeMinio:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, str]] = {}
        self.removed: list[tuple[str, str]] = []
        self.opened: list[FakeObject] = []
        self.error: Exception | None = None

    def _maybe_fail(self) -> None:
        if self.error is not None:
            raise self.error

    def put_object(self, bucket, key, stream, length, content_type):
        self._maybe_fail()
        data = stream.read()
        assert len(data) == length
        self.objects[(bucket, key)] = (data, content_type)

    def get_object(self, bucket, key):
        self._maybe_fail()
        obj = FakeObject(self.objects[(bucket, key)][0])
        self.opened.append(obj)
        return obj

    def remove_object(self, bucket, key):
        self._maybe_fail()
        self.removed.append((bucket, key))


@pytest.fixture
def storage() -> tuple[MinioStorage, FakeMinio]:
    store = MinioStorage(Settings(database_url="x", minio_media_bucket="media"))
    fake = FakeMinio()
    store._client = fake
    return store, fake


async def test_round_trip_through_the_client(storage) -> None:
    store, fake = storage
    await store.put("k/original.png", b"bytes", "image/png")
    assert fake.objects[("media", "k/original.png")] == (b"bytes", "image/png")
    assert await store.get("k/original.png") == b"bytes"
    # The pooled connection is always handed back.
    assert fake.opened[0].closed and fake.opened[0].released
    await store.delete(["a", "b"])
    assert fake.removed == [("media", "a"), ("media", "b")]
    assert store.location("k") == "s3://media/k"


@pytest.mark.parametrize(
    "error",
    [
        MinioException("bucket gone"),
        urllib3.exceptions.MaxRetryError(None, "/", "refused"),
        ConnectionRefusedError("refused"),
    ],
)
async def test_transport_failures_become_storage_unavailable(storage, error) -> None:
    store, fake = storage
    fake.error = error
    for call in (store.put("k", b"x", "image/png"), store.get("k"), store.delete(["k"])):
        with pytest.raises(StorageUnavailable):
            await call


async def test_other_errors_are_not_masked(storage) -> None:
    """A bug (here a missing key in the fake) must not be reported as an outage."""
    store, _ = storage
    with pytest.raises(KeyError):
        await store.get("never-written")
