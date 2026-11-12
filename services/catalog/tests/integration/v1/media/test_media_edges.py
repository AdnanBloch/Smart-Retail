"""Media edge cases: magic bytes at the API, unknown media, best-effort object cleanup."""

import io
import uuid

import pytest
from PIL import Image

from tests.integration.conftest import create_product


def encoded(fmt: str) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (3, 3), "green").save(buffer, format=fmt)
    return buffer.getvalue()


async def _upload(client, headers, product, *, name, data, ctype):
    return await client.post(
        f"/api/v1/products/{product['id']}/media/upload",
        data={"version": str(product["version"])},
        files={"file": (name, data, ctype)},
        headers=headers,
    )


@pytest.mark.parametrize(
    ("fmt", "name", "ctype", "ext"),
    [
        ("JPEG", "a.JPEG", "image/jpeg", "jpg"),
        ("WEBP", "a.webp", "image/webp", "webp"),
        ("PNG", "no-extension", "image/png", "png"),
    ],
)
async def test_each_allowed_type_is_stored_under_its_canonical_extension(
    client, callers, storage, fmt, name, ctype, ext
) -> None:
    product = await create_product(client, callers.merchant_a)
    r = await _upload(
        client, callers.merchant_a, product, name=name, data=encoded(fmt), ctype=ctype
    )
    assert r.status_code == 201, r.text
    [key] = storage.objects
    assert key.endswith(f"/original.{ext}") and storage.objects[key][1] == ctype


@pytest.mark.parametrize(
    ("name", "data", "ctype"),
    [
        ("a.jpg", encoded("PNG"), "image/jpeg"),  # declared JPEG, bytes are PNG
        ("a.png", encoded("JPEG"), "image/png"),  # declared PNG, bytes are JPEG
        ("a.webp", b"RIFF\x00\x00\x00\x00WAVEfmt ", "image/webp"),  # RIFF but not WEBP
        ("a.png", b"\x89PN", "image/png"),  # truncated signature
        ("a.svg", b"<svg/>", "image/svg+xml"),  # not an allowed type
    ],
)
async def test_magic_bytes_must_match_the_declared_type(
    client, callers, storage, name, data, ctype
) -> None:
    product = await create_product(client, callers.merchant_a)
    r = await _upload(client, callers.merchant_a, product, name=name, data=data, ctype=ctype)
    assert r.status_code == 415 and r.json()["type"].endswith("/unsupported-media-type"), r.text
    assert storage.objects == {}
    detail = await client.get(f"/api/v1/products/{product['id']}", headers=callers.merchant_a)
    assert detail.json()["version"] == product["version"]


async def test_exactly_the_size_limit_is_accepted(client, callers, app) -> None:
    product = await create_product(client, callers.merchant_a)
    data = encoded("PNG")
    app.state.settings.media_max_bytes = len(data)
    r = await _upload(
        client, callers.merchant_a, product, name="a.png", data=data, ctype="image/png"
    )
    assert r.status_code == 201
    app.state.settings.media_max_bytes = len(data) - 1
    product = {**product, "version": r.json()["version"]}
    r = await _upload(
        client, callers.merchant_a, product, name="a.png", data=data, ctype="image/png"
    )
    assert r.status_code == 413


async def test_stale_version_upload_is_409_and_stores_nothing(client, callers, storage) -> None:
    product = await create_product(client, callers.merchant_a)
    stale = {**product, "version": product["version"] + 5}
    r = await _upload(
        client, callers.merchant_a, stale, name="a.png", data=encoded("PNG"), ctype="image/png"
    )
    assert r.status_code == 409 and storage.objects == {}


async def test_remove_unknown_media_is_404(client, callers) -> None:
    r = await client.delete(f"/api/v1/media/{uuid.uuid4()}?version=1", headers=callers.admin)
    assert r.status_code == 404 and r.json()["type"].endswith("/media-not-found")


async def test_storage_down_on_delete_still_removes_the_row(client, callers, storage) -> None:
    """The row is the truth; an orphaned object is harmless (best effort after commit)."""
    product = await create_product(client, callers.merchant_a)
    r = await _upload(
        client, callers.merchant_a, product, name="a.png", data=encoded("PNG"), ctype="image/png"
    )
    media = r.json()
    storage.fail = True
    r = await client.delete(
        f"/api/v1/media/{media['id']}?version={media['version']}", headers=callers.merchant_a
    )
    assert r.status_code == 204
    storage.fail = False
    assert len(storage.objects) == 1  # left behind, not lost
    detail = await client.get(f"/api/v1/products/{product['id']}", headers=callers.merchant_a)
    assert detail.json()["media"] == []
