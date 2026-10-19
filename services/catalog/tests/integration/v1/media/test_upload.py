"""Multipart upload to object storage (RFC 0004 §3.1.2.1, B12)."""

import io

from PIL import Image

from tests.integration.conftest import create_product


def png(size=(4, 3)) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", size, "red").save(buffer, format="PNG")
    return buffer.getvalue()


async def _upload(client, headers, product, *, name="a.png", data=None, ctype="image/png"):
    return await client.post(
        f"/api/v1/products/{product['id']}/media/upload",
        data={"version": str(product["version"]), "alt_text": "front"},
        files={"file": (name, data if data is not None else png(), ctype)},
        headers=headers,
    )


async def test_upload_stores_the_original_then_records_pending(client, callers, storage) -> None:
    product = await create_product(client, callers.merchant_a)
    r = await _upload(client, callers.merchant_a, product)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["processing_status"] == "pending" and body["content_type"] == "image/png"
    assert body["version"] == product["version"] + 1 and body["derived"] is None
    [key] = storage.objects
    assert key.startswith(f"products/{product['id']}/") and key.endswith("/original.png")
    assert body["url"] == f"s3://product-media/{key}"
    assert r.headers["location"] == f"/api/v1/media/{body['id']}"

    # Owners see pending media; the public does not.
    owner = await client.get(f"/api/v1/products/{product['id']}", headers=callers.merchant_a)
    assert [m["id"] for m in owner.json()["media"]] == [body["id"]]

    # Deleting the row deletes the object, after commit.
    r = await client.delete(
        f"/api/v1/media/{body['id']}?version={body['version']}", headers=callers.merchant_a
    )
    assert r.status_code == 204 and storage.objects == {}


async def test_upload_validation(client, callers, app, storage) -> None:
    product = await create_product(client, callers.merchant_a)
    cases = [
        ({"ctype": "image/gif"}, 415),
        ({"ctype": "image/png", "name": "a.jpg"}, 415),  # extension disagrees
        ({"data": b"GIF89a not a png"}, 415),  # magic bytes disagree
        ({"data": b""}, 415),
    ]
    for kwargs, expected in cases:
        r = await _upload(client, callers.merchant_a, product, **kwargs)
        assert r.status_code == expected, (kwargs, r.text)
    app.state.settings.media_max_bytes = 10
    r = await _upload(client, callers.merchant_a, product)
    assert r.status_code == 413 and r.json()["type"].endswith("/media-too-large")
    assert storage.objects == {}


async def test_storage_down_records_nothing(client, callers, storage) -> None:
    product = await create_product(client, callers.merchant_a)
    storage.fail = True
    r = await _upload(client, callers.merchant_a, product)
    assert r.status_code == 503 and r.json()["type"].endswith("/storage-unavailable")
    detail = await client.get(f"/api/v1/products/{product['id']}", headers=callers.merchant_a)
    assert detail.json()["media"] == [] and detail.json()["version"] == product["version"]


async def test_upload_ownership(client, callers) -> None:
    product = await create_product(client, callers.merchant_a)
    # Another merchant's draft is not confirmed to exist (404); customers are refused.
    assert (await _upload(client, callers.merchant_b, product)).status_code == 404
    assert (await _upload(client, callers.customer, product)).status_code == 403
