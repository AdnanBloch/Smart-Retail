import io

import pytest
from PIL import Image

from app.core.config import Settings
from app.core.errors import MediaTooLarge, UnsupportedMediaType
from app.media.processing import MediaUndecodable, check_upload, derive

ALLOWED = frozenset({"image/jpeg", "image/png", "image/webp"})


def image_bytes(fmt: str, size=(40, 20), mode="RGB") -> bytes:
    buffer = io.BytesIO()
    colour = (0, 128, 0, 100) if mode == "RGBA" else (0, 128, 0)
    Image.new(mode, size, colour).save(buffer, format=fmt)
    return buffer.getvalue()


@pytest.mark.parametrize(
    ("fmt", "ctype", "name", "ext"),
    [
        ("PNG", "image/png", "a.png", "png"),
        ("JPEG", "image/jpeg", "a.jpeg", "jpg"),
        ("WEBP", "image/webp", None, "webp"),
        ("PNG", "image/png; charset=binary", "A.PNG", "png"),
    ],
)
def test_accepted_uploads(fmt, ctype, name, ext) -> None:
    data = image_bytes(fmt)
    assert (
        check_upload(data, declared_type=ctype, filename=name, allowed=ALLOWED, max_bytes=10**6)
        == ext
    )


def test_refused_uploads() -> None:
    png = image_bytes("PNG")
    with pytest.raises(MediaTooLarge):
        check_upload(png, declared_type="image/png", filename=None, allowed=ALLOWED, max_bytes=10)
    for kwargs in (
        {"declared_type": None},
        {"declared_type": "image/gif"},
        {"declared_type": "image/png", "allowed": frozenset({"image/jpeg"})},
        {"declared_type": "image/png", "filename": "a.webp"},
        {"declared_type": "image/jpeg"},  # magic bytes say PNG
    ):
        args = {"filename": None, "allowed": ALLOWED, "max_bytes": 10**6, **kwargs}
        with pytest.raises(UnsupportedMediaType):
            check_upload(png, **args)
    riff_not_webp = b"RIFF\x00\x00\x00\x00WAVEfmt "
    with pytest.raises(UnsupportedMediaType):
        check_upload(
            riff_not_webp,
            declared_type="image/webp",
            filename=None,
            allowed=ALLOWED,
            max_bytes=10**6,
        )


def test_derive_keeps_aspect_and_never_upscales() -> None:
    out = derive(image_bytes("PNG", (1200, 600)), {"thumb": 150, "huge": 5000}, max_pixels=10**7)
    with Image.open(io.BytesIO(out["thumb"])) as thumb:
        assert thumb.format == "WEBP" and thumb.size == (150, 75)
    with Image.open(io.BytesIO(out["huge"])) as huge:
        assert huge.size == (1200, 600)
    # Transparency survives as RGBA.
    rgba = derive(image_bytes("PNG", mode="RGBA"), {"t": 10}, max_pixels=10**7)
    with Image.open(io.BytesIO(rgba["t"])) as t:
        assert t.mode == "RGBA"


def test_derive_refuses_bombs_and_garbage() -> None:
    with pytest.raises(MediaUndecodable, match="exceeds"):
        derive(image_bytes("PNG", (100, 100)), {"t": 10}, max_pixels=5000)
    with pytest.raises(MediaUndecodable, match="cannot decode"):
        derive(b"\x89PNG\r\n\x1a\nnope", {"t": 10}, max_pixels=10**7)


def test_media_settings_parse() -> None:
    settings = Settings(database_url="x", media_derived_sizes="a:1, b:20")
    assert settings.derived_sizes == {"a": 1, "b": 20}
    assert "image/png" in settings.allowed_content_types
