"""Upload validation and derivative generation (RFC 0004 §3.1.1.4 ``process_media``).

Pure functions over bytes: no storage, no database. Uploads are checked
against the declared type, the extension and the magic bytes; derivatives
are WebP, longest edge per configured size, with EXIF stripped and a pixel
ceiling that refuses decompression bombs.
"""

import io
import warnings
from pathlib import PurePath

from PIL import Image, ImageOps, UnidentifiedImageError

from app.core.errors import MediaTooLarge, UnsupportedMediaType

# content type → (extension, magic-byte test)
_SIGNATURES: dict[str, tuple[str, tuple[bytes, ...]]] = {
    "image/jpeg": ("jpg", (b"\xff\xd8\xff",)),
    "image/png": ("png", (b"\x89PNG\r\n\x1a\n",)),
    "image/webp": ("webp", (b"RIFF",)),
}
_EXTENSIONS = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png", "webp": "image/webp"}


class MediaUndecodable(Exception):
    """Not an image we can decode, or over the pixel ceiling. Never retried."""


def _magic_matches(content_type: str, data: bytes) -> bool:
    _, prefixes = _SIGNATURES[content_type]
    if not any(data.startswith(p) for p in prefixes):
        return False
    return content_type != "image/webp" or data[8:12] == b"WEBP"


def check_upload(
    data: bytes,
    *,
    declared_type: str | None,
    filename: str | None,
    allowed: frozenset[str],
    max_bytes: int,
) -> str:
    """Returns the canonical extension, or raises 413 / 415."""
    if len(data) > max_bytes:
        raise MediaTooLarge(f"Uploads are limited to {max_bytes} bytes")
    if not data:
        raise UnsupportedMediaType("The upload is empty")
    content_type = (declared_type or "").split(";")[0].strip().lower()
    if content_type not in allowed or content_type not in _SIGNATURES:
        raise UnsupportedMediaType(f"Allowed types: {', '.join(sorted(allowed))}")
    suffix = PurePath(filename or "").suffix.lower().lstrip(".")
    if suffix and _EXTENSIONS.get(suffix) != content_type:
        raise UnsupportedMediaType("The file extension does not match its content type")
    if not _magic_matches(content_type, data):
        raise UnsupportedMediaType("The file content does not match its content type")
    return _SIGNATURES[content_type][0]


def derive(data: bytes, sizes: dict[str, int], *, max_pixels: int) -> dict[str, bytes]:
    """One WebP per size, longest edge ``sizes[name]``; never upscales."""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            image = Image.open(io.BytesIO(data))
            width, height = image.size
            if width * height > max_pixels:
                raise MediaUndecodable(f"{width}x{height} exceeds {max_pixels} pixels")
            image.load()
    except MediaUndecodable:
        raise
    except (UnidentifiedImageError, OSError, Image.DecompressionBombError, Warning) as exc:
        raise MediaUndecodable(f"cannot decode image: {exc}") from exc
    # Apply the EXIF orientation, then drop all metadata by re-encoding.
    oriented: Image.Image = ImageOps.exif_transpose(image)
    oriented = oriented.convert("RGBA" if "A" in oriented.getbands() else "RGB")
    out: dict[str, bytes] = {}
    for name, edge in sizes.items():
        copy = oriented.copy()
        copy.thumbnail((edge, edge))
        buffer = io.BytesIO()
        copy.save(buffer, format="WEBP", quality=82)
        out[name] = buffer.getvalue()
    return out
