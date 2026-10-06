"""Media request validation (RFC 0004 §3.1.2)."""

import pytest
from pydantic import ValidationError

from app.media.v1.schemas import MediaCreate


def test_media_requires_https() -> None:
    MediaCreate(url="https://cdn.example.com/a.jpg", version=1)
    for bad in ("http://cdn.example.com/a.jpg", "not a url", "ftp://x/y"):
        with pytest.raises(ValidationError):
            MediaCreate(url=bad, version=1)
