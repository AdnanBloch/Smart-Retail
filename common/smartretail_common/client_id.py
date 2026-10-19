"""``X-Client-Id`` (C21): an opaque per-device token chosen by the client.

It only ever narrows *where* a notification is shown first, never what a
caller may do, so a malformed value is treated as absent rather than refused.
"""

import re

from fastapi import Request

CLIENT_ID = re.compile(r"[A-Za-z0-9_-]{8,64}")
HEADER = "x-client-id"


def valid_client_id(value: str | None) -> str | None:
    return value if value is not None and CLIENT_ID.fullmatch(value) else None


def client_id(request: Request) -> str | None:
    return valid_client_id(request.headers.get(HEADER))
