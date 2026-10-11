"""``Idempotency-Key`` mechanism (RFC 0000 B7, RFC 0005 ORD-7).

Only the pieces that hold no business rule live here: reading and
canonicalising the header, and hashing a request body. The claim table and
its rules belong to the service that owns the aggregate (R8); in Milestone 2
that is ``order``.
"""

import hashlib
import json
import uuid
from decimal import Decimal
from typing import Any

from smartretail_common.errors import BadRequest

HEADER = "Idempotency-Key"
REPLAYED_HEADER = "Idempotency-Replayed"
MAX_KEY_LENGTH = 255


class IdempotencyKeyRequired(BadRequest):
    type_slug = "idempotency-key-required"
    title = "Idempotency-Key header is required"


class IdempotencyKeyInvalid(BadRequest):
    type_slug = "idempotency-key-invalid"
    title = "Idempotency-Key header is malformed"


def canonical_key(raw: str | None) -> str:
    """Trimmed; lower-cased when it parses as a UUID. 1-255 printable ASCII."""
    if raw is None or not raw.strip():
        raise IdempotencyKeyRequired()
    key = raw.strip()
    if len(key) > MAX_KEY_LENGTH or not (key.isascii() and key.isprintable()):
        raise IdempotencyKeyInvalid(f"Use 1-{MAX_KEY_LENGTH} printable ASCII characters")
    try:
        return str(uuid.UUID(key))
    except ValueError:
        return key


def _normalise(value: Any) -> Any:
    # Numbers are normalised so "10", 10 and 10.0 hash alike; list order is kept.
    if isinstance(value, dict):
        return {str(k): _normalise(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_normalise(v) for v in value]
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, int | float | Decimal):
        normal = Decimal(str(value)).normalize()
        return format(normal, "f")
    return str(value)


def request_hash(body: Any) -> str:
    """SHA-256 over canonical JSON: sorted keys, no whitespace, normalised numbers."""
    canonical = json.dumps(_normalise(body), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()
