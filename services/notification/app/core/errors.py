"""notification problem types (RFC 0007 §3.1.2.1)."""

from smartretail_common.errors import BadRequest, DomainError, NotFound, Unauthorized


class NotificationNotFound(NotFound):
    """Unknown id or another user's: indistinguishable by design (§3.1.2.2)."""

    type_slug = "notification-not-found"
    title = "Notification not found"


class ConflictingReadFilters(BadRequest):
    type_slug = "conflicting-read-filters"
    title = "read and unread_only disagree"


class MalformedAck(BadRequest):
    type_slug = "malformed-ack"
    title = "Malformed acknowledgement"


class TooManyRequests(DomainError):
    status = 429
    type_slug = "rate-limited"
    title = "Too many requests"

    def __init__(self, detail: str, *, retry_after: int) -> None:
        super().__init__(detail, headers={"Retry-After": str(retry_after)})


class StreamTicketInvalid(Unauthorized):
    type_slug = "stream-ticket-invalid"
    title = "Stream ticket missing, unknown, expired or already used"


class TooManyStreams(DomainError):
    status = 429
    type_slug = "too-many-streams"
    title = "Too many concurrent streams"

    def __init__(self, limit: int) -> None:
        super().__init__(
            f"At most {limit} concurrent streams per user", headers={"Retry-After": "10"}
        )
