"""``/api/v1/notifications/stream-ticket`` response (RFC 0007 §3.1.2.1)."""

from app.core.schemas import OutModel


class StreamTicketOut(OutModel):
    ticket: str
    expires_in: int
