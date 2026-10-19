"""Single-use stream tickets (C19): ``nt:ticket:{ticket}`` → ``{user_id, client_id, jwt_exp}``.

``EventSource`` cannot send ``Authorization``; putting the JWT in the URL
would leak it into logs. A ticket is 32 CSPRNG bytes, lives 60 s, and is
redeemed with an atomic ``GETDEL`` so it works once. Tickets are never logged.
"""

import json
import re
import secrets
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis.asyncio import Redis

_SHAPE = re.compile(r"[A-Za-z0-9_-]{32,64}")


def ticket_key(ticket: str) -> str:
    return f"nt:ticket:{ticket}"


@dataclass(frozen=True)
class Ticket:
    user_id: str
    client_id: str | None
    jwt_exp: int


async def mint(redis: "Redis", ticket: Ticket, *, ttl_seconds: int) -> str:
    token = secrets.token_urlsafe(32)
    value = json.dumps(
        {"user_id": ticket.user_id, "client_id": ticket.client_id, "jwt_exp": ticket.jwt_exp}
    )
    await redis.set(ticket_key(token), value, ex=ttl_seconds)
    return token


async def redeem(redis: "Redis", token: str) -> Ticket | None:
    """The ticket's claims, or ``None`` if it is malformed, unknown, expired or spent."""
    if not _SHAPE.fullmatch(token):
        return None
    raw = await redis.getdel(ticket_key(token))
    if raw is None:
        return None
    try:
        data = json.loads(raw)
        return Ticket(
            user_id=str(data["user_id"]),
            client_id=data.get("client_id"),
            jwt_exp=int(data["jwt_exp"]),
        )
    except (ValueError, KeyError, TypeError):
        return None
