"""auth vocabularies (RFC 0003 §3.1.1.3)."""

from enum import StrEnum


class TokenType(StrEnum):
    """Value of the ``typ`` claim. Not persisted."""

    ACCESS = "access"
    SERVICE = "service"
