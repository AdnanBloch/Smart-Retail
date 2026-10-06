"""Vocabularies shared by more than one catalog entity (RFC 0004 §3.1.1.3).

The only place these values live (R8). A vocabulary used by one entity lives
in that entity's own ``enums.py``.
"""

from enum import StrEnum


class CallerRole(StrEnum):
    """Role claim values as issued by identity. Read from tokens, never stored."""

    CUSTOMER = "customer"
    MERCHANT = "merchant"
    ADMIN = "admin"
