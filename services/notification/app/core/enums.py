"""Vocabularies shared by more than one notification entity (RFC 0007 §3.1.1.2).

The only place these values live (R8). A vocabulary used by one entity lives
in that entity's own ``enums.py``.
"""

from enum import StrEnum


class CallerRole(StrEnum):
    """Role claim values as issued by identity. Read from tokens, never stored."""

    CUSTOMER = "customer"
    MERCHANT = "merchant"
    ADMIN = "admin"
