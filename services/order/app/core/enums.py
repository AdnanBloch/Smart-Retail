"""Vocabularies shared by more than one order module (RFC 0005 §3.1.1.2).

The only place these values live (R8). Order vocabularies, including the
``Idempotency-Key`` states, are in ``app/v1/orders/enums.py``.
"""

from enum import StrEnum


class CallerRole(StrEnum):
    """Role claim values as issued by identity. Read from tokens, never stored."""

    CUSTOMER = "customer"
    MERCHANT = "merchant"
    ADMIN = "admin"
