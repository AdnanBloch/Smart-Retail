"""Vocabularies of request-level machinery (RFC 0005 §3.1.1.2). The only place
these values live (R8); order vocabularies are in ``app/orders/enums.py``."""

from enum import StrEnum

from smartretail_common.enums import DomainEnum


class IdempotencyState(DomainEnum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


class CallerRole(StrEnum):
    """Role claim values as issued by identity. Read from tokens, never stored."""

    CUSTOMER = "customer"
    MERCHANT = "merchant"
    ADMIN = "admin"
