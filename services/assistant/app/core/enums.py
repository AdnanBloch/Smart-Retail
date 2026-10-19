"""Vocabularies shared by more than one assistant entity (RFC 0009 §3.1.1.2).

A vocabulary used by one entity lives in that entity's own ``enums.py``.
"""

from enum import StrEnum

from smartretail_common.enums import DomainEnum


class CallerRole(StrEnum):
    """Role claim values as issued by identity. Read from tokens, never stored."""

    CUSTOMER = "customer"
    MERCHANT = "merchant"
    ADMIN = "admin"


class ContentType(DomainEnum):
    """Merchant content types (Part B §2B); shared by generations/ and prompts/."""

    DESCRIPTION = "description"
    SEO_TITLE = "seo_title"
    FAQ = "faq"
    PROMOTIONAL = "promotional"
    BENEFITS_SUMMARY = "benefits_summary"
    CROSS_SELL = "cross_sell"


class FinishReason(DomainEnum):
    """Why a turn or generation ended; shared by chat/ and generations/ (S4 passthrough)."""

    STOP = "stop"
    LENGTH = "length"
    ERROR = "error"
    REFUSED = "refused"
    BUDGET_EXCEEDED = "budget_exceeded"
