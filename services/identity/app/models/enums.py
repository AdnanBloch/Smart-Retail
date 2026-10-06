"""identity vocabularies (RFC 0003 §3.1.1.3). The only place these values live (R8)."""

from enum import StrEnum

from smartretail_common.enums import DomainEnum


class UserRole(DomainEnum):
    CUSTOMER = "customer"
    MERCHANT = "merchant"
    ADMIN = "admin"


class MerchantStatus(DomainEnum):
    PENDING = "pending"
    APPROVED = "approved"
    SUSPENDED = "suspended"


# Roles a caller may choose at registration; admin is never self-assigned (I3).
SELF_REGISTERABLE_ROLES = frozenset({UserRole.CUSTOMER, UserRole.MERCHANT})


class TokenType(StrEnum):
    """Value of the ``typ`` claim. Not persisted."""

    ACCESS = "access"
    SERVICE = "service"
