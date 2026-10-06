"""Vocabularies shared by more than one identity entity (RFC 0003 §3.1.1.3).

The only place these values live (R8). A vocabulary used by one entity lives
in that entity's own ``enums.py``.
"""

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
