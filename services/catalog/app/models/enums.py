"""catalog vocabularies (RFC 0004 §3.1.1.3). The only place these values live (R8)."""

from enum import StrEnum

from smartretail_common.enums import DomainEnum


class ProductStatus(DomainEnum):
    # `publishing` arrives with the publishing workflow in Milestone 2.
    DRAFT = "draft"
    PUBLISHED = "published"
    INACTIVE = "inactive"
    ARCHIVED = "archived"


class ProductAuditAction(DomainEnum):
    CREATED = "created"
    UPDATED = "updated"
    STATUS_CHANGED = "status_changed"
    DELETED = "deleted"
    VARIANT_ADDED = "variant_added"
    VARIANT_UPDATED = "variant_updated"
    VARIANT_REMOVED = "variant_removed"
    MEDIA_ADDED = "media_added"
    MEDIA_REMOVED = "media_removed"
    MEDIA_PROCESSED = "media_processed"
    PUBLISH_FAILED = "publish_failed"


class CallerRole(StrEnum):
    """Role claim values as issued by identity. Read from tokens, never stored."""

    CUSTOMER = "customer"
    MERCHANT = "merchant"
    ADMIN = "admin"
