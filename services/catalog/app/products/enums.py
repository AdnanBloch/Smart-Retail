"""Product vocabularies (RFC 0004 §3.1.1.3). The only place these values live (R8)."""

from enum import StrEnum

from smartretail_common.enums import DomainEnum


class ProductStatus(DomainEnum):
    DRAFT = "draft"
    # Reachable only on the publish path: written when a publish is accepted and
    # left by the publishing workflow (RFC 0004 T3-T6).
    PUBLISHING = "publishing"
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


class PublishStep(DomainEnum):
    """``product_publish_failures.failed_step`` (CAT-5): the activity that failed."""

    START = "start"
    VALIDATE_PRODUCT = "validate_product"
    PROCESS_MEDIA = "process_media"
    BUILD_SEARCH_DOCUMENT = "build_search_document"
    SYNC_DOWNSTREAM = "sync_downstream"
    MARK_PUBLISHED = "mark_published"


class PublishState(StrEnum):
    """``GET /products/{id}/publish-status`` ``state``; computed, never stored."""

    IDLE = "idle"
    RUNNING = "running"
    PUBLISHED = "published"
    FAILED = "failed"


class UnpublishReason(StrEnum):
    MERCHANT_UNPUBLISHED = "merchant_unpublished"
    MERCHANT_SUSPENDED = "merchant_suspended"
    ARCHIVED = "archived"


class CatalogEvent(StrEnum):
    """Event types written to the outbox (RFC 0000 §3.1.2.3)."""

    PUBLISHED = "catalog.product.published"
    UPDATED = "catalog.product.updated"
    UNPUBLISHED = "catalog.product.unpublished"
