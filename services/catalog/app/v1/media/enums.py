"""Media vocabularies (RFC 0004 §3.1.1.3). The only place these values live (R8)."""

from smartretail_common.enums import DomainEnum


class MediaProcessingStatus(DomainEnum):
    PENDING = "pending"
    READY = "ready"
    FAILED = "failed"
