"""generations/ vocabularies (RFC 0009 §3.1.1.2)."""

from smartretail_common.enums import DomainEnum


class BulkJobStatus(DomainEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIALLY_FAILED = "partially_failed"
    FAILED = "failed"
    CANCELLED = "cancelled"
