"""What crosses the Temporal boundary for ``ProductPublishingWorkflow``.

Identifiers and step results only (B2): never the product. Plain dataclasses
of strings so the default converter round-trips them and the workflow
sandbox can import this module.
"""

from dataclasses import dataclass

WORKFLOW_NAME = "ProductPublishingWorkflow"
CURRENT_STEP_QUERY = "current_step"


@dataclass(frozen=True)
class PublishInput:
    product_id: str
    workflow_id: str
    actor_id: str
    correlation_id: str | None = None


@dataclass(frozen=True)
class MediaInput:
    publish: PublishInput
    media_id: str


@dataclass(frozen=True)
class FailureInput:
    publish: PublishInput
    failed_step: str
    reason: str


@dataclass(frozen=True)
class PublishOutcome:
    status: str  # "published" | "failed"
    failed_step: str | None = None
    reason: str | None = None


# Activity names; also the PublishStep values recorded on failure.
VALIDATE_PRODUCT = "validate_product"
PROCESS_MEDIA = "process_media"
BUILD_SEARCH_DOCUMENT = "build_search_document"
SYNC_DOWNSTREAM = "sync_downstream"
MARK_PUBLISHED = "mark_published"
RECORD_PUBLISH_FAILURE = "record_publish_failure"
