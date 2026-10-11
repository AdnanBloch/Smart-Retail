"""catalog problem types (RFC 0004 §3.1.2.3)."""

from smartretail_common.errors import (
    Conflict,
    DependencyUnavailable,
    DomainError,
    NotFound,
    Unprocessable,
)


class ProductNotFound(NotFound):
    type_slug = "product-not-found"
    title = "Product not found"


class VariantNotFound(NotFound):
    type_slug = "variant-not-found"
    title = "Variant not found"


class MediaNotFound(NotFound):
    type_slug = "media-not-found"
    title = "Media not found"


class CategoryNotFound(NotFound):
    type_slug = "category-not-found"
    title = "Category not found"


class VersionConflict(Conflict):
    type_slug = "version-conflict"
    title = "The product was modified by someone else"

    def __init__(self, current_version: int) -> None:
        super().__init__(
            f"Current version is {current_version}; reload and retry",
            extras={"current_version": current_version},
        )


class DuplicateSku(Conflict):
    type_slug = "duplicate-sku"
    title = "SKU already exists"


class DuplicateSlug(Conflict):
    type_slug = "duplicate-slug"
    title = "Slug already exists"


class CategoryInUse(Conflict):
    type_slug = "category-in-use"
    title = "Category is referenced by products or child categories"


class ProductArchived(Conflict):
    type_slug = "product-archived"
    title = "Archived products cannot be modified"


class ProductPublishing(Conflict):
    type_slug = "product-publishing"
    title = "The product is being published"

    def __init__(self) -> None:
        super().__init__("Wait for the publish to finish; see publish-status")


class PublishAlreadyRunning(Conflict):
    type_slug = "publish-already-running"
    title = "A publish is already running"

    def __init__(self, workflow_id: str) -> None:
        super().__init__(
            f"Workflow {workflow_id} is still running", extras={"workflow_id": workflow_id}
        )


class PublishStartFailed(DependencyUnavailable):
    """CAT-9: the workflow could not be started; the product is back in draft."""

    type_slug = "publish-start-failed"
    title = "The publishing workflow could not be started"


class PublishSuperseded(Conflict):
    """The calling workflow is no longer the product's current publish."""

    type_slug = "publish-superseded"
    title = "This publish was superseded"


class ProductIncomplete(Unprocessable):
    type_slug = "product-incomplete"
    title = "The product is not complete enough to publish"

    def __init__(self, missing: list[str]) -> None:
        super().__init__(
            f"Missing: {', '.join(missing)}",
            violations=[
                {"field": f, "message": "required to publish", "code": "missing"} for f in missing
            ],
        )
        self.extras["missing"] = missing


class WouldBreakPublished(Conflict):
    """A change to a published product that would leave it incomplete (RFC 0004 §3.1.2.1)."""

    type_slug = "would-break-published"
    title = "A published product must stay complete"

    def __init__(self, missing: list[str]) -> None:
        super().__init__(
            f"This change would leave the published product without: {', '.join(missing)}. "
            "Unpublish it first.",
            extras={"missing": missing},
        )


class StorageUnavailable(DependencyUnavailable):
    type_slug = "storage-unavailable"
    title = "Media storage is unavailable"


class MediaTooLarge(DomainError):
    status = 413
    type_slug = "media-too-large"
    title = "The upload is too large"


class UnsupportedMediaType(DomainError):
    status = 415
    type_slug = "unsupported-media-type"
    title = "The upload's type is not allowed"


def unknown_reference(field: str, message: str) -> Unprocessable:
    return Unprocessable(
        message, violations=[{"field": field, "message": message, "code": "unknown"}]
    )
