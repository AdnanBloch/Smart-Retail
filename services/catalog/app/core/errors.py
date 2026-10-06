"""catalog problem types (RFC 0004 §3.1.2.3)."""

from smartretail_common.errors import Conflict, NotFound, NotImplementedYet, Unprocessable


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


class PublishingNotAvailable(NotImplementedYet):
    type_slug = "publishing-not-available"
    title = "Publishing is not available yet"

    def __init__(self) -> None:
        super().__init__(
            "Publishing is an orchestrated workflow (validation, media, indexing) that "
            "arrives in Milestone 2. Products can move between draft, inactive and archived."
        )


def unknown_reference(field: str, message: str) -> Unprocessable:
    return Unprocessable(
        message, violations=[{"field": field, "message": message, "code": "unknown"}]
    )
