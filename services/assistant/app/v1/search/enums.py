"""search/ vocabularies (RFC 0009 §3.1.1.2)."""

from smartretail_common.enums import DomainEnum


class ChunkType(DomainEnum):
    IDENTITY = "identity"
    DESCRIPTION = "description"
    ATTRIBUTES = "attributes"
    VARIANTS = "variants"
    CATEGORY = "category"


class IndexStatus(DomainEnum):
    INDEXED = "indexed"
    PENDING = "pending"
    FAILED = "failed"
    REMOVED = "removed"


class SearchMode(DomainEnum):
    HYBRID = "hybrid"
    DENSE = "dense"
    KEYWORD = "keyword"


class IndexOutcome(DomainEnum):
    """What one indexing call did (metrics label ``outcome``, §3.1.1.7)."""

    INDEXED = "indexed"
    REMOVED = "removed"
    SKIPPED_STALE = "skipped_stale"
