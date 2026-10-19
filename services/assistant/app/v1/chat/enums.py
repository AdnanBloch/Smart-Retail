"""chat/ vocabularies (RFC 0009 §3.1.1.2). ``FinishReason`` is shared: ``app.core.enums``."""

from smartretail_common.enums import DomainEnum


class MessageRole(DomainEnum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"


class Intent(DomainEnum):
    """The classifier's routes. ``smalltalk`` is added to the RFC's five so a
    greeting is answered without retrieval (passthrough read policy, S4)."""

    PRODUCT_SEARCH = "product_search"
    COMPARE = "compare"
    FOLLOW_UP = "follow_up"
    TOO_VAGUE = "too_vague"
    SMALLTALK = "smalltalk"
    OUT_OF_SCOPE = "out_of_scope"


RETRIEVING = frozenset({Intent.PRODUCT_SEARCH, Intent.COMPARE, Intent.FOLLOW_UP})
