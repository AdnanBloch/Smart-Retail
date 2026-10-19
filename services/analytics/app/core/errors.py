"""analytics problem types (RFC 0008 §3.1.2.1)."""

from smartretail_common.errors import DependencyUnavailable, Unprocessable


class MongoUnavailable(DependencyUnavailable):
    """MongoDB is unreachable. The API answers 503; the consumer base waits on
    it instead of dead-lettering (a store outage is never the message's fault)."""

    type_slug = "analytics-store-unavailable"
    title = "The analytics store is unavailable"

    def __init__(self, detail: str = "MongoDB is unreachable") -> None:
        super().__init__(detail, retry_after=5)


class TransactionsUnavailable(RuntimeError):
    """The server is not a replica set, so multi-document transactions are
    impossible and the consumer would double count or lose increments (ANL-1)."""


class InvalidWindow(Unprocessable):
    type_slug = "invalid-window"
    title = "Invalid reporting window"

    def __init__(self, detail: str, field: str = "from") -> None:
        super().__init__(detail, violations=[{"field": field, "message": detail, "code": "window"}])


class MerchantScopeMismatch(Unprocessable):
    type_slug = "merchant-scope-mismatch"
    title = "A merchant can only query its own figures"

    def __init__(self) -> None:
        super().__init__(
            "merchant_id differs from the merchant in your token",
            violations=[
                {"field": "merchant_id", "message": "must equal your own id", "code": "scope"}
            ],
        )
