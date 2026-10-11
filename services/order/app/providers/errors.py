"""Typed provider errors (RFC 0005 §3.1.1.4): business failures are never retried,
transient ones are."""


class PaymentDeclined(Exception):
    """A declined card is retried zero times."""


class AddressRejected(Exception):
    """The carrier refuses the address; not retryable."""


class ProviderTransientError(Exception):
    """A network blip or provider hiccup; retried under the activity's policy."""
