import email_validator
from pydantic import BaseModel, ConfigDict

# Seed and demo accounts use *.local addresses (RFC 0003 T15), which
# email-validator rejects as special-use by default. Allow ".local" and
# ".test" so local development and tests can use them; the other reserved
# names (localhost, invalid, onion, arpa) stay rejected.
email_validator.SPECIAL_USE_DOMAIN_NAMES[:] = [
    d for d in email_validator.SPECIAL_USE_DOMAIN_NAMES if d not in {"local", "test"}
]


class StrictModel(BaseModel):
    """Request bodies reject unknown fields (RFC 0003 IDN-2, I4)."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class OutModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)
