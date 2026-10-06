"""Pydantic bases every entity's versioned schemas build on."""

from typing import Annotated

import email_validator
from pydantic import BaseModel, ConfigDict, Field

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


# Field types used by more than one entity's schemas.
FullName = Annotated[str, Field(min_length=1, max_length=200)]
Phone = Annotated[str, Field(min_length=1, max_length=32)]
Password = Annotated[str, Field(min_length=1, max_length=1024)]
