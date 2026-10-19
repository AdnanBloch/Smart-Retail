"""``X-Client-Id`` / ack ``client_id`` shape (C21): opaque, 8-64 of [A-Za-z0-9_-]."""

import uuid

import pytest
from pydantic import ValidationError

from app.v1.notifications.schemas import AckIn
from smartretail_common.client_id import valid_client_id


@pytest.mark.parametrize("value", ["abcdefgh", "A" * 64, "b3f1-x_9Z0aa"])
def test_valid_client_ids(value) -> None:
    assert valid_client_id(value) == value
    assert AckIn(client_id=value).client_id == value


@pytest.mark.parametrize("value", ["short", "A" * 65, "has space!", "ünïcode1", ""])
def test_malformed_client_ids(value) -> None:
    assert valid_client_id(value) is None
    with pytest.raises(ValidationError):
        AckIn(client_id=value)


def test_ack_body_rules() -> None:
    body = AckIn.model_validate(
        {"client_id": "client-01", "up_to_seq": 3, "ids": [str(uuid.uuid4())]}
    )
    assert body.up_to_seq == 3 and len(body.ids) == 1
    with pytest.raises(ValidationError):
        AckIn.model_validate({"client_id": "client-01", "up_to_seq": -1})
    with pytest.raises(ValidationError):
        AckIn.model_validate({"client_id": "client-01", "extra": 1})
    with pytest.raises(ValidationError):
        AckIn.model_validate({"client_id": "client-01", "ids": ["nope"]})
