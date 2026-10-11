import uuid
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from temporalio.exceptions import ActivityError, ApplicationError, RetryState

from app.core.errors import ProductIncomplete, StorageUnavailable
from app.media.enums import MediaProcessingStatus as M
from app.products import completeness, search_document
from app.workflows.publishing import failure_reason
from smartretail_common.temporal import as_activity_errors


def product(**overrides):
    fields = {
        "id": uuid.uuid4(),
        "merchant_id": uuid.uuid4(),
        "version": 3,
        "name": "Shoe",
        "sku": "S",
        "description": "Nice",
        "category_id": uuid.uuid4(),
        "category": SimpleNamespace(id=uuid.uuid4(), name="Running"),
        "attributes": {},
        "base_price": Decimal("10.00"),
        "currency": "USD",
        "variants": [
            SimpleNamespace(
                id=uuid.uuid4(), sku="S-M", options={}, price_override=None, is_active=True
            ),
            SimpleNamespace(
                id=uuid.uuid4(),
                sku="S-L",
                options={},
                price_override=Decimal("12.50"),
                is_active=False,
            ),
        ],
        "media": [
            SimpleNamespace(
                id=uuid.uuid4(), url="u", alt_text=None, position=0, processing_status=M.READY
            )
        ],
    }
    return SimpleNamespace(**{**fields, **overrides})


def test_completeness() -> None:
    assert completeness.missing_fields(product()) == []
    bare = product(name=" ", description=None, category_id=None, variants=[], media=[])
    assert completeness.missing_fields(bare) == [
        "name",
        "description",
        "category_id",
        "variants",
        "media",
    ]
    pending = product(media=[SimpleNamespace(processing_status=M.PENDING)])
    assert completeness.missing_fields(pending) == []
    assert completeness.missing_fields(pending, require_ready_media=True) == ["media"]


def test_search_document_is_descriptive_and_jsonable() -> None:
    p = product()
    doc = search_document.build(
        p, category_path=["Footwear", "Running"], built_at=datetime(2026, 1, 1, tzinfo=UTC)
    )
    assert doc["category"]["path"] == ["Footwear", "Running"]
    assert [v["price"] for v in doc["variants"]] == ["10.00", "12.50"]
    assert doc["built_at"].startswith("2026-01-01") and doc["product_id"] == str(p.id)
    assert (
        search_document.build(product(category=None), category_path=[], built_at=datetime.now(UTC))[
            "category"
        ]
        is None
    )


def test_failure_reason_reads_the_innermost_cause() -> None:
    inner = ApplicationError("no stock rows", type="DependencyUnavailable")
    err = ActivityError(
        "activity failed",
        scheduled_event_id=1,
        started_event_id=2,
        identity="w",
        activity_type="sync_downstream",
        activity_id="1",
        retry_state=RetryState.MAXIMUM_ATTEMPTS_REACHED,
    )
    err.__cause__ = inner
    assert failure_reason(err) == "DependencyUnavailable: no stock rows"
    plain = ActivityError(
        "x",
        scheduled_event_id=1,
        started_event_id=2,
        identity="w",
        activity_type="a",
        activity_id="1",
        retry_state=None,
    )
    plain.__cause__ = TimeoutError("late")
    assert failure_reason(plain) == "TimeoutError: late"


async def test_domain_errors_map_to_retry_classes() -> None:
    async def incomplete():
        raise ProductIncomplete(["name"])

    async def storage():
        raise StorageUnavailable("down")

    with pytest.raises(ApplicationError) as business:
        await as_activity_errors(incomplete)
    assert business.value.non_retryable and business.value.type == "ProductIncomplete"
    with pytest.raises(ApplicationError) as transient:
        await as_activity_errors(storage)
    assert not transient.value.non_retryable
