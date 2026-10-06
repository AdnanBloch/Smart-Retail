import pytest
from pydantic import ValidationError

from app.models.media import MediaCreate
from app.models.product import ProductCreate, ProductPatch
from app.services.listing_repo import ProductFilters
from app.utils.helpers import diff
from smartretail_common.errors import Unprocessable


def test_attributes_validation() -> None:
    ProductCreate(sku="A", name="n", base_price="1.00", attributes={"tags": ["a", "b"], "n": 1})
    for bad in ({"x": {"nested": 1}}, {"": 1}, {f"k{i}": i for i in range(51)}, {"x": [[1]]}):
        with pytest.raises(ValidationError):
            ProductCreate(sku="A", name="n", base_price="1.00", attributes=bad)


def test_price_and_currency() -> None:
    for bad in ({"base_price": "-1"}, {"base_price": "1.001"}, {"currency": "usd"}):
        with pytest.raises(ValidationError):
            ProductCreate(**{"sku": "A", "name": "n", "base_price": "1.00", **bad})


def test_patch_rejects_null_required_fields_but_allows_null_description() -> None:
    ProductPatch(version=1, description=None, category_id=None)
    with pytest.raises(ValidationError):
        ProductPatch(version=1, name=None)
    with pytest.raises(ValidationError):
        ProductPatch(name="x")  # version is mandatory


def test_media_requires_https() -> None:
    MediaCreate(url="https://cdn.example.com/a.jpg", version=1)
    for bad in ("http://cdn.example.com/a.jpg", "not a url", "ftp://x/y"):
        with pytest.raises(ValidationError):
            MediaCreate(url=bad, version=1)


def test_diff_only_changed_fields() -> None:
    assert diff({"a": 1, "b": 2}, {"a": 1, "b": 3}) == {"b": {"from": 2, "to": 3}}


def test_price_range_validated() -> None:
    with pytest.raises(Unprocessable):
        ProductFilters(min_price=10, max_price=5)
