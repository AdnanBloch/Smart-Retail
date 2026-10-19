"""Response contracts of ``/api/v1/analytics/*`` (RFC 0008 §3.1.2.1).

Money is a string with its currency beside it, never a JSON number (S2);
ratios are 4-decimal strings, ``null`` when the denominator is zero. Every
response carries ``as_of``: the ``occurred_at`` of the newest applied event.
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class Money(BaseModel):
    amount: str
    currency: str


class ScopeOut(BaseModel):
    merchant_id: str | None


class Fresh(BaseModel):
    as_of: datetime | None


class Overview(Fresh):
    total_customers: int
    total_merchants: int
    total_products: int
    orders_total: int
    revenue_total: Money
    assistant_interactions: int
    assistant_questions_asked: int
    assistant_questions_answered: int
    assistant_refusals: int


class OrdersPoint(BaseModel):
    period: str
    orders_created: int
    orders_confirmed: int
    orders_failed: int
    confirmation_rate: str | None


class OrdersSeries(Fresh):
    scope: ScopeOut
    granularity: str
    series: list[OrdersPoint]


class RevenueTotals(BaseModel):
    currency: str
    revenue: str
    orders_confirmed: int
    average_order_value: str | None


class RevenuePoint(RevenueTotals):
    period: str


class RevenueSeries(Fresh):
    scope: ScopeOut
    granularity: str
    basis: Literal["order_total", "merchant_line_total"]
    series: list[RevenuePoint]
    # One entry per currency: daily series never mix currencies (ANL-4).
    window: list[RevenueTotals]


class PopularProduct(BaseModel):
    product_id: str
    name: str | None
    merchant_id: str | None
    quantity_sold: int
    revenue: Money
    order_count: int
    last_sold_at: datetime | None


class PopularWindow(BaseModel):
    start: str
    end: str


class PopularProducts(Fresh):
    scope: ScopeOut
    rank_by: str
    window: PopularWindow | None
    items: list[PopularProduct]


class StockoutPoint(BaseModel):
    period: str
    stockouts: int
    order_attempts: int
    stockout_rate: str | None


class StockoutWindow(BaseModel):
    stockouts: int
    order_attempts: int
    stockout_rate: str | None


class StockoutVariant(BaseModel):
    variant_id: str
    product_id: str | None
    stockouts: int


class Stockouts(Fresh):
    scope: ScopeOut
    granularity: str
    series: list[StockoutPoint]
    window: StockoutWindow
    top_variants: list[StockoutVariant]


class FailureItem(BaseModel):
    source: str | None = None
    reason: str | None = None
    count: int


class Failures(Fresh):
    items: list[FailureItem]
