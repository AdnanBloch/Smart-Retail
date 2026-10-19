"""Daily token budgets, Postgres only (E10, AST-D21 as amended by AST-D34).

Three levels: the per-request bound is context assembly and ``LLM_MAX_TOKENS``;
per scope (``user:<uuid>`` or ``ip:<sha256>``) and ``global`` per UTC day are
here. :meth:`check` runs before any stream opens and raises ``429`` with
``Retry-After`` to the next UTC midnight; :meth:`charge` is one atomic upsert
per scope in the caller's transaction (the one that writes the message or
generation). Check-then-charge can overshoot by one request's spend, which
the aggregate invariant allows (§3.1.1.2).
"""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.errors import BudgetExceeded
from app.v1.generations.models import TokenBudget

GLOBAL = "global"


def utc_today(now: datetime | None = None) -> date:
    return (now or datetime.now(UTC)).astimezone(UTC).date()


def next_reset(now: datetime | None = None) -> datetime:
    today = utc_today(now)
    return datetime(today.year, today.month, today.day, tzinfo=UTC) + timedelta(days=1)


def scope_kind(scope: str) -> str:
    return scope.split(":", 1)[0]


@dataclass(frozen=True)
class Usage:
    scope: str
    tokens_used: int
    requests: int
    cost_usd: Decimal
    limit: int
    resets_at: datetime


class BudgetRepo:
    def __init__(self, session: AsyncSession, settings: Settings) -> None:
        self.session = session
        self.settings = settings

    def limit_for(self, scope: str) -> int:
        if scope == GLOBAL:
            return self.settings.daily_token_budget_global
        return self.settings.daily_token_budget_user

    async def usage(self, scope: str, *, now: datetime | None = None) -> Usage:
        row = await self.session.get(TokenBudget, (scope, utc_today(now)))
        return Usage(
            scope=scope,
            tokens_used=row.tokens_used if row else 0,
            requests=row.requests if row else 0,
            cost_usd=row.cost_usd if row else Decimal(0),
            limit=self.limit_for(scope),
            resets_at=next_reset(now),
        )

    async def check(self, *scopes: str, now: datetime | None = None) -> None:
        """Raises :class:`BudgetExceeded` if any scope has spent its daily limit."""
        for scope in (*scopes, GLOBAL):
            usage = await self.usage(scope, now=now)
            if usage.tokens_used >= usage.limit:
                raise BudgetExceeded(scope_kind(scope), now=now)

    async def charge(
        self, scope: str, tokens: int, cost: Decimal, *, now: datetime | None = None
    ) -> None:
        """Adds one request's spend to ``scope`` and ``global`` atomically."""
        day = utc_today(now)
        for target in dict.fromkeys((scope, GLOBAL)):
            insert = pg_insert(TokenBudget).values(
                scope=target, date=day, tokens_used=tokens, requests=1, cost_usd=cost
            )
            await self.session.execute(
                insert.on_conflict_do_update(
                    index_elements=["scope", "date"],
                    set_={
                        "tokens_used": TokenBudget.tokens_used + insert.excluded.tokens_used,
                        "requests": TokenBudget.requests + 1,
                        "cost_usd": TokenBudget.cost_usd + insert.excluded.cost_usd,
                        "updated_at": func.now(),
                    },
                )
            )

    async def today(self, *, limit: int = 100, now: datetime | None = None) -> list[Usage]:
        rows = (
            await self.session.scalars(
                select(TokenBudget)
                .where(TokenBudget.date == utc_today(now))
                .order_by(TokenBudget.tokens_used.desc())
                .limit(limit)
            )
        ).all()
        return [
            Usage(
                r.scope,
                r.tokens_used,
                r.requests,
                r.cost_usd,
                self.limit_for(r.scope),
                next_reset(now),
            )
            for r in rows
        ]


def cost_of(settings: Settings, model: str, prompt_tokens: int, completion_tokens: int) -> Decimal:
    price_in, price_out = settings.price(model)
    cost = (Decimal(prompt_tokens) * price_in + Decimal(completion_tokens) * price_out) / Decimal(
        1_000_000
    )
    return cost.quantize(Decimal("0.000001"))
