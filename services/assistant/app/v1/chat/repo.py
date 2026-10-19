"""ConversationRepo: ownership, windowed memory, persistence of a turn (§3.1.3).

A conversation belongs to one ``user_id``, or — anonymous — to one
``session_id`` with no user (AST-13). Anything else is reported as not found,
so existence is not leaked. A turn is persisted in one transaction: the user
and assistant messages, the conversation counters, the budget charge and the
``assistant.interaction`` outbox row (A10, E13).
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.enums import FinishReason
from app.core.errors import ConversationNotFound, MessageNotFound
from app.core.events import emit_interaction, interaction_payload
from app.v1.chat.enums import RETRIEVING, Intent, MessageRole
from app.v1.chat.models import Conversation, Message
from app.v1.generations.budget_repo import BudgetRepo
from smartretail_common.pagination import PageParams, Slice


@dataclass(frozen=True)
class TurnRecord:
    conversation_id: uuid.UUID
    message_id: uuid.UUID
    user_id: str | None
    session_id: str | None
    question: str
    answer: str
    intent: str
    sources: list[dict[str, Any]]
    prompt_tokens: int
    completion_tokens: int
    model: str
    cost_usd: Decimal
    latency_ms: int
    time_to_first_token_ms: int | None
    finish_reason: FinishReason
    budget_scope: str
    correlation_id: str | None


def answered(intent: str, finish: FinishReason, sources: list[dict[str, Any]]) -> bool:
    """A grounded answer: not a refusal, clarification, error or budget rejection (AST-6)."""
    return (
        finish in (FinishReason.STOP, FinishReason.LENGTH)
        and Intent.has(intent)
        and Intent(intent) in RETRIEVING
        and bool(sources)
    )


class ConversationRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def owned(
        self, conversation_id: uuid.UUID, *, user_id: str | None, session_id: str | None
    ) -> Conversation:
        conversation = await self.session.get(Conversation, conversation_id)
        if conversation is None:
            raise ConversationNotFound()
        if user_id is not None:
            if conversation.user_id is None or str(conversation.user_id) != user_id:
                raise ConversationNotFound()
        elif conversation.user_id is not None or (
            session_id is None or conversation.session_id != session_id
        ):
            raise ConversationNotFound()
        return conversation

    async def history(self, conversation_id: uuid.UUID, *, turns: int) -> list[Message]:
        rows = (
            await self.session.scalars(
                select(Message)
                .where(Message.conversation_id == conversation_id)
                .order_by(Message.created_at.desc(), Message.role.desc())
                .limit(max(2, 2 * turns))
            )
        ).all()
        return list(reversed(rows))

    async def messages(self, conversation_id: uuid.UUID) -> list[Message]:
        return list(
            (
                await self.session.scalars(
                    select(Message)
                    .where(Message.conversation_id == conversation_id)
                    .order_by(Message.created_at, Message.role.desc())
                )
            ).all()
        )

    async def list_for_user(self, user_id: str, params: PageParams) -> Slice[Conversation]:
        where = Conversation.user_id == uuid.UUID(user_id)
        total = await self.session.scalar(
            select(func.count()).select_from(Conversation).where(where)
        )
        rows = (
            await self.session.scalars(
                select(Conversation)
                .where(where)
                .order_by(Conversation.last_message_at.desc().nulls_last(), Conversation.id)
                .limit(params.limit)
                .offset(params.offset)
            )
        ).all()
        return Slice(list(rows), int(total or 0), params)

    async def delete(self, conversation: Conversation) -> None:
        await self.session.delete(conversation)
        await self.session.commit()

    async def feedback(
        self, message_id: uuid.UUID, user_id: str, helpful: bool, comment: str | None
    ) -> None:
        row = (
            await self.session.execute(
                select(Message, Conversation)
                .join(Conversation, Conversation.id == Message.conversation_id)
                .where(Message.id == message_id)
            )
        ).first()
        if row is None:
            raise MessageNotFound()
        message, conversation = row
        if (
            conversation.user_id is None
            or str(conversation.user_id) != user_id
            or message.role != MessageRole.ASSISTANT
        ):
            raise MessageNotFound()
        message.feedback_helpful = helpful
        message.feedback_comment = comment
        await self.session.commit()

    async def persist_turn(self, record: TurnRecord, settings: Settings) -> None:
        """One transaction: messages, counters, budget charge, outbox row."""
        now = datetime.now(UTC)
        async with self.session.begin():
            conversation = await self.session.get(
                Conversation, record.conversation_id, with_for_update=True
            )
            if conversation is None:
                conversation = Conversation(
                    id=record.conversation_id,
                    user_id=uuid.UUID(record.user_id) if record.user_id else None,
                    session_id=record.session_id if record.user_id is None else None,
                    title=record.question[:80],
                )
                self.session.add(conversation)
                await self.session.flush()
            self.session.add(
                Message(
                    conversation_id=record.conversation_id,
                    role=MessageRole.USER,
                    content=record.question,
                    correlation_id=record.correlation_id,
                    created_at=now,
                )
            )
            self.session.add(
                Message(
                    id=record.message_id,
                    conversation_id=record.conversation_id,
                    role=MessageRole.ASSISTANT,
                    content=record.answer,
                    intent=record.intent,
                    sources=record.sources,
                    prompt_tokens=record.prompt_tokens,
                    completion_tokens=record.completion_tokens,
                    model=record.model,
                    cost_usd=record.cost_usd,
                    latency_ms=record.latency_ms,
                    time_to_first_token_ms=record.time_to_first_token_ms,
                    finish_reason=record.finish_reason,
                    correlation_id=record.correlation_id,
                    created_at=now + timedelta(microseconds=1),
                )
            )
            tokens = record.prompt_tokens + record.completion_tokens
            conversation.message_count = (conversation.message_count or 0) + 2
            conversation.total_tokens = (conversation.total_tokens or 0) + tokens
            conversation.last_message_at = now
            await BudgetRepo(self.session, settings).charge(
                record.budget_scope, tokens, record.cost_usd
            )
            emit_interaction(
                self.session,
                aggregate_type="conversation",
                aggregate_id=record.conversation_id,
                correlation_id=record.correlation_id,
                payload=interaction_payload(
                    interaction_id=record.message_id,
                    kind="chat",
                    conversation_id=record.conversation_id,
                    user_id=record.user_id,
                    intent=record.intent,
                    finish_reason=record.finish_reason,
                    answered=answered(record.intent, record.finish_reason, record.sources),
                    source_product_ids=[s["id"] for s in record.sources],
                    model=record.model,
                    input_tokens=record.prompt_tokens,
                    output_tokens=record.completion_tokens,
                    cost_usd=record.cost_usd,
                    latency_ms=record.latency_ms,
                    time_to_first_token_ms=record.time_to_first_token_ms,
                    occurred_at=now,
                ),
            )


async def sweep_anonymous(session: AsyncSession, *, hours: int, now: datetime | None = None) -> int:
    """Deletes anonymous conversations (messages cascade) idle for ``hours`` (Q5 (0005))."""
    cutoff = (now or datetime.now(UTC)) - timedelta(hours=hours)
    result = await session.execute(
        delete(Conversation).where(
            Conversation.user_id.is_(None),
            func.coalesce(Conversation.last_message_at, Conversation.created_at) < cutoff,
        )
    )
    await session.commit()
    return int(getattr(result, "rowcount", 0) or 0)
