"""``/api/v1/assistant/chat*``, ``/conversations*``, ``/messages/{id}/feedback`` (§3.1.2.1).

Everything that can fail before a stream opens fails as a normal problem
response — validation (422), the input guard (422), conversation ownership
(404), the daily budget (429 + ``Retry-After``), the stream cap (503 +
``Retry-After``). Only then does ``/chat`` return ``200 text/event-stream``.
"""

import secrets
import uuid
from typing import Any

from fastapi import APIRouter, Depends, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.db import get_db
from app.core.dependencies import (
    Principal,
    caller_ip_scope,
    get_settings,
    optional_customer,
    require_customer,
)
from app.core.errors import MessageBlocked, StreamCapReached
from app.core.safety import looks_like_injection
from app.core.sse import SSE_HEADERS, Emit, SseEvent, pump
from app.v1.chat.agent.turn import ChatTurn, TurnInput
from app.v1.chat.enums import MessageRole
from app.v1.chat.repo import ConversationRepo
from app.v1.chat.schemas import (
    ChatIn,
    ChatSyncOut,
    ConversationDetail,
    ConversationOut,
    FeedbackIn,
    MessageOut,
)
from app.v1.generations.budget_repo import BudgetRepo
from app.v1.search.repo import SearchFilters
from smartretail_common.context import get_request_id
from smartretail_common.errors import PROBLEM_CONTENT_TYPE, Unprocessable
from smartretail_common.pagination import Page, PageParams, page_params

router = APIRouter()


async def prepare_turn(
    request: Request,
    body: ChatIn,
    principal: Principal | None,
    session: AsyncSession,
    settings: Settings,
    instance: str,
) -> TurnInput:
    message = body.message.strip()
    if not message or len(message) > settings.message_max_chars:
        raise Unprocessable(
            violations=[
                {
                    "field": "message",
                    "message": f"must be 1 to {settings.message_max_chars} characters",
                    "code": "length",
                }
            ]
        )
    if looks_like_injection(message):
        raise MessageBlocked("The message looks like an attempt to change the assistant's rules")
    user_id = principal.subject if principal else None
    session_id = None if principal else (body.session_id or secrets.token_urlsafe(24))
    history: list[dict[str, str]] = []
    prior_sources: list[dict[str, Any]] = []
    repo = ConversationRepo(session)
    if body.conversation_id is not None:
        conversation = await repo.owned(
            body.conversation_id, user_id=user_id, session_id=session_id
        )
        conversation_id = conversation.id
        rows = await repo.history(conversation_id, turns=settings.memory_window_turns)
        history = [{"role": str(m.role), "content": m.content} for m in rows]
        last_assistant = [m for m in rows if m.role == MessageRole.ASSISTANT]
        if last_assistant:
            prior_sources = list(last_assistant[-1].sources or [])
    else:
        conversation_id = uuid.uuid4()
    scope = f"user:{user_id}" if user_id else caller_ip_scope(request)
    await BudgetRepo(session, settings).check(scope)
    await session.rollback()  # read-only; release the connection before streaming
    return TurnInput(
        message=message,
        conversation_id=conversation_id,
        user_id=user_id,
        session_id=session_id,
        filters=body.filters.to_filters() if body.filters else SearchFilters(),
        history=history,
        prior_sources=prior_sources,
        budget_scope=scope,
        correlation_id=get_request_id(),
        instance=instance,
    )


@router.post("/chat", response_model=None)
async def chat(
    request: Request,
    body: ChatIn,
    principal: Principal | None = Depends(optional_customer),
    session: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> StreamingResponse:
    turn = await prepare_turn(request, body, principal, session, settings, request.url.path)
    state = request.app.state
    if state.active_streams >= settings.max_concurrent_streams:
        raise StreamCapReached(retry_after=1)
    state.active_streams += 1
    runner: ChatTurn = state.chat_turn

    async def produce(emit: Emit) -> None:
        await runner.run(turn, emit)

    def release() -> None:
        state.active_streams -= 1

    return StreamingResponse(
        pump(
            produce,
            keepalive_seconds=settings.sse_keepalive_seconds,
            is_disconnected=request.is_disconnected,
            on_close=release,
        ),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


@router.post("/chat/sync", response_model=ChatSyncOut)
async def chat_sync(
    request: Request,
    body: ChatIn,
    principal: Principal | None = Depends(optional_customer),
    session: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> Any:
    turn = await prepare_turn(request, body, principal, session, settings, request.url.path)
    events: list[SseEvent] = []

    async def collect(event: SseEvent) -> None:
        events.append(event)

    runner: ChatTurn = request.app.state.chat_turn
    outcome = await runner.run(turn, collect)
    error = next((e for e in events if e.event == "error"), None)
    if error is not None and not outcome.persisted:
        return JSONResponse(
            error.data, status_code=int(error.data["status"]), media_type=PROBLEM_CONTENT_TYPE
        )
    return ChatSyncOut(
        conversation_id=turn.conversation_id,
        message_id=outcome.message_id,
        session_id=turn.session_id,
        content=outcome.content,
        sources=outcome.sources,
        usage=outcome.usage,
        finish_reason=str(outcome.finish_reason),
    )


@router.get("/conversations", response_model=Page[ConversationOut])
async def list_conversations(
    params: PageParams = Depends(page_params),
    session: AsyncSession = Depends(get_db),
    principal: Principal = Depends(require_customer),
) -> Page[ConversationOut]:
    page = await ConversationRepo(session).list_for_user(principal.subject, params)
    return page.to_page(ConversationOut.model_validate)


@router.get("/conversations/{conversation_id}", response_model=ConversationDetail)
async def get_conversation(
    conversation_id: uuid.UUID,
    session: AsyncSession = Depends(get_db),
    principal: Principal = Depends(require_customer),
) -> ConversationDetail:
    repo = ConversationRepo(session)
    conversation = await repo.owned(conversation_id, user_id=principal.subject, session_id=None)
    messages = await repo.messages(conversation.id)
    return ConversationDetail(
        **ConversationOut.model_validate(conversation).model_dump(),
        messages=[
            MessageOut(
                id=m.id,
                role=str(m.role),
                content=m.content,
                intent=str(m.intent) if m.intent else None,
                sources=m.sources or [],
                prompt_tokens=m.prompt_tokens,
                completion_tokens=m.completion_tokens,
                model=m.model,
                cost_usd=m.cost_usd,
                finish_reason=str(m.finish_reason) if m.finish_reason else None,
                feedback_helpful=m.feedback_helpful,
                created_at=m.created_at,
            )
            for m in messages
        ],
    )


@router.delete("/conversations/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_conversation(
    conversation_id: uuid.UUID,
    session: AsyncSession = Depends(get_db),
    principal: Principal = Depends(require_customer),
) -> Response:
    repo = ConversationRepo(session)
    conversation = await repo.owned(conversation_id, user_id=principal.subject, session_id=None)
    await repo.delete(conversation)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/messages/{message_id}/feedback", status_code=status.HTTP_204_NO_CONTENT)
async def feedback(
    message_id: uuid.UUID,
    body: FeedbackIn,
    session: AsyncSession = Depends(get_db),
    principal: Principal = Depends(require_customer),
) -> Response:
    await ConversationRepo(session).feedback(
        message_id, principal.subject, body.helpful, body.comment
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
