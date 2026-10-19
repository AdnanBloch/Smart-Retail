"""The customer graph (E1, E2, E6, AST-D14), compiled once with LangGraph.

    guard → load_memory → classify ─┬─ product_search → retrieve ──┐
                                    ├─ compare → compare ───────────┤→ hydrate → assemble_context
                                    ├─ follow_up → resolve_follow_up┘
                                    ├─ too_vague → clarify
                                    ├─ smalltalk → smalltalk
                                    └─ out_of_scope → refuse

The graph prepares the turn and ends with either an ``LLMRequest`` or a
templated (``canned``) answer. Generation itself — the streamed provider call,
cancellation and persistence — runs outside the graph in :mod:`.turn`, so a
client disconnect cancels the provider stream directly (AST-D38).
"""

import uuid
from typing import Any, cast

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph

from app.core.clients.llm import LLMError, LLMRequest
from app.core.errors import LLMUnavailable
from app.core.safety import sanitize_text
from app.v1.chat.agent.context import assemble, canned_refusal
from app.v1.chat.agent.intents import parse_classification, resolve_follow_up, rule_classify
from app.v1.chat.agent.state import AgentDeps, TurnState, Usage
from app.v1.chat.enums import Intent
from app.v1.search.enums import SearchMode
from app.v1.search.repo import Hit

RETRIEVE_LIMIT = 5


def deps_of(config: RunnableConfig) -> AgentDeps:
    return cast(AgentDeps, config["configurable"]["deps"])


async def guard(state: TurnState, config: RunnableConfig) -> dict[str, Any]:
    deps = deps_of(config)
    return {
        "message": sanitize_text(state["message"], max_chars=deps.settings.message_max_chars),
        "usage": state.get("usage") or Usage(),
    }


async def load_memory(state: TurnState, config: RunnableConfig) -> dict[str, Any]:
    window = deps_of(config).settings.memory_window_turns
    history = state.get("history") or []
    return {"memory": history[-2 * window :] if window else []}


async def classify(state: TurnState, config: RunnableConfig) -> dict[str, Any]:
    deps = deps_of(config)
    has_prior = bool(state.get("prior_sources"))
    rules = rule_classify(state["message"], has_prior=has_prior)
    request = LLMRequest(
        model=deps.settings.llm_classifier_model,
        system=deps.classifier.render(has_prior="yes" if has_prior else "no"),
        messages=[{"role": "user", "content": state["message"]}],
        max_tokens=deps.settings.llm_classifier_max_tokens,
        purpose="classify",
        hints={"classification": dict(rules)},
    )
    usage = state["usage"]
    try:
        result = await deps.provider.complete(request)
    except (LLMUnavailable, LLMError):
        classification = rules  # routing never fails on the provider
    else:
        usage.add(result.model, result.input_tokens, result.output_tokens)
        classification = parse_classification(result.text, rules, has_prior=has_prior)
    return {"classification": classification, "intent": classification["intent"], "usage": usage}


def route(state: TurnState) -> str:
    return {
        Intent.PRODUCT_SEARCH: "retrieve",
        Intent.COMPARE: "compare",
        Intent.FOLLOW_UP: "resolve_follow_up",
        Intent.TOO_VAGUE: "clarify",
        Intent.SMALLTALK: "smalltalk",
        Intent.OUT_OF_SCOPE: "refuse",
    }.get(Intent.parse(state["intent"], default=Intent.PRODUCT_SEARCH), "retrieve")


async def retrieve(state: TurnState, config: RunnableConfig) -> dict[str, Any]:
    deps = deps_of(config)
    raw = await deps.search.repo.search(
        state["message"], state["filters"], SearchMode.HYBRID, RETRIEVE_LIMIT
    )
    return {"candidates": raw.hits, "degraded": raw.degraded, "retrieved": True}


async def compare(state: TurnState, config: RunnableConfig) -> dict[str, Any]:
    """Retrieve once per term: the best product per compared term (E6)."""
    deps = deps_of(config)
    terms = state["classification"].get("compare_terms") or []
    picked: dict[Any, Hit] = {}
    degraded = None
    for term in terms[:3]:
        raw = await deps.search.repo.search(term, state["filters"], SearchMode.HYBRID, 3)
        degraded = degraded or raw.degraded
        for hit in raw.hits:
            if hit.product_id not in picked:
                picked[hit.product_id] = hit
                break
    if len(picked) < 2:
        raw = await deps.search.repo.search(
            state["message"], state["filters"], SearchMode.HYBRID, RETRIEVE_LIMIT
        )
        degraded = degraded or raw.degraded
        for hit in raw.hits:
            picked.setdefault(hit.product_id, hit)
            if len(picked) >= 3:
                break
    return {"candidates": list(picked.values()), "degraded": degraded, "retrieved": True}


async def resolve_follow_up_node(state: TurnState, config: RunnableConfig) -> dict[str, Any]:
    """Re-hydrates the prior result set, then picks what the follow-up refers to."""
    prior = state.get("prior_sources") or []
    candidates = [
        Hit(
            product_id=uuid.UUID(s["id"]),
            merchant_id=uuid.UUID(int=0),
            category_id=None,
            name=s.get("name") or "",
            score=float(s.get("score") or 0.0),
            chunk_id=uuid.UUID(s["chunk_id"]) if s.get("chunk_id") else uuid.UUID(int=0),
            chunk_type="prior",
            content=s.get("matched_text") or "",
            match_reason="follow_up",
        )
        for s in prior
    ]
    return {"candidates": candidates, "retrieved": True}


async def hydrate(state: TurnState, config: RunnableConfig) -> dict[str, Any]:
    deps = deps_of(config)
    results, _ = await deps.search.hydrate(state.get("candidates") or [])
    sources = [r.as_source() for r in results]
    if state.get("intent") == Intent.FOLLOW_UP:
        sources = resolve_follow_up(state["message"], sources)
    return {"sources": sources}


async def assemble_context(state: TurnState, config: RunnableConfig) -> dict[str, Any]:
    deps = deps_of(config)
    assembled = assemble(
        deps.chat_system.body,
        state["message"],
        state.get("memory") or [],
        state.get("sources") or [],
        budget=deps.settings.llm_context_token_budget,
    )
    request = LLMRequest(
        model=deps.settings.llm_model,
        system=assembled.system,
        messages=assembled.messages,
        max_tokens=deps.settings.llm_max_tokens,
        purpose="answer",
        hints={
            "intent": state.get("intent"),
            "products": [
                {
                    "id": p["id"],
                    "name": p["name"],
                    "price": p.get("price"),
                    "currency": p.get("currency"),
                    "in_stock": p.get("in_stock"),
                    "snippet": (p.get("matched_text") or "").split("\n", 1)[-1][:160],
                }
                for p in assembled.products
            ],
            "categories": deps.categories,
        },
    )
    return {"request": request, "sources": assembled.products}


async def clarify(state: TurnState, config: RunnableConfig) -> dict[str, Any]:
    deps = deps_of(config)
    assembled = assemble(
        deps.chat_system.body
        + "\n\nThe request is too vague to search. Ask at most two short questions"
        " (budget, purpose or recipient) and offer a few categories to choose from.",
        state["message"],
        state.get("memory") or [],
        [],
        budget=deps.settings.llm_context_token_budget,
    )
    request = LLMRequest(
        model=deps.settings.llm_model,
        system=assembled.system,
        messages=assembled.messages,
        max_tokens=min(300, deps.settings.llm_max_tokens),
        purpose="clarify",
        hints={"categories": deps.categories},
    )
    return {"request": request, "sources": []}


async def smalltalk(state: TurnState, config: RunnableConfig) -> dict[str, Any]:
    deps = deps_of(config)
    request = LLMRequest(
        model=deps.settings.llm_model,
        system=deps.chat_system.body,
        messages=[{"role": "user", "content": state["message"]}],
        max_tokens=min(200, deps.settings.llm_max_tokens),
        purpose="smalltalk",
    )
    return {"request": request, "sources": []}


async def refuse(state: TurnState, config: RunnableConfig) -> dict[str, Any]:
    return {"request": None, "canned": canned_refusal(), "sources": []}


def build_graph() -> Any:
    builder = StateGraph(TurnState)
    builder.add_node("guard", guard)
    builder.add_node("load_memory", load_memory)
    builder.add_node("classify", classify)
    builder.add_node("retrieve", retrieve)
    builder.add_node("compare", compare)
    builder.add_node("resolve_follow_up", resolve_follow_up_node)
    builder.add_node("hydrate", hydrate)
    builder.add_node("assemble_context", assemble_context)
    builder.add_node("clarify", clarify)
    builder.add_node("smalltalk", smalltalk)
    builder.add_node("refuse", refuse)
    builder.add_edge(START, "guard")
    builder.add_edge("guard", "load_memory")
    builder.add_edge("load_memory", "classify")
    builder.add_conditional_edges(
        "classify",
        route,
        ["retrieve", "compare", "resolve_follow_up", "clarify", "smalltalk", "refuse"],
    )
    for node in ("retrieve", "compare", "resolve_follow_up"):
        builder.add_edge(node, "hydrate")
    builder.add_edge("hydrate", "assemble_context")
    for node in ("assemble_context", "clarify", "smalltalk", "refuse"):
        builder.add_edge(node, END)
    return builder.compile()
