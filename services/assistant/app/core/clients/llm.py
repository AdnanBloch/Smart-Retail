"""The LLM provider interface and its implementations (E11, AST-D12, AST-D30, AST-D32).

* :class:`AnthropicProvider` — Claude through the official ``anthropic`` SDK
  (``AsyncAnthropic``): ``messages.stream`` for streamed answers,
  ``messages.create`` otherwise. Only long-established request parameters are
  used (``model``, ``max_tokens``, ``system``, ``messages``).
* :class:`ScriptedProvider` — the default and the test double: deterministic,
  key-less, free. It renders templated answers from structured ``hints`` the
  caller supplies (the same facts the real model receives in the prompt),
  cites ``[product:<id>]``, streams word by word and reports approximate token
  counts (:mod:`app.core.tokens`) at zero cost.

Used by chat/ and generations/, so it lives in ``core/clients`` (AST-D29).
"""

import asyncio
import json
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from app.core.config import Settings
from app.core.enums import FinishReason
from app.core.errors import LLMUnavailable
from app.core.tokens import count_tokens

Purpose = Literal["classify", "answer", "clarify", "smalltalk", "generate"]


@dataclass(frozen=True)
class LLMRequest:
    model: str
    system: str
    messages: list[dict[str, str]]
    max_tokens: int
    purpose: Purpose
    # Structured facts for the scripted double; the real provider ignores them.
    hints: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LLMResult:
    text: str
    input_tokens: int
    output_tokens: int
    model: str
    finish_reason: FinishReason


class LLMError(Exception):
    """A request the provider rejected (4xx other than 429): not retryable."""


class LLMProvider(Protocol):
    name: str

    async def complete(self, request: LLMRequest) -> LLMResult: ...

    def stream(self, request: LLMRequest) -> AsyncIterator[str | LLMResult]:
        """Yields text deltas, then exactly one final :class:`LLMResult`."""
        ...


# --- Anthropic ---------------------------------------------------------------

STOP_REASONS = {
    "end_turn": FinishReason.STOP,
    "stop_sequence": FinishReason.STOP,
    "max_tokens": FinishReason.LENGTH,
    "refusal": FinishReason.REFUSED,
}


def map_stop_reason(stop_reason: str | None) -> FinishReason:
    return STOP_REASONS.get(stop_reason or "end_turn", FinishReason.STOP)


class AnthropicProvider:
    name = "anthropic"

    def __init__(self, api_key: str, *, timeout: float, client: Any = None) -> None:
        if client is None:  # pragma: no cover - needs a real key
            import anthropic

            client = anthropic.AsyncAnthropic(api_key=api_key, timeout=timeout, max_retries=2)
        self.client = client

    @staticmethod
    def _translate(exc: Exception) -> Exception:
        import anthropic

        if isinstance(exc, anthropic.APIConnectionError):  # includes APITimeoutError
            return LLMUnavailable("the model provider did not answer")
        if isinstance(exc, anthropic.APIStatusError):
            if exc.status_code == 429 or exc.status_code >= 500:
                return LLMUnavailable(f"the model provider answered {exc.status_code}")
            return LLMError(f"the model provider rejected the request ({exc.status_code})")
        return exc

    def _params(self, request: LLMRequest) -> dict[str, Any]:
        return {
            "model": request.model,
            "max_tokens": request.max_tokens,
            "system": request.system,
            "messages": request.messages,
        }

    @staticmethod
    def _result(message: Any, model: str) -> LLMResult:
        text = "".join(b.text for b in message.content if getattr(b, "type", None) == "text")
        return LLMResult(
            text=text,
            input_tokens=int(message.usage.input_tokens),
            output_tokens=int(message.usage.output_tokens),
            model=getattr(message, "model", None) or model,
            finish_reason=map_stop_reason(message.stop_reason),
        )

    async def complete(self, request: LLMRequest) -> LLMResult:
        try:
            message = await self.client.messages.create(**self._params(request))
        except Exception as exc:
            raise self._translate(exc) from exc
        return self._result(message, request.model)

    async def stream(self, request: LLMRequest) -> AsyncIterator[str | LLMResult]:
        try:
            async with self.client.messages.stream(**self._params(request)) as stream:
                async for text in stream.text_stream:
                    yield text
                message = await stream.get_final_message()
        except Exception as exc:
            translated = self._translate(exc)
            if translated is exc:
                raise
            raise translated from exc
        yield self._result(message, request.model)


# --- Scripted ----------------------------------------------------------------

_WORDS = re.compile(r"\S+\s*")


def _price(product: dict[str, Any]) -> str:
    if product.get("price") is None:
        return ""
    return f" at {product['price']} {product.get('currency') or ''}".rstrip()


def _stock(product: dict[str, Any]) -> str:
    if product.get("in_stock") is None:
        return " (availability unknown)"
    return " (in stock)" if product["in_stock"] else " (out of stock)"


def scripted_text(request: LLMRequest) -> str:
    """The deterministic answer for a request, from its hints."""
    hints = request.hints
    if request.purpose == "classify":
        return json.dumps(hints.get("classification", {"intent": "product_search"}))
    if request.purpose == "smalltalk":
        return (
            "Hello! I can help you find products, compare them, or check prices and "
            "availability. What are you shopping for?"
        )
    if request.purpose == "clarify":
        options = hints.get("categories") or ["electronics", "clothing", "home", "sports"]
        return (
            "Could you tell me a little more? What is your budget, and what will it be used "
            f"for? You could also pick a category: {', '.join(options)}."
        )
    if request.purpose == "generate":
        return _scripted_generation(hints)
    products: list[dict[str, Any]] = hints.get("products") or []
    if not products:
        options = hints.get("categories") or []
        tail = f" You could try browsing {', '.join(options)}." if options else ""
        return "I don't have any products matching that in the catalog right now." + tail
    intent = hints.get("intent")
    if intent == "compare" and len(products) >= 2:
        lines = [
            f"{p['name']} [product:{p['id']}]{_price(p)}{_stock(p)}: {p.get('snippet', '')}"
            for p in products
        ]
        return "Here is how they compare. " + " ".join(lines)
    lead = "Based on the earlier results, " if intent == "follow_up" else "Here is what I found: "
    parts = [
        f"{p['name']} [product:{p['id']}]{_price(p)}{_stock(p)} - {p.get('snippet', '')}".strip()
        for p in products[:3]
    ]
    return lead + " ".join(parts)


def _scripted_generation(hints: dict[str, Any]) -> str:
    product = hints.get("product") or {}
    name = product.get("name", "This product")
    tone = hints.get("tone") or "friendly"
    content_type = hints.get("content_type", "description")
    facts = product.get("summary") or ""
    keywords = ", ".join(hints.get("keywords") or [])
    if content_type == "seo_title":
        text = f"{name} | {product.get('category') or 'Shop'} - Buy Online"
    elif content_type == "faq":
        text = (
            f"Q: What is {name}? A: {facts or name}. "
            f"Q: Who is it for? A: Anyone looking for a reliable {name}."
        )
    elif content_type == "promotional":
        text = f"Discover {name} - a {tone} pick you will love. {facts}"
    elif content_type == "benefits_summary":
        text = f"Why {name}: {facts or 'quality you can rely on'}."
    elif content_type == "cross_sell":
        siblings = hints.get("siblings") or []
        pairs = ", ".join(siblings) if siblings else "other products from this store"
        text = f"Customers who choose {name} also like {pairs}."
    else:
        text = f"{name}: {facts}".strip()
    if keywords:
        text += f" Keywords: {keywords}."
    max_length = hints.get("max_length")
    return text[:max_length] if max_length else text


class ScriptedProvider:
    """Deterministic double. ``token_delay`` slows streaming (disconnect tests);
    ``fail_before`` / ``fail_after_tokens`` inject provider failures."""

    name = "scripted"

    def __init__(
        self,
        *,
        token_delay: float = 0.0,
        fail_before: frozenset[str] = frozenset(),
        fail_after_tokens: int | None = None,
    ) -> None:
        self.token_delay = token_delay
        self.fail_before = fail_before
        self.fail_after_tokens = fail_after_tokens
        self.requests: list[LLMRequest] = []
        self.cancelled = 0
        self.completed = 0

    def _usage(self, request: LLMRequest, text: str) -> tuple[str, int, int, FinishReason]:
        prompt = request.system + " ".join(m["content"] for m in request.messages)
        tokens = _WORDS.findall(text)
        finish = FinishReason.STOP
        if count_tokens(text) > request.max_tokens:
            kept: list[str] = []
            for word in tokens:
                if count_tokens("".join([*kept, word])) > request.max_tokens:
                    break
                kept.append(word)
            text, finish = "".join(kept), FinishReason.LENGTH
        return text, count_tokens(prompt), count_tokens(text), finish

    async def complete(self, request: LLMRequest) -> LLMResult:
        self.requests.append(request)
        if request.purpose in self.fail_before:
            raise LLMUnavailable("scripted failure")
        text, prompt_tokens, completion_tokens, finish = self._usage(
            request, scripted_text(request)
        )
        self.completed += 1
        return LLMResult(text, prompt_tokens, completion_tokens, self.name, finish)

    async def stream(self, request: LLMRequest) -> AsyncIterator[str | LLMResult]:
        self.requests.append(request)
        if request.purpose in self.fail_before:
            raise LLMUnavailable("scripted failure")
        text, prompt_tokens, completion_tokens, finish = self._usage(
            request, scripted_text(request)
        )
        try:
            for i, word in enumerate(_WORDS.findall(text)):
                if self.fail_after_tokens is not None and i >= self.fail_after_tokens:
                    raise LLMUnavailable("scripted mid-stream failure")
                if self.token_delay:
                    await asyncio.sleep(self.token_delay)
                yield word
        except (GeneratorExit, asyncio.CancelledError):
            # The consumer stopped reading: the provider stream is cancelled.
            self.cancelled += 1
            raise
        self.completed += 1
        yield LLMResult(text, prompt_tokens, completion_tokens, self.name, finish)


def build_llm(settings: Settings) -> LLMProvider:
    if settings.llm_provider == "anthropic":  # pragma: no cover - needs a real key
        assert settings.anthropic_api_key is not None  # Settings refuses otherwise
        return AnthropicProvider(
            settings.anthropic_api_key.get_secret_value(),
            timeout=settings.llm_request_timeout_seconds,
        )
    return ScriptedProvider()
