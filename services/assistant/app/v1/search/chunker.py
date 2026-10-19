"""Typed chunks with an identity prefix and bounded sizes (§3.1.1.2, AST-D5).

* Every chunk starts with the product's identity line, so no chunk is ever
  anonymous ("Machine washable, 280g" of what?).
* Description text is packed sentence by sentence to ~``target`` tokens,
  never above ``max``, with ``overlap`` tokens carried into the next chunk.
* Short fields (attributes, variants, category) are one chunk each, never
  split or padded; an over-long one is cut to ``max``.
* At most ``max_chunks`` per product: description chunks are dropped from the
  end beyond it, and the plan says so (the caller logs it).

Token counts are approximate (:mod:`app.core.tokens`, AST-D33).
"""

import hashlib
import re
from dataclasses import dataclass

from app.core.tokens import count_tokens
from app.v1.search.enums import ChunkType
from app.v1.search.renderer import RenderedProduct

_SENTENCE = re.compile(r"(?<=[.!?])\s+")


@dataclass(frozen=True)
class Chunk:
    chunk_index: int
    chunk_type: ChunkType
    content: str
    token_count: int


@dataclass(frozen=True)
class ChunkPlan:
    chunks: list[Chunk]
    truncated: bool


def normalise(text: str) -> str:
    return " ".join(text.lower().split())


def content_hash(model: str, text: str) -> str:
    """``sha256(model, normalised text)`` (D6): trivially different text hashes alike."""
    return hashlib.sha256(f"{model}\x00{normalise(text)}".encode()).hexdigest()


def _cut(words: list[str], budget: int) -> list[str]:
    kept: list[str] = []
    for word in words:
        if count_tokens(" ".join([*kept, word])) > budget:
            break
        kept.append(word)
    return kept or words[:1]


def _tail(words: list[str], budget: int) -> list[str]:
    kept: list[str] = []
    for word in reversed(words):
        if count_tokens(" ".join([word, *kept])) > budget:
            break
        kept.insert(0, word)
    return kept


def split_description(text: str, *, target: int, hard_max: int, overlap: int) -> list[str]:
    """Packs sentences into windows of about ``target`` tokens (≤ ``hard_max``)
    with ``overlap`` tokens repeated at the start of each following window."""
    if not text.strip():
        return []
    pieces: list[list[str]] = []
    for sentence in _SENTENCE.split(text.strip()):
        words = sentence.split()
        while words:  # a sentence longer than the target is split by words
            head = _cut(words, target)
            pieces.append(head)
            words = words[len(head) :]

    windows: list[list[str]] = []
    current: list[str] = []
    for piece in pieces:
        if current and count_tokens(" ".join(current + piece)) > target:
            windows.append(current)
            carry = _tail(current, overlap)
            current = carry + piece
        else:
            current = current + piece
        if count_tokens(" ".join(current)) > hard_max:
            current = _cut(current, hard_max)
    if current:
        windows.append(current)
    return [" ".join(w) for w in windows]


def chunk_product(
    rendered: RenderedProduct,
    *,
    target: int = 256,
    max_tokens: int = 512,
    overlap: int = 80,
    max_chunks: int = 12,
) -> ChunkPlan:
    prefix = rendered.identity_prefix
    prefix_tokens = count_tokens(prefix) + 1
    body_max = max(16, max_tokens - prefix_tokens)
    body_target = max(16, min(target - prefix_tokens, body_max))
    body_overlap = max(1, min(overlap, body_target // 2))

    def bounded(body: str) -> str:
        if count_tokens(body) <= body_max:
            return body
        return " ".join(_cut(body.split(), body_max))

    others = [
        (ChunkType.ATTRIBUTES, rendered.attributes),
        (ChunkType.VARIANTS, rendered.variants),
        (ChunkType.CATEGORY, rendered.category),
    ]
    others = [(t, bounded(b)) for t, b in others if b]
    descriptions = split_description(
        rendered.description, target=body_target, hard_max=body_max, overlap=body_overlap
    )
    room = max(0, max_chunks - 1 - len(others))
    truncated = len(descriptions) > room
    descriptions = descriptions[:room]

    bodies: list[tuple[ChunkType, str]] = [(ChunkType.IDENTITY, bounded(rendered.identity))]
    bodies += [(ChunkType.DESCRIPTION, d) for d in descriptions]
    bodies += others
    chunks = []
    for index, (chunk_type, body) in enumerate(bodies[:max_chunks]):
        content = f"{prefix}\n{body}"
        chunks.append(Chunk(index, chunk_type, content, count_tokens(content)))
    return ChunkPlan(chunks=chunks, truncated=truncated)
