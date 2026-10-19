"""The embedding provider interface and its implementations (D10, AST-D31).

* :class:`HashingEmbeddings` (``hashing-v1``, the default): deterministic,
  local, free. Words (lower-cased, lightly stemmed, stop words dropped),
  word bigrams and character trigrams are hashed into 1024 signed buckets and
  L2-normalised, so texts sharing vocabulary score higher under cosine. It is
  a lexical stand-in that keeps the whole pipeline real without a key — not a
  semantic model.
* :class:`VoyageEmbeddings` (``voyage-3.5``, 1024 dims) over HTTP. Anthropic
  has no embeddings endpoint; Voyage is the provider Anthropic recommends.
  ``input_type`` distinguishes documents from queries. Timeouts, connection
  errors, 429 and 5xx raise :class:`EmbeddingUnavailable` (a
  ``DependencyUnavailable``, so the indexer waits and search degrades).

Only search/ embeds, so the client lives under ``search/clients`` (AST-D29).
"""

import hashlib
import itertools
import re
from typing import Literal, Protocol

import httpx
import numpy as np

from app.core.config import Settings
from app.core.errors import EmbeddingUnavailable
from app.core.tokens import count_tokens

InputType = Literal["document", "query"]

_WORD = re.compile(r"[a-z0-9]+")
_STOP = frozenset(
    "a an and are as at be but by for from has have i in is it its me my of on or our "
    "show that the this to was we what which with you your".split()
)


class EmbeddingError(Exception):
    """The provider rejected the request (4xx other than 429): not retryable."""


class EmbeddingProvider(Protocol):
    model: str
    dimension: int

    async def embed(self, texts: list[str], *, input_type: InputType) -> list[list[float]]: ...


def _stem(word: str) -> str:
    for suffix in ("ing", "ies", "es", "ed", "s"):
        if len(word) > len(suffix) + 2 and word.endswith(suffix):
            return word[: -len(suffix)] + ("y" if suffix == "ies" else "")
    return word


def _bucket(feature: str, dimension: int) -> tuple[int, float]:
    digest = int.from_bytes(hashlib.blake2b(feature.encode(), digest_size=8).digest(), "big")
    return digest % dimension, 1.0 if (digest >> 63) & 1 else -1.0


class HashingEmbeddings:
    model = "hashing-v1"

    def __init__(self, dimension: int = 1024) -> None:
        self.dimension = dimension

    def vector(self, text: str) -> list[float]:
        words = [_stem(w) for w in _WORD.findall(text.lower()) if w not in _STOP]
        features: list[tuple[str, float]] = []
        for word in words:
            features.append(("w:" + word, 1.0))
            padded = f"#{word}#"
            features += [("t:" + padded[i : i + 3], 0.3) for i in range(len(padded) - 2)]
        features += [("b:" + a + "_" + b, 0.5) for a, b in itertools.pairwise(words)]
        if not features:
            features = [("empty", 1.0)]
        vec = np.zeros(self.dimension, dtype=np.float64)
        for feature, weight in features:
            index, sign = _bucket(feature, self.dimension)
            vec[index] += sign * weight
        norm = float(np.linalg.norm(vec))
        if norm == 0.0:  # every feature cancelled out
            index, _ = _bucket("empty", self.dimension)
            vec[index], norm = 1.0, 1.0
        return [float(x) for x in vec / norm]

    async def embed(self, texts: list[str], *, input_type: InputType) -> list[list[float]]:
        return [self.vector(t) for t in texts]


class VoyageEmbeddings:
    def __init__(
        self,
        api_key: str,
        http: httpx.AsyncClient,
        *,
        base_url: str = "https://api.voyageai.com/v1",
        model: str = "voyage-3.5",
        dimension: int = 1024,
        timeout: float = 10.0,
    ) -> None:
        self.api_key = api_key
        self.http = http
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.dimension = dimension
        self.timeout = timeout
        self.tokens_used = 0

    async def embed(self, texts: list[str], *, input_type: InputType) -> list[list[float]]:
        if not texts:
            return []
        body = {
            "input": texts,
            "model": self.model,
            "input_type": input_type,
            "output_dimension": self.dimension,
        }
        try:
            response = await self.http.post(
                f"{self.base_url}/embeddings",
                json=body,
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=self.timeout,
            )
        except httpx.HTTPError as exc:
            raise EmbeddingUnavailable(f"embedding provider: {type(exc).__name__}") from exc
        if response.status_code == 429 or response.status_code >= 500:
            raise EmbeddingUnavailable(f"embedding provider answered {response.status_code}")
        if response.status_code != 200:
            raise EmbeddingError(f"embedding provider answered {response.status_code}")
        data = response.json()
        rows = sorted(data["data"], key=lambda row: row["index"])
        self.tokens_used += int(data.get("usage", {}).get("total_tokens", 0)) or sum(
            count_tokens(t) for t in texts
        )
        vectors = [list(map(float, row["embedding"])) for row in rows]
        if len(vectors) != len(texts) or any(len(v) != self.dimension for v in vectors):
            raise EmbeddingError("embedding provider returned an unexpected shape")
        return vectors


def build_embeddings(settings: Settings, http: httpx.AsyncClient) -> EmbeddingProvider:
    if settings.embedding_provider == "voyage":
        assert settings.voyage_api_key is not None  # Settings refuses otherwise
        return VoyageEmbeddings(
            settings.voyage_api_key.get_secret_value(),
            http,
            base_url=settings.voyage_base_url,
            model=settings.active_embedding_model,
            dimension=settings.embedding_dimension,
            timeout=settings.voyage_timeout_seconds,
        )
    return HashingEmbeddings(settings.embedding_dimension)
