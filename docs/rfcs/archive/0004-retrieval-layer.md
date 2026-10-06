# RFC 0004 — Retrieval Layer

> **SUPERSEDED — archived Sep 29, 2026.** The RFC set was reorganised from milestone-based to architecture- and service-based. This document is kept for history only and is no longer maintained. Its content now lives in: [0009-assistant-service](../0009-assistant-service.md), [0001-platform-and-shared-infrastructure](../0001-platform-and-shared-infrastructure.md). See [docs/README.md](../../README.md).

| | |
| --- | --- |
| **Owner** | @muhammad.adnan |
| **Collaborators** | SarwanAhmed |
| **Status** | **DRAFT** · IN-REVIEW · DONE |
| **Created** | Sep 22, 2026 |
| **Related Epic or story** | Milestone 5 — Retrieval Layer ([milestones.md](../../milestones.md)), Week 4 ([execution-guidelines.md](../../execution-guidelines.md)) |
| **Builds on** | [RFC 0000](0000-system-architecture.md) · [RFC 0003](0003-events-and-observability.md) |

---

### Milestone source text (verbatim)

> **Week 4 — Data Preparation + Retrieval**
>
> - Content chunking
> - Embedding generation
> - Vector database integration
> - Retrieval pipeline
>
> **Expected Outcome**
> - Content indexed for semantic search
> - Relevant data retrieval working

Requirements are in [Part B](../../part-b.md) §1 (Content Publishing Workflow) and
§2A (Contextual Product Q&A, retrieval half). Domain wording is mapped per
[RFC 0000](0000-system-architecture.md) §11 — *course content* reads as
*product catalog content*.

---

## Contents

1. [Title](#1-title)
2. [Overview](#2-overview)
   - 2.1. [Goals](#21-goals)
   - 2.2. [Non-goals](#22-non-goals)
3. [Solution Overview](#3-solution-overview)
   - 3.1. [Specification](#31-specification)
     - 3.1.1. [Architectural changes](#311-architectural-changes)
     - 3.1.2. [API changes](#312-api-changes)
     - 3.1.3. [Data/schema changes](#313-dataschema-changes)
     - 3.1.4. [Breaking changes](#314-breaking-changes)
     - 3.1.5. [Action Plan](#315-action-plan)
   - 3.2. [Rationale](#32-rationale)
   - 3.3. [Tradeoffs](#33-tradeoffs)
     - 3.3.1. [Challenges and Open Questions](#331-challenges-and-open-questions)
4. [Dependencies/blockers/known issues](#4-dependenciesblockersknown-issues)
5. [Alternate solutions](#5-alternate-solutions)
6. [Production readiness](#6-production-readiness)
   - 6.1. [Scalability and Fault tolerance](#61-scalability-and-fault-tolerance)
   - 6.2. [Security considerations](#62-security-considerations)
   - 6.3. [Deployment strategy](#63-deployment-strategy)
7. [Test Plan](#7-test-plan)
8. [Impact Analysis](#8-impact-analysis)
9. [Useful Links/References](#9-useful-linksreferences)
10. [Related tickets/issues](#10-related-ticketsissues)
11. [Notes](#11-notes)
12. [Comments](#12-comments)

---

## 1. Title

**The retrieval layer — the `assistant` service, event-driven product indexing, chunking and embedding, vector storage in pgvector, and a hybrid semantic search pipeline.**

This is the first half of Part B. It builds everything the assistant needs to find relevant products, and stops short of generating an answer with them. The deliverable is a retrieval pipeline good enough that Milestone 6's answer quality is limited by the model, not by what it was given.

---

## 2. Overview

[Part B](../../part-b.md) opens by naming the gap: product interactions generate rich data that is not used, search is keyword-based with no semantic understanding, and customers struggle to find the right product. The queries it wants to support — *"which phone is best under my budget"*, *"recommend gifts for kids age 10"*, *"show beginner fitness products"* — share a property: none of them contain the words that appear in the product records that answer them.

Fixing that is a retrieval problem, and retrieval is where RAG systems actually succeed or fail. A weak retriever cannot be rescued by a strong model; it just produces fluent answers about the wrong products. So Milestone 5 is treated as a system with its own quality bar and its own evaluation set, rather than as plumbing on the way to Milestone 6.

This RFC adds the `assistant` service (:8007) with its retrieval half, an `indexer-worker` that consumes `catalog.product.published`, chunking and embedding, pgvector as the store, and a hybrid search that combines dense vectors with the keyword index `catalog` already has.

The pipeline is event-driven by construction: [RFC 0003](0003-events-and-observability.md) already publishes every product lifecycle event, and `products.search_document` is already persisted, so indexing is a consumer and a backfill rather than a new coupling into `catalog`.

### 2.1. Goals

| # | Goal | How it is verified |
| --- | --- | --- |
| G1 | Publishing a product indexes it automatically, with no manual step | E2E: publish → within seconds the product is semantically searchable |
| G2 | Updating or unpublishing a product updates or removes its vectors | Tests per event type; an unpublished product never appears in results |
| G3 | Product text is chunked with a defensible strategy that keeps each chunk self-contained | Unit tests on boundaries, sizes, and overlap; every chunk carries its product's identity |
| G4 | Embeddings are generated in batches, cached, and never recomputed for unchanged text | Re-running the indexer on unchanged products makes zero embedding API calls |
| G5 | Semantic search returns relevant results for natural-language queries | A 30-query evaluation set with labelled relevant products; **Recall@5 ≥ 0.8** |
| G6 | Hybrid search outperforms either dense or keyword search alone on that set | Measured: all three reported, hybrid highest |
| G7 | Retrieval respects visibility and filters — only published products, with price and category filters applied | Tests: unpublished, archived, and suspended-merchant products never surface |
| G8 | Products published before this milestone are backfilled | `make reindex` indexes the whole catalog from `search_document` |
| G9 | The pipeline is observable — indexing lag, embedding cost, search latency, and result counts | Grafana panels with live data |
| G10 | Retrieval latency is acceptable for an interactive path | p95 under 300ms for search excluding the query-embedding call; reported separately |

### 2.2. Non-goals

| Excluded | Why / when |
| --- | --- |
| The conversational assistant, LLM answer generation, streaming | Milestone 6. This milestone returns ranked products, not prose |
| Merchant content generation | Milestone 6 |
| Conversation history and memory | Milestone 6. The `conversations` tables are created here so M6 is additive, but nothing writes them |
| A reranking model (cross-encoder) | Deferred. Hybrid fusion is measured first; a reranker is only justified if it is what the evaluation shows is missing |
| Fine-tuning or training an embedding model | Far out of scope |
| Multimodal / image embeddings | Part B asks about text. Product images are not embedded |
| Personalised or behaviour-based recommendations | Part B's "cross-sell suggestions" is a generation task in M6, not a recommender |
| Multilingual retrieval | Corpus and queries are English |
| Replacing `catalog`'s keyword search | Hybrid *uses* it. The existing filter-and-sort browse API is unchanged |
| Real-time index updates measured in milliseconds | Seconds is the target; it is an eventually-consistent index by design |

---

## 3. Solution Overview

### 3.1. Specification

#### 3.1.1. Architectural changes

**What joins the topology.**

```
   catalog ──▶ outbox ──▶ relay ──▶ Kafka
                                     │ catalog.product.published
                                     │ catalog.product.updated
                                     │ catalog.product.unpublished
                                     ▼
                         ┌────────────────────────┐
                         │    indexer-worker      │ ← new
                         │  1 fetch search_document│
                         │  2 chunk               │
                         │  3 hash → cache lookup │
                         │  4 embed (batched)     │
                         │  5 upsert vectors      │
                         └───────────┬────────────┘
                                     │
                      ┌──────────────▼───────────────┐
                      │  assistant_db (Postgres)     │ ← new
                      │  + pgvector extension        │
                      │    product_chunks            │
                      │    embedding_cache           │
                      │    index_state               │
                      │    conversations (M6, empty) │
                      └──────────────▲───────────────┘
                                     │
   client ──▶ gateway ──▶ ┌──────────┴───────────┐
                          │  assistant  (:8007)  │ ← new
                          │  retrieval half only │
                          │  GET/POST /search/*  │
                          └──────────┬───────────┘
                                     │ embed query
                                     ▼
                          ┌──────────────────────┐
                          │  embedding provider  │
                          │  (OpenAI / local)    │
                          └──────────────────────┘
```

**New architectural decisions.**

- **D1 — pgvector, not a separate vector database.** The corpus is a few thousand products, a few tens of thousands of chunks. pgvector in the existing Postgres instance handles that comfortably, adds no container to a Compose file that already runs twenty-one, and — decisively — lets metadata filters (`status`, `merchant_id`, `price`, `category`) and vector similarity run in one query with one consistency model. Closes [RFC 0000](0000-system-architecture.md) Q2. The retrieval interface is narrow enough that Qdrant is a later swap; see §5.
- **D2 — `assistant` owns `assistant_db`, including the vectors.** Chunks are a derived projection, not catalog data. `catalog` must not grow a vector column, and `assistant` must not read `catalog_db`.
- **D3 — Indexing is event-driven, never polled.** The `indexer-worker` is one more consumer on the [RFC 0003](0003-events-and-observability.md) base class, with the same dedupe, retry, and DLQ behaviour. No component polls another's database.
- **D4 — The indexed unit is the chunk; the retrieved unit is the product.** Chunks are embedded and searched, then results are grouped by product and scored by their best chunk. The customer asks about products, not passages.
- **D5 — Structured product data is rendered into natural-language text before embedding.** `{"color": "red", "size": "M"}` embeds poorly; "Available in red, size M." embeds well. A template per field group turns the record into prose. This is the single highest-leverage decision for retrieval quality and the one most often skipped.
- **D6 — Content-hash embedding cache.** The cache key is `sha256(model, normalised_chunk_text)`. A product edited in one field re-embeds one chunk; a reindex of unchanged content costs nothing (G4). This is both a cost control and what makes iterating on the pipeline affordable.
- **D7 — Hybrid retrieval with Reciprocal Rank Fusion.** Dense results from pgvector and keyword results from Postgres full-text search are fused by RRF, which needs no score normalisation between two incomparable scales. Weights are configurable and defaulted from the evaluation.
- **D8 — Filters are applied in the SQL query, not after retrieval.** Post-filtering silently shrinks a top-k below k and can return nothing when matches exist. Pre-filtering with an indexed `WHERE` on an HNSW scan is what pgvector supports and what keeps k honest.
- **D9 — Live facts are hydrated at read time, never embedded.** Price and stock change constantly; embedding them would make the index wrong within minutes and cause a reindex on every price edit. Vectors carry descriptive text; the search response joins current price from the chunk metadata's product row and current availability from `inventory`.
- **D10 — The embedding provider is an interface with three implementations:** the real provider, an on-disk cached provider for development, and a deterministic fake for tests. No test ever makes a network call or costs money.
- **D11 — Retrieval quality is measured, not asserted.** A checked-in evaluation set of 30 queries with labelled relevant products, and a `make evaluate` target reporting Recall@k, MRR, and nDCG for dense, keyword, and hybrid. Every pipeline change is judged against it (G5, G6).
- **D12 — `assistant` is one service with two routers,** not two services. Retrieval and generation share the embedding client, the chunk store, and the conversation store; splitting them would add a network hop to every query for no ownership gain. Closes [RFC 0000](0000-system-architecture.md) Q1.

**Chunking strategy.** A product is not a document; it is a record with fields of very different character. Chunking it as if it were prose loses the structure that makes it findable.

```
  product + variants + category + merchant
                │
                ▼
   render to natural language (D5)
                │
    ┌───────────┼────────────┬─────────────┬──────────────┐
    ▼           ▼            ▼             ▼              ▼
 identity   description   attributes    variants      category
 chunk      chunk(s)      chunk         chunk         chunk
 (always)   (split if     (rendered     (rendered     (path +
            > 512 tok,    from jsonb)   options)      siblings)
            80 tok
            overlap)
                │
                ▼
  every chunk is prefixed with the product's identity line:
  "PRODUCT: Acme Runner X1 | BRAND: Acme | CATEGORY: Footwear > Running"
```

- **Identity prefix on every chunk (D3 of retrieval quality).** A chunk reading "Machine washable, 280g" is unretrievable and, worse, unusable as context because nothing says what it describes. The prefix costs tokens and buys both.
- **Target ~256 tokens, hard maximum 512, overlap 80** on split description text only. Short fields are never padded or merged.
- **Chunk count is bounded** at 12 per product; beyond that, description chunks are truncated, so one merchant's essay cannot dominate the index.

**Indexing pipeline, per event.**

```
  catalog.product.published / .updated
        │
        │ consumer base class: dedupe on event_id, retry, DLQ  (RFC 0003)
        ▼
  1. load search_document from the event payload
  2. render → chunk → normalise                        (D5, chunking)
  3. for each chunk: sha256(model, text)
        cache hit  → reuse vector, no API call         (D6)
        cache miss → collect for batching
  4. embed misses in one batched call (≤ 96 per call)
  5. transaction:
        delete chunks for this product whose hash is no longer present
        upsert chunks with vectors + metadata
        update index_state (version, chunk count, indexed_at)
     commit
  6. emit metric: chunks indexed, cache hit rate, tokens spent

  catalog.product.unpublished / archived
        → delete every chunk for that product, update index_state  (G2)
```

**Search pipeline, per query.**

```
  query: "affordable running shoes for beginners"
   + filters {category?, max_price?, merchant_id?}
        │
        ├─▶ embed query (cached by hash; typical queries repeat)
        │        │
        │        ▼
        │   dense: pgvector cosine, HNSW, pre-filtered on status +
        │          merchant_active + optional filters          (D8)
        │          → top 50 chunks
        │
        └─▶ keyword: Postgres full-text over chunk text,
                     same WHERE clause
                     → top 50 chunks
                        │
                        ▼
              Reciprocal Rank Fusion (D7)
                 score = Σ w_i / (k + rank_i),  k = 60
                        │
                        ▼
              group chunks → products, keep best chunk per product  (D4)
                        │
                        ▼
              hydrate live price and availability                  (D9)
                        │
                        ▼
              top N products, each with its matching chunk,
              scores, and the reason it matched
```

The matching chunk is returned, not just the product id, because Milestone 6 needs exactly that text as grounding context and should not have to re-derive it.

#### 3.1.2. API changes

**assistant — retrieval**

| Method | Path | Auth | Role | Purpose |
| --- | --- | --- | --- | --- |
| `POST` | `/api/v1/search/semantic` | public | — | Body: `{query, limit?, filters?: {category_id, min_price, max_price, merchant_id}, mode?: "hybrid"\|"dense"\|"keyword"}`. Returns products with `score`, `matched_chunk`, and live `price` / `availability` |
| `GET` | `/api/v1/search/similar/{product_id}` | public | — | Products similar to a given one, by centroid of its chunks. Free once the vectors exist, and the foundation for M6's cross-sell |
| `GET` | `/api/v1/search/index/status` | bearer | `admin` | Indexed product count, chunk count, lag, last error, cache hit rate |
| `POST` | `/api/v1/search/index/reindex` | bearer | `admin` | Trigger a full or scoped backfill. Returns `202` with a job id (G8) |
| `GET` | `/api/v1/search/index/{product_id}` | bearer | owner `merchant`, `admin` | The chunks stored for one product — the debugging view, and how a merchant sees what the assistant knows |

**No changes to existing service APIs.** This milestone consumes events and adds a service; `catalog`, `order`, and `inventory` are untouched. `gateway` gains one route prefix.

`GET /api/v1/inventory/{variant_id}` is called by the hydration step (D9), through the existing public route, with its 10-second cache from [RFC 0003](0003-events-and-observability.md) absorbing the load.

#### 3.1.3. Data/schema changes

**Every column of every table in the platform is specified in
[RFC 0006 §3.1.3](0006-service-decomposition-and-data-model.md#313-dataschema-changes).**
This section names only what this milestone creates or amends; it does not
restate columns, so a type has exactly one definition in the RFC set.

**Databases created:** `assistant_db` — PostgreSQL with the `vector` extension.

| Table | Action | Columns |
| --- | --- | --- |
| [`product_chunks`](0006-service-decomposition-and-data-model.md#product_chunks) | Created — chunk text, `vector(1536)`, `tsvector`, pre-filter snapshots | [0006](0006-service-decomposition-and-data-model.md#product_chunks) |
| [`embedding_cache`](0006-service-decomposition-and-data-model.md#embedding_cache) | Created — embeddings keyed by content hash | [0006](0006-service-decomposition-and-data-model.md#embedding_cache) |
| [`index_state`](0006-service-decomposition-and-data-model.md#index_state) | Created — per-product index freshness | [0006](0006-service-decomposition-and-data-model.md#index_state) |
| [`query_log`](0006-service-decomposition-and-data-model.md#query_log) | Created — every search, for the evaluation set | [0006](0006-service-decomposition-and-data-model.md#query_log) |
| [`conversations`](0006-service-decomposition-and-data-model.md#conversations) · [`messages`](0006-service-decomposition-and-data-model.md#messages) | **Created empty**, used in [RFC 0005](0005-ai-assistant-and-streaming.md) | [0006](0006-service-decomposition-and-data-model.md#conversations) |
| [`processed_events`](0006-service-decomposition-and-data-model.md#processed_events) | Created — S13 dedupe | [0006](0006-service-decomposition-and-data-model.md#processed_events) |

Creating `conversations` and `messages` now means Milestone 6 adds no migration
to a service that is already running, and the schema can be reviewed a week earlier.

**Vocabularies introduced ([RFC 0006](0006-service-decomposition-and-data-model.md#s4--enums-as-text) S4):** `ChunkType`, `IndexStatus`, `SearchMode`.

**Vector indexing.** HNSW on `product_chunks.embedding`, `vector_cosine_ops`,
`m = 16`, `ef_construction = 64`, with `ef_search` tuned per query; GIN on
`search_vector`. The parameters and the HNSW-over-IVFFlat decision are recorded
with the table in [RFC 0006 §3.1.3.5](0006-service-decomposition-and-data-model.md#product_chunks) and argued in §3.2 below.
The index build is part of the migration; on a large corpus the HNSW build is the
slow step and is run after the backfill rather than before.

**No changes to any other database.** `catalog.products.search_document`, added in
[RFC 0003](0003-events-and-observability.md), is the input and already exists.

#### 3.1.4. Breaking changes

**None.** This milestone is purely additive: one service, one worker, one database, one route prefix. Every existing endpoint behaves exactly as before, and no existing schema changes.

The one behavioural interaction: the hydration step adds read traffic to `GET /inventory/{variant_id}`, absorbed by the cache introduced in [RFC 0003](0003-events-and-observability.md).

#### 3.1.5. Action Plan

Ordered so that retrieval quality can be measured before it is optimised — the evaluation harness lands before the pipeline is tuned, not after.

| # | Task | Output | Depends on |
| --- | --- | --- | --- |
| T1 | pgvector image and extension; `assistant` skeleton; Alembic; `assistant_db` created | Service starts, migration to head, `vector` extension loaded | RFC 0003 done |
| T2 | Embedding provider interface + real, disk-cached, and fake implementations (D10) | Unit-tested; tests make no network call | T1 |
| T3 | Product renderer: structured record → natural-language text (D5) | Golden-file tests over representative products | T1 |
| T4 | Chunker: typed chunks, identity prefix, size and overlap rules, bound of 12 | Unit tests on boundaries, sizes, prefixes (G3) | T3 |
| T5 | Embedding cache with content hashing | Test: second call for identical text makes zero API calls (G4) | T2, T4 |
| T6 | Chunk storage, HNSW and GIN indexes, upsert-and-prune transaction | Migration; storage round-trip test | T4 |
| T7 | `indexer-worker` consuming the three catalog topics, on the RFC 0003 base class | Publish → indexed end to end (G1); unpublish removes (G2) | T6, T5 |
| T8 | Backfill: `make reindex`, batched over `search_document`, resumable | Whole catalog indexed from empty (G8) | T7 |
| T9 | Dense search: pgvector cosine with pre-filters (D8) | Returns ranked chunks; filters are in the SQL, verified by `EXPLAIN` | T6 |
| T10 | Keyword search over `search_vector`, same filter clause | Returns ranked chunks | T6 |
| T11 | **Evaluation harness**: 30-query set with labelled products, `make evaluate` reporting Recall@k, MRR, nDCG for all three modes | Baseline numbers for dense and keyword recorded | T9, T10 |
| T12 | RRF fusion (D7), weights tuned against the evaluation set | **Hybrid beats both baselines** (G6) | T11 |
| T13 | Grouping to products, best-chunk selection, live hydration (D4, D9) | Search returns products with current price and availability | T12 |
| T14 | `POST /search/semantic` with filters, limits, and modes | Contract tests; visibility rules enforced (G7) | T13 |
| T15 | `GET /search/similar/{id}` by chunk centroid | Tests green | T13 |
| T16 | Index status, reindex trigger, and per-product chunk inspection endpoints | Tests green; admin-gated | T8 |
| T17 | Query logging and zero-result tracking | Rows written; zero-result queries queryable | T14 |
| T18 | Observability: indexing lag, cache hit rate, tokens and cost, search latency by mode, zero-result rate; Grafana panel | Panels live (G9) | T14 |
| T19 | Tuning pass against the evaluation set — chunk size, overlap, RRF weights, k | **Recall@5 ≥ 0.8** (G5); the tuning decisions recorded in §11 | T12, T18 |
| T20 | Seed corpus: ~200 realistic products across 8 categories with genuine descriptions | Enough corpus for the evaluation to mean something | T8 |
| T21 | End-to-end: publish → index → semantic query → correct product, and the update and unpublish paths | `make test` green cold | T19, T20 |

### 3.2. Rationale

**Why retrieval gets its own milestone and its own quality bar.** The failure mode of a RAG system is not an error; it is a confident answer about the wrong product. That failure is introduced in retrieval and is invisible from the generation side, because the model dutifully answers using whatever it was handed. Treating this week as plumbing — "get vectors in a database, move on" — is the standard way to arrive at Milestone 6 with an assistant that sounds good and is wrong, and no way to tell which half is at fault. Hence D11 and G5: a checked-in evaluation set, measured before the LLM is anywhere near the system.

**Why pgvector rather than a dedicated vector database.** Three reasons, in order of weight. First, filtering: nearly every real query here is *semantic plus constraints* — under a budget, in a category, in stock, published. With pgvector that is one SQL statement with one consistency model; with an external store it is either post-filtering (which breaks k, D8) or duplicating catalog metadata into the vector store and keeping it in sync. Second, scale: tens of thousands of chunks is squarely inside pgvector's comfortable range, far from where a specialised engine's advantages appear. Third, operational cost: the Compose file already runs twenty-one containers after Milestone 3. The honest counterargument is that a dedicated store is what a larger system would use — which is why the retrieval interface is narrow enough to swap (§5).

**Why structured fields are rendered to prose before embedding.** Embedding models are trained on natural language. `{"material": "mesh", "weight_g": 280}` produces a vector that sits nowhere useful in the space, while "Made from breathable mesh; weighs 280 grams" lands near queries about lightweight breathable shoes. This is a template, not a model, and it is the cheapest large gain in the pipeline — which is precisely why it is stated as a decision rather than left to whoever writes the indexer.

**Why every chunk carries an identity prefix.** Two independent reasons. For retrieval, a chunk of bare attributes matches a query about attributes and nothing else — the product's name and category are often the strongest signal a query carries. For generation, an unprefixed chunk handed to a model as context invites it to attribute a feature to the wrong product, which is the most damaging hallucination this system could produce. Thirty tokens per chunk is cheap insurance against both.

**Why hybrid rather than dense alone.** Dense retrieval is weak exactly where commerce needs strength: exact model numbers, SKUs, brand names, and rare terms. A customer searching "SR-450X" wants that product, and a semantic model will happily return things that are conceptually similar to a product code. Keyword search is weak where Part B's example queries live — "gifts for kids age 10" shares no vocabulary with the products that answer it. The two fail in complementary places, which is the textbook case for fusion. RRF is used because it combines ranks rather than scores, so cosine similarity and BM25 never have to be made comparable.

**Why price and stock are hydrated rather than embedded.** Embedding volatile facts makes the index wrong continuously and forces a reindex on every price change — the single most frequent catalog write. Worse, a stale embedded price would be presented to a customer as current. Vectors describe what a product *is*; the response reports what it currently *costs* and whether it can be bought.

**Why the embedding cache is keyed by content hash rather than product id.** Product-keyed caching invalidates everything on any edit. Content-keyed caching means a merchant fixing a typo in the description re-embeds one chunk, and a change to the chunking strategy — which happens repeatedly during T19's tuning — re-embeds only the chunks whose text actually changed. It turns iteration from expensive into free, which is what makes the tuning pass possible at all.

**Why indexing is a Kafka consumer rather than a call from `catalog`.** `catalog` should not know that a retrieval layer exists; adding a call would couple publishing latency and availability to a service that did not exist a week earlier. The event already exists, the base class already handles dedupe and DLQ, and a failed index leaves the product published and searchable by keyword — a degradation, not an outage. This is the payoff for the event backbone built in Milestone 3.

**Why the conversation tables are created a week early.** M6 is a heavy week. Creating two empty tables now costs nothing, removes a migration from that week, and gets the conversation schema reviewed alongside this one.

### 3.3. Tradeoffs

| Trade being made | The cost | Why it is acceptable |
| --- | --- | --- |
| pgvector rather than a dedicated vector store | Less impressive on paper; would not scale to millions of vectors; fewer index-tuning knobs | Correct at this scale, and the filtering integration is a genuine advantage rather than a compromise. The swap is narrow (§5) |
| Eventually consistent index | A product is unsearchable for a few seconds after publishing | Seconds, and it fails safe — keyword browse still finds it |
| Chunk duplication of catalog data | The same text lives in `catalog_db` and `assistant_db` | It is a derived projection, rebuildable at any time by `make reindex`. That is what projections are |
| Identity prefix on every chunk | ~30 tokens per chunk in storage and embedding cost | Buys retrievability and prevents cross-product attribution in M6. The cheapest good decision in the design |
| Hybrid search | Two queries and a fusion step per search; more code; two index types to maintain | Measured improvement (G6). If the evaluation showed no gain, the honest response would be to drop it |
| An evaluation set built by hand | Half a day to write 30 queries and label them; inherently subjective | Without it, every later decision is guesswork and no regression is detectable. Half a day is cheap for that |
| Embedding provider as an external dependency | An API key, a cost, a rate limit, and a network failure mode | Cached aggressively; batched; the fake covers tests; a local model is a drop-in (Q3) |
| Live hydration per search | One extra call per result set | Cached at 10s from RFC 0003, and the alternative is showing customers stale prices |
| Bound of 12 chunks per product | A very long description is truncated | Prevents one merchant dominating the index; the limit is configurable and logged when hit |
| Vectors in the same Postgres instance | Index builds and searches contend with transactional load | Separate logical database, and the HNSW build is the only heavy operation. Splitting the instance is a connection-string change |

#### 3.3.1. Challenges and Open Questions

| # | Question | Current position | Needs a decision by |
| --- | --- | --- | --- |
| Q1 | Which embedding model? | `text-embedding-3-small` (1536 dims) as the default — cheap, strong, and widely benchmarked. `dimension` is configuration, and a change triggers a full reindex flagged by `index_state.embedding_model` | T2 |
| Q2 | HNSW or IVFFlat? | HNSW. Better recall at this size, no training step, and no rebuild as the corpus grows — IVFFlat's list count would need revisiting constantly during a week of reindexing | Decided |
| Q3 | Should a local embedding model (sentence-transformers) be the default instead? | No, but it is the fallback if the API key is unavailable. Local models cost a container, ~400MB, and CPU inference latency; the provider interface makes it a swap | If provider access fails |
| Q4 | Is 30 evaluation queries enough? | It is enough to catch regressions and rank three strategies, and not enough to make fine distinctions. Stated plainly rather than overclaimed | — |
| Q5 | Does the assistant need a reranker? | Not yet. Decide from T19's numbers: if hybrid Recall@20 is high but Recall@5 is low, a reranker is the right fix; if both are low, retrieval is the problem and a reranker would not help | After T19 |
| Q6 | How are products from suspended merchants excluded? | `product_status` is denormalised into the chunk and updated by the `merchant.status_changed` reaction already built in [RFC 0003](0003-events-and-observability.md) Q6, which unpublishes the products and so removes the chunks | Decided |
| Q7 | Should the query embedding be cached? | Yes, in the same `embedding_cache` — real query distributions are heavily repeated, and it removes the provider from the latency path for common queries | Decided |
| Q8 | Chunk size of 256 tokens — right for product data? | It is a starting point, tuned in T19 against the evaluation set. Product text is short, so the risk is over-splitting rather than under | T19 |
| Q9 | What happens to search when the embedding provider is down? | Fall back to keyword-only with a flag in the response, rather than returning an error. Degraded is better than absent | Decided |
| Q10 | Should `query_log` retain user ids indefinitely? | No — 90 days, then anonymised. It is a log for tuning, not a behavioural profile | Decided |

---

## 4. Dependencies/blockers/known issues

**New dependencies.**

| Dependency | Version | Used for |
| --- | --- | --- |
| `pgvector/pgvector:pg16` | 0.7.x | Vector storage and HNSW indexing |
| `pgvector` (Python) | 0.3.x | SQLAlchemy vector type |
| `openai` *(or the chosen provider's SDK)* | 1.5x | Embedding generation |
| `tiktoken` | 0.8.x | Token counting for chunk sizing and cost accounting |
| `numpy` | 2.x | Centroid computation and fusion arithmetic |
| `sentence-transformers` *(optional)* | 3.x | Local embedding fallback (Q3) |

**External dependency.** An LLM provider API key — the only external account in the entire project, and the reason [RFC 0000](0000-system-architecture.md) §4 flags obtaining it before Week 4 rather than during it.

**Blockers.** Only the API key. The fake and disk-cached providers mean development and tests are never blocked by it.

**Known issues carried into this milestone.**

- Index lag of a few seconds after publishing.
- A changed embedding model requires a full reindex (detected, not automatic).
- No reranking, so precision at the very top of the list is whatever fusion produces.
- The evaluation set is small and written by one person.
- English only.
- Zero-result queries are logged but there is no automatic query-expansion or spelling correction.
- Search results are not personalised in any way.
- The Postgres instance now carries vector index builds alongside transactional load.

---

## 5. Alternate solutions

**Alternative 1 — Qdrant, Weaviate, or Milvus as a dedicated vector database.** Purpose-built, better at scale, richer index tuning, built-in payload filtering.

*Rejected for this scale, not on merit.* At tens of thousands of chunks, pgvector's recall and latency are comparable, and the integration advantages are real: one transaction, one backup, one consistency model, and metadata filters that are ordinary SQL predicates. The deciding factor is that pre-filtering by status, merchant, category, and price is the common case here, and doing that against an external store means either post-filtering or replicating catalog metadata into it. The retrieval layer is isolated behind a `VectorStore` protocol with four methods (`upsert`, `search`, `delete_by_product`, `count`), so the swap is a class, not a rewrite — and would be the right call if the corpus grew by two orders of magnitude.

**Alternative 2 — Elasticsearch or OpenSearch with dense vector support.** One engine for keyword and vector, mature hybrid support, excellent text analysis.

*Rejected.* A JVM container with a substantial memory footprint added to a stack that is already at the machine's limit after Milestone 3, plus a second source of truth for product text to keep in sync. Postgres full-text search is adequate for the keyword half at this corpus size, and it shares the transaction with the vector half.

**Alternative 3 — Dense retrieval only; no hybrid.** Simpler: one query, one index, no fusion.

*Rejected* on the failure mode. Exact identifiers — SKUs, model numbers, brand names — are common in commerce queries and are where dense retrieval is weakest. The cost of hybrid is one additional query and ~40 lines of fusion, and G6 requires the improvement to be demonstrated rather than assumed.

**Alternative 4 — Keyword search only, with query expansion via an LLM.** No embeddings, no vector store, no reindexing; use the model to rewrite the query into keywords.

*Rejected.* It adds an LLM call to the latency path of every search, fails on genuinely semantic queries where no keyword rewriting bridges the gap ("gifts for kids age 10"), and leaves Part B's "semantic product search using embeddings" requirement unmet. It is, however, a reasonable fallback for the degraded path (Q9).

**Alternative 5 — Embed the whole product as one document; no chunking.** Simpler, one vector per product, no grouping step.

*Rejected.* A single vector averaging identity, description, attributes, and every variant is diffuse, and a query matching one narrow aspect matches it weakly. It also removes the ability to hand a model the specific passage that matched, which is precisely what Milestone 6 needs as grounding. Chunking is the reason the assistant can cite a reason for a match.

**Alternative 6 — Index on a schedule by polling `catalog_db`.** No Kafka dependency, simple cron.

*Rejected.* It violates the boundary rule that has held since [RFC 0006](0006-service-decomposition-and-data-model.md#311-architectural-changes) R1 — no service reads another's database — introduces minutes of staleness instead of seconds, and wastes the event backbone built for exactly this. `make reindex` exists for backfill, which is the legitimate batch case.

**Alternative 7 — A managed retrieval service (Pinecone, a vendor RAG API).** Less to build and operate.

*Rejected.* An external account and network dependency for a local Compose assignment, and it would remove the chunking, embedding, and fusion work that is the substance of the milestone.

---

## 6. Production readiness

### 6.1. Scalability and Fault tolerance

- **Indexing failure is isolated.** The `indexer-worker` is a Kafka consumer with the [RFC 0003](0003-events-and-observability.md) retry and DLQ behaviour. A failed index leaves the product published and keyword-searchable; it does not fail the publish, and the DLQ preserves the event for replay.
- **Provider failure degrades rather than breaks.** Embeddings unavailable → indexing retries with backoff and the event waits in Kafka; search falls back to keyword-only with an explicit flag (Q9).
- **The cache absorbs most of the cost and most of the latency.** Repeated queries never reach the provider; unchanged content never re-embeds.
- **Reindex is resumable and idempotent**, batched with a cursor, safe to interrupt and restart — which matters because it will be run repeatedly during tuning.
- **Pre-filtering keeps k honest** (D8), so a heavily filtered query returns a full result set instead of a handful of survivors.
- **HNSW search is sub-linear** and `ef_search` is tunable at query time, giving a recall-versus-latency dial without an index rebuild.
- **Backpressure.** Indexing lag is bounded by Kafka retention and visible as a metric; the worker scales horizontally to the partition count.
- **Blast radius.** `assistant` being down removes semantic search only. Browse, checkout, and every Part A flow are untouched, because nothing in Part A depends on it.
- **Known ceilings.** Single Postgres; HNSW index build is memory-hungry on a large reindex; one embedding provider with its own rate limit.

### 6.2. Security considerations

- **Visibility is enforced in the query, not the response.** Only `published` products from active merchants are indexed, and the filter is in the `WHERE` clause. An unpublished or archived product cannot leak through a semantic query — the highest-risk failure in this milestone, since a vector index is an easy place to forget authorisation (G7).
- **Merchant isolation** — `/search/index/{product_id}` shows chunks only for products the caller owns.
- **Prompt-injection groundwork.** Merchant-supplied product text will become LLM context in Milestone 6. Indexing strips control characters, normalises whitespace, bounds length, and stores content verbatim but clearly delimited by chunk type, so M6 can fence it. Nothing in a description is ever treated as an instruction by this milestone, and [RFC 0005](0005-ai-assistant-and-streaming.md) inherits the delimiters.
- **The API key lives only in `assistant` and the indexer**, from the environment, never logged and never in a span attribute or an error message.
- **`query_log` is data about people.** User ids are retained 90 days then anonymised (Q10); the log is admin-only and never exposed through a public endpoint.
- **Search endpoints are public and hit a paid provider**, so they are rate limited at the gateway — 20 searches per minute per IP, 60 per authenticated user. Without this, the public search endpoint is a way to spend the project's API budget.
- **Query text is user input reaching an external service.** Length-bounded, control characters stripped, and never interpolated into SQL — the ORM and bound parameters throughout.
- **Accepted gaps** — no encryption at rest for embeddings (they are derived from public product text); no per-merchant quota on indexing volume.

### 6.3. Deployment strategy

- **Compose profile `ai`** adds `assistant` and `indexer-worker`. The Postgres image changes to `pgvector/pgvector:pg16`, which is a drop-in for the existing one; the extension is created by migration in `assistant_db` only.
- **`make reindex`** for a full backfill, `make evaluate` for the retrieval evaluation. Both are first-class targets because both are run repeatedly.
- **Model and dimension are configuration.** `EMBEDDING_MODEL` and `EMBEDDING_DIMENSION` are validated at startup against `index_state`; a mismatch logs a loud warning and marks the index stale rather than silently producing garbage similarity.
- **The disk-cached provider is the development default**, so a developer without an API key can run and test everything except a genuinely new embedding.
- **Index build** is part of the migration; on a large corpus the HNSW build is the slow step and is run after the backfill rather than before, which is an order the reindex task enforces.
- **Rollback.** The service is additive, so rolling it back removes semantic search and affects nothing else. `assistant_db` can be dropped and rebuilt from events and `search_document` at any time — it holds no original data.

---

## 7. Test Plan

**Unit** — no I/O, deterministic fake provider.

- Renderer: each field group produces the expected prose; missing and empty fields are handled without producing "None" in the text (golden files).
- Chunker: size limits respected; overlap correct; identity prefix on every chunk; short fields never split; the 12-chunk bound enforced; a product with only a name still produces one valid chunk.
- Content hashing: whitespace and case normalisation make trivially different text hash identically; genuinely different text does not collide.
- RRF: known rank inputs produce the known fused order; weight changes move results as expected; an empty result list from one side does not break fusion.
- Token counting matches the tokeniser within tolerance.
- Filter construction: every filter combination produces a valid, injection-free predicate.

**Integration** — real Postgres with pgvector, fake or cached provider.

- Upsert, search, and delete round-trip; cosine ordering is correct for known vectors.
- **Re-indexing an unchanged product makes zero provider calls** (G4), asserted against the fake's call counter.
- Editing one field re-embeds only the affected chunks and prunes the stale ones.
- `unpublish` removes every chunk for the product and updates `index_state` (G2).
- **Visibility: draft, archived, and suspended-merchant products never appear in any search result, in any mode, with any filter** (G7).
- Pre-filtering: a filtered query returns a full `limit` where enough matches exist, verified against a post-filtered implementation returning fewer.
- Similar-products returns the product's neighbours and never the product itself.
- Backfill from an empty index reaches full coverage and is resumable after an interruption (G8).
- Provider down → search returns keyword results with the degradation flag, not an error (Q9).

**Retrieval quality** — the evaluation harness (D11, G5, G6), run by `make evaluate`.

- A checked-in set of 30 queries against the seed corpus, each labelled with the products that should be returned, spanning: natural-language intent ("something for a beginner runner"), constraint queries ("wireless headphones under 100"), exact identifiers (SKU, model number), category browsing, and vague queries ("a gift for my nephew").
- Reported per mode: **Recall@1, @5, @10, MRR, nDCG@10**, and mean latency.
- **Acceptance: hybrid Recall@5 ≥ 0.8, and hybrid ≥ both single-mode baselines on Recall@5 and MRR** (G5, G6).
- The harness runs in CI against the fake provider's cached vectors, so quality regressions are caught by a diff rather than by a demo.
- Deliberate negative cases: a query with no valid answer returns few or no results rather than confidently returning the nearest unrelated product.

**Performance**

- Search latency p95 measured over 100 queries against the seed corpus, reported with and without the query-embedding call (G10).
- Indexing throughput: products per second on a full backfill, and cache hit rate during it.

**End-to-end** — through the gateway, cold start.

1. Publish a product with a distinctive description → poll → it is semantically searchable within seconds by a query sharing no exact words with it (G1).
2. Edit its description → the change is retrievable; the old text is not.
3. Unpublish → it disappears from every mode (G2, G7).
4. Republish → it returns.
5. A query with filters returns only matching products, with current price and availability from `inventory` (D9).
6. `make reindex` from empty reproduces the full index, and the evaluation numbers are unchanged (G8).
7. Assert one trace spans gateway → assistant → embedding call → Postgres, with the indexing trace linked to the publishing trace through the event's `traceparent`.

**Explicitly not tested** — answer quality (nothing generates answers yet); corpora beyond the seed size; multilingual queries.

---

## 8. Impact Analysis

**On Part A's services.** Effectively none. `assistant` consumes events and calls two public APIs. No Part A service knows it exists, which is the property the event backbone was built for, and it means a failure here cannot affect checkout.

**On Milestone 6.**

| What this RFC delivers | What M6 does with it |
| --- | --- |
| `retrieve(query, filters, k)` returning products with their matching chunk text | Becomes the retrieval node of the LangGraph agent, unchanged |
| Chunks with identity prefixes and token counts | Context assembly within a token budget, with no risk of cross-product attribution |
| `conversations` and `messages` tables, empty | No migration needed in M6 |
| Embedding cache and provider interface | Reused for any embedding M6 needs; the same fake makes M6's tests free |
| `similar/{product_id}` | The basis for cross-sell suggestions |
| Delimited, sanitised merchant text | The starting point for prompt-injection defence |
| A measured retrieval baseline | When an answer is wrong, it is answerable whether retrieval or generation caused it — the single most useful thing to have going into M6 |

**On effort.** Lighter than Milestones 2 and 3 in component count — one service, one worker, one database — but with a different kind of difficulty: the work is iterative and quality-driven rather than structural. The action plan's ordering reflects that; T11's evaluation harness lands before T12's fusion so that tuning is measurement rather than intuition.

**Risks.**

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Retrieval quality disappoints and the cause is unclear | Medium | High | The evaluation set isolates it. If dense is weak, the renderer and chunker are the suspects; if both modes are weak, the corpus is the suspect |
| The seed corpus is too small or too synthetic for the evaluation to mean anything | **High** | High | T20 is a real task with a real budget — 200 products with genuine descriptions across 8 categories, not lorem ipsum. Everything downstream depends on it |
| Embedding costs or rate limits during repeated reindexing | Medium | Medium | Content-hash cache (D6); batching; the disk-cached provider for development; a small model |
| API key unavailable when Week 4 starts | Low | High | Flagged in [RFC 0000](0000-system-architecture.md) §4; the local model fallback (Q3) unblocks everything but quality comparisons |
| Time spent tuning chunk sizes with no measurable gain | Medium | Medium | T19 is time-boxed and judged only by the evaluation numbers; a change that does not move them is reverted |
| Vector index build contending with transactional load | Low | Medium | Separate logical database; backfill run deliberately, not automatically |
| Unpublished products leaking into search results | Low | **High** | G7 is an explicit goal with a dedicated test across every mode and filter combination |

**On reviewers.** Milestone 5's review should be able to check: publishing indexes automatically; unpublishing removes; no unpublished product can be surfaced by any query; the evaluation numbers are real and reproducible by running `make evaluate`; and hybrid's advantage over each baseline is measured rather than asserted.

---

## 9. Useful Links/References

**Project documents**

- [RFC 0000 — System Architecture](0000-system-architecture.md) · [RFC 0003 — Events & Observability](0003-events-and-observability.md) · [RFC 0005 — AI Assistant & Streaming](0005-ai-assistant-and-streaming.md)
- [Part B](../../part-b.md) · [Milestones](../../milestones.md) · [Execution Guidelines](../../execution-guidelines.md)

**Technical references**

- pgvector — https://github.com/pgvector/pgvector
- pgvector HNSW indexing and tuning — https://github.com/pgvector/pgvector#hnsw
- OpenAI embeddings guide — https://platform.openai.com/docs/guides/embeddings
- Reciprocal Rank Fusion (Cormack et al., 2009) — https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf
- BEIR — heterogeneous retrieval benchmarking methodology — https://github.com/beir-cellar/beir
- Postgres full-text search — https://www.postgresql.org/docs/16/textsearch.html
- Chunking strategies for RAG — https://www.pinecone.io/learn/chunking-strategies/
- `tiktoken` — https://github.com/openai/tiktoken
- Retrieval evaluation metrics (MRR, nDCG) — https://en.wikipedia.org/wiki/Discounted_cumulative_gain

---

## 10. Related tickets/issues

| ID | Title | Maps to |
| --- | --- | --- |
| *(to be created)* | M5-T1 … M5-T21 | The tasks in §3.1.5, one ticket each |
| — | Milestone 5 — Retrieval Layer | This RFC |
| — | RFC 0000 Q1 | `assistant` as one service — closed here (D12) |
| — | RFC 0000 Q2 | pgvector versus a dedicated vector DB — closed here (D1) |
| — | RFC 0003 T21 | `catalog.product.published` — consumed here |
| — | RFC 0005 — AI Assistant & Streaming | Consumes the retrieval interface built here |

---

## 11. Notes

- **On treating retrieval as a quality problem.** The one decision that most determines whether Part B succeeds is building the evaluation set in T11 before tuning anything. Without it, Milestone 6 debugging becomes guesswork about which half is wrong, in the last week of the project.
- **On the seed corpus.** T20 looks like a chore and is not. Thirty evaluation queries against twenty lorem-ipsum products measure nothing. The corpus needs real category spread, realistic descriptions, overlapping products that are genuinely hard to tell apart, and a few with model numbers — because that is what makes the hybrid comparison meaningful.
- **On the renderer.** D5 is the least technically interesting decision here and probably the highest-leverage. It is worth a golden-file test per product shape.
- **Tuning record.** T19's outcomes — final chunk size, overlap, RRF weights, `ef_search`, and the evaluation numbers before and after — are recorded here when the milestone completes, so the review can see what was tried and what it bought.

  | Parameter | Initial | Final | Recall@5 before → after |
  | --- | --- | --- | --- |
  | *(filled in at T19)* | | | |

- **Definition of done for Milestone 5.** Goals G1–G10 verified by their stated checks; `make up-all && make migrate && make seed && make reindex && make evaluate && make test` green from a clean clone; the evaluation table above filled in; this RFC moved to **DONE** with review outcomes folded in.

---

## 12. Comments

*Reviewer comments go here. Please raise objections against the numbered decisions (D1–D12), the open questions (Q1–Q10), the chunking strategy in §3.1.1, or the rejected alternatives in §5, so the discussion attaches to something specific.*

| Reviewer | Section | Comment | Resolution |
| --- | --- | --- | --- |
| | | | |
