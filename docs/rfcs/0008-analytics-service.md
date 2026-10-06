# RFC 0008 — `analytics` Service

| | |
| --- | --- |
| **Owner** | @muhammad.adnan |
| **Collaborators** | SarwanAhmed |
| **Status** | **DRAFT** · IN-REVIEW · DONE |
| **Created** | Sep 29, 2026 |
| **Supersedes** | The analytics content of [archive/0003-events-and-observability.md](archive/0003-events-and-observability.md) (C8, C9, the nine-metric table, the analytics endpoints, `analytics_db`, Q4, Q10, T17–T19) · the Part B analytics additions of [archive/0005-ai-assistant-and-streaming.md](archive/0005-ai-assistant-and-streaming.md) (E13 consumer side, G10, Q3, Q11, T20–T21, `assistant_conversions`) · the analytics rows of [archive/0000-system-architecture.md](archive/0000-system-architecture.md) (service inventory row 7, store selection, Q8) · the `analytics_db` catalogue and the Mongo `processed_events` variant of [archive/0006-service-decomposition-and-data-model.md](archive/0006-service-decomposition-and-data-model.md) |
| **Depends on** | [RFC 0000 — Architecture](0000-architecture.md) · [RFC 0001 — Platform and Shared Infrastructure](0001-platform-and-shared-infrastructure.md) · [RFC 0002 — `gateway`](0002-gateway-service.md) · [RFC 0003 — `identity`](0003-identity-service.md) · [RFC 0004 — `catalog`](0004-catalog-service.md) · [RFC 0005 — `order`](0005-order-service.md) · [RFC 0006 — `inventory`](0006-inventory-service.md) · [RFC 0009 — `assistant`](0009-assistant-service.md) |

> **Reading note.** This RFC is the single specification of the `analytics` service: what it owns, every event it consumes and the exact rollup each event moves, the endpoints under `/api/v1/analytics/*`, and every MongoDB collection in `analytics_db`. It is written against the cross-cutting rules in [RFC 0000](0000-architecture.md) — the ownership test, R1–R8, A1–A14, the event catalogue and envelope, S1–S15, and the shared `outbox` / `processed_events` tables — which are not restated here. Hosting concerns (the Kafka consumer base class, the Celery worker and Beat, the MongoDB container, the observability stack) belong to [RFC 0001](0001-platform-and-shared-infrastructure.md). Original decision IDs from the archive (A*, C*, E*, R*, S*, Q*) are kept verbatim; decisions new to this RFC carry the `ANL-` prefix. Where two archived RFCs use the same question number, the source RFC is named alongside it (for example "Q8 (archive 0000)").

---

## Contents

1. [Title](#1-title)
2. [Overview](#2-overview)
   - 2.1. [Goals](#21-goals)
   - 2.2. [Non-goals](#22-non-goals)
3. [Solution Overview](#3-solution-overview)
   - 3.1. [Specification](#31-specification)
     - 3.1.1. [Architectural changes](#311-architectural-changes)
       - 3.1.1.1. [Responsibility and boundaries](#3111-responsibility-and-boundaries)
       - 3.1.1.2. [Domain model](#3112-domain-model)
       - 3.1.1.3. [State machines](#3113-state-machines)
       - 3.1.1.4. [Workflows and async jobs](#3114-workflows-and-async-jobs)
       - 3.1.1.5. [Dependencies and degradation](#3115-dependencies-and-degradation)
       - 3.1.1.6. [Configuration](#3116-configuration)
       - 3.1.1.7. [Observability](#3117-observability)
     - 3.1.2. [API changes](#312-api-changes)
       - 3.1.2.1. [Endpoints](#3121-endpoints)
       - 3.1.2.2. [Role matrix](#3122-role-matrix)
       - 3.1.2.3. [Events produced](#3123-events-produced)
       - 3.1.2.4. [Events consumed](#3124-events-consumed)
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

**`analytics` (:8006) — an event-fed, MongoDB-backed reporting service that computes every [Part A](../part-a.md) §5 and [Part B](../part-b.md) analytics metric from consumed events, never calls another service, and cannot affect checkout.**

---

## 2. Overview

[Part A](../part-a.md) names "fragmented data — order data, shipment data, customer activity, and reporting metrics spread across multiple systems" as a problem, lists "analytics updates" and "order analytics processing" as work that must run off the request path (§2, §3 step 6, §4), and specifies nine metrics (§5). [Part B](../part-b.md) adds four assistant metrics. Every one of them is answered here.

`analytics` is a **pure consumer**. It subscribes to every topic in the [event catalogue](0000-architecture.md#3123-event-catalogue), aggregates on ingest into a small set of MongoDB collections, and serves admin- and merchant-scoped read APIs from those collections. It never calls another service and never reads another database (C8). Every number it reports is derived from events it has consumed, which is exactly what makes it safe for it to be slow, behind, or down: if `analytics` or its MongoDB is unavailable, events accumulate in Kafka and are processed on recovery, and checkout never notices.

The design has four moving parts:

1. **The consumer** — one Kafka consumer group, `analytics`, subscribing to all topics (Q2, archive 0003). Each event is deduplicated against `processed_events` and applied to the rollups in **one MongoDB transaction** (ANL-1), so a counter moves exactly once per event even under redelivery.
2. **The rollups** — `counters` (monotonic totals), `daily_metrics` (per-day, per-scope time series), `product_stats` (per-product totals for ranking), `failure_stats` (failed events and workflow issues), and `assistant_conversions` (assistant interaction → order correlation). `raw_events` keeps every envelope for 7 days as the backfill escape hatch (Q4, archive 0003).
3. **The reconciliation job** — `rebuild_daily_rollups`, a Celery Beat task on the `analytics` queue, which recomputes the time-series buckets still covered by `raw_events` and the statistics that cannot be incrementally maintained (Q8, archive 0000).
4. **The read API** — `/api/v1/analytics/*`, where windows are composed from daily buckets and rankings are computed on read (C9).

### 2.1. Goals

| # | Goal | How it is verified |
| --- | --- | --- |
| ANL-G1 | All nine [Part A](../part-a.md) §5 metrics are computed and queryable (archive 0003 G6) | One test per metric, asserting an exact value after a scripted event sequence |
| ANL-G2 | The four [Part B](../part-b.md) analytics metrics are computed — usage, questions asked/answered, conversion after interaction, average latency (archive 0005 G10) | One test per metric |
| ANL-G3 | The consumer is idempotent — replaying the entire topic changes nothing (archive 0003 G3) | Replay test: reset offsets to earliest, re-consume, assert every counter and document is unchanged |
| ANL-G4 | A crash at any point in event handling never double-counts and never drops an increment | Kill the consumer between transaction commit and offset commit, and inside the transaction; assert exact counters after restart |
| ANL-G5 | `analytics` cannot affect checkout | Checkout E2E green with `analytics` and its MongoDB stopped; restart drains lag and every counter converges |
| ANL-G6 | Rollup computation is off every request path (archive 0003 G9) | No handler in any service performs rollup computation inline; `analytics` read handlers only read |
| ANL-G7 | A merchant can never see or infer another merchant's figures | Merchant A's revenue, orders, popularity and stockout queries exclude merchant B's data entirely |
| ANL-G8 | Consumer lag is visible (archive 0003 T10, Q7) | `/metrics` exports lag per topic and partition; the event-pipeline Grafana panel shows it |
| ANL-G9 | A metric nobody thought of can be backfilled without replaying Kafka (Q4, archive 0003) | Backfill test: add a projection, run the backfill over `raw_events`, assert the result equals live ingestion of the same events |

### 2.2. Non-goals

| Excluded | Why / where it lives instead |
| --- | --- |
| Querying source services or their databases | Forbidden by C8 and R1/R4. Every number comes from an event |
| Kafka Streams, ksqlDB, Flink | Python consumers are adequate for the metric set; a stream processor is a large new concept for no requirement (archive 0003 §2.2) |
| Exactly-once Kafka semantics (transactions) | At-least-once plus an idempotent consumer is the answer; Kafka transactions do not extend to a MongoDB write (archive 0003 §3.2, Alternative 3) |
| Alerting on consumer lag | Metrics and a dashboard panel only; there is nothing to alert into (Q7, archive 0003). Alerting in general is out of scope ([RFC 0001](0001-platform-and-shared-infrastructure.md)) |
| Real-time (sub-second) dashboards | Analytics is eventually consistent; a dashboard can lag a checkout by seconds, which is correct for reporting |
| Causal attribution of assistant conversions | Conversion after AI interaction is correlational and labelled as such (Q3, archive 0005) |
| Recommendations or behavioural modelling | Part B's cross-sell is a generation task in `assistant` ([RFC 0009](0009-assistant-service.md)); no recommender is built |
| Customer-facing analytics | No customer endpoint exists; merchants see their own scope, admins see everything |
| Token spend and budget reporting | Owned by `assistant` (`GET /api/v1/assistant/usage/admin`, [RFC 0009](0009-assistant-service.md)); `analytics` reports only the Part B metrics |
| Multi-node MongoDB, sharding | One laptop; a single-node replica set (ANL-1). The change is configuration |
| Search-quality analytics from `query_log` | `query_log` belongs to `assistant` and is read there for the evaluation set and latency ([RFC 0009](0009-assistant-service.md)); `analytics` sees only `assistant.interaction` |

---

## 3. Solution Overview

### 3.1. Specification

#### 3.1.1. Architectural changes

```
   identity   catalog   order   inventory   assistant        (producers, via outbox-relay)
       │         │        │         │           │
       └─────────┴────────┴────┬────┴───────────┘
                               ▼
            Kafka  smartretail.<domain>.<event>  (+ <topic>.dlq)
                               │
                               │  consumer group "analytics"  (one process, all topics)
                               ▼
   ┌─────────────────── analytics :8006 ─────────────────────────────┐
   │ consumers/  restore traceparent → decode Avro → handler           │
   │   BEGIN (Mongo session, single-node replica set, ANL-1)           │
   │     insert processed_events {_id:{group,event_id}}  ─ dup? abort, │
   │     insert raw_events (envelope)                       skip       │
   │     $inc / $set / upsert: counters, daily_metrics, product_stats, │
   │                           failure_stats, assistant_conversions    │
   │   COMMIT → commit Kafka offset                                    │
   │                                                                   │
   │ api/v1/analytics/*   read-only; windows composed on read (C9)     │
   └───────────────────────────────┬──────────────────────────────────┘
                                   │
                          ┌────────▼────────┐        ┌──────────────────────┐
                          │ MongoDB         │◀───────│ celery-worker (Beat) │
                          │ analytics_db    │        │ rebuild_daily_rollups│
                          └─────────────────┘        │ queue: analytics     │
                                                     └──────────────────────┘
   Nothing on the order path calls analytics, and analytics calls nothing.
```

##### 3.1.1.1. Responsibility and boundaries

**Owns.** Metric rollups and series — the fact recorded in the [ownership map](0000-architecture.md#3136-ownership-map) as "Metric rollups | `analytics` | — | —": nothing else writes them and nothing else reads them except through this service's API.

| Owns | Does not own |
| --- | --- |
| `counters`, `daily_metrics`, `product_stats`, `failure_stats`, `raw_events`, `assistant_conversions`, and its `processed_events` (Mongo variant) | Any source fact — users, products, orders, stock, conversations. It holds derived copies, never the truth |
| The metric definitions (§3.1.1.2) and their read APIs | Event payload schemas — each producer owns its topic's schema ([RFC 0000](0000-architecture.md#3123-event-catalogue)) |
| The `rebuild_daily_rollups` task logic and the backfill CLI | The Celery worker, Beat scheduler and RabbitMQ that host the task ([RFC 0001](0001-platform-and-shared-infrastructure.md)) |
| Consumer handler logic for every topic | The consumer base class — dedupe hook, retry, DLQ routing, lag metrics — in `common` ([RFC 0001](0001-platform-and-shared-infrastructure.md)) |
| Its Mongo credential and its database | The MongoDB container ([RFC 0001](0001-platform-and-shared-infrastructure.md)) |
| — | Token-budget and spend reporting (`assistant`, [RFC 0009](0009-assistant-service.md)) |
| — | Prometheus RED metrics of other services ([RFC 0001](0001-platform-and-shared-infrastructure.md)) |

**Ownership-test row** ([RFC 0000](0000-architecture.md#3112-the-ownership-test)):

| # | Candidate | State it would own | Invariant | Independent change | Distinct profile | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| 7 | `analytics` | rollups, series | counters monotonic per event | yes | append-heavy, document store, must never affect checkout | **Service** |

The service-inventory rationale ([archive 0000](archive/0000-system-architecture.md) §3.1.1): **kept separate** — read-mostly, append-heavy, different store (document), different query shape, and it must not be able to slow a checkout down.

**Rules that bind it.**

| Rule | How it applies to `analytics` |
| --- | --- |
| R1 | `analytics` reads and writes only `analytics_db`, with its own MongoDB user holding `readWrite` on that database alone. The R1 grant test is extended to Mongo (§7) |
| R3 | It is the sole writer of every collection in `analytics_db`. The Celery task runs `analytics` code with `analytics`' credential, so the writer is still `analytics` |
| R4 | It never joins or reads another service's data; events are its only input |
| R5 / A13 | It makes **no** synchronous cross-service call, in or out of the order path. The one permitted synchronous call in the system (`order → catalog`) does not involve it |
| R6 / A14 | Product names in `product_stats` are snapshots from order lines, never looked up live |
| R7 | Its reads never require another service to be up; Kafka being down stops ingestion, not reads |
| R8 | Metric definitions live in `analytics`' service layer, not in `common` |
| A1, A2 | A service with its own database. A2's shared Postgres instance does not apply: `analytics_db` is MongoDB ([store selection](0000-architecture.md#3137-store-selection)) |
| A4, A5 | Every endpoint verifies the RS256 JWT locally against `identity`'s JWKS; the gateway's check is not trusted |
| A6 | Async throughout: Motor for MongoDB, the async consumer base class |
| A7 | Errors are RFC 9457 problem details |
| A8 / C11 / C12 | `traceparent` is restored from the envelope on consume, so an event's processing is part of the producer's trace |
| A11 / C4 / S13 | Every event is recorded in `processed_events` before its effect, atomically with it (ANL-1) |
| A12 / C7 | Kafka carries the facts it consumes; the scheduled rollup is a Celery Beat task; it runs no Temporal workflow |
| C6 | Three in-process attempts with exponential backoff, then `<topic>.dlq`, then offset commit |
| C8 | Consume-and-aggregate, never query-the-source |
| C9 | Counters incremented on ingest; time series bucketed on ingest; ranking computed on read |
| E13 | Part B metrics come from `assistant.interaction` through this pipeline, not a second one |
| S2 | Money is `Decimal128`, never a float, with an ISO 4217 currency beside it |
| S5 (broken) | `raw_events` is hard-deleted by TTL — derived, rebuildable, TTL-bounded |
| S8 (broken) | `analytics_db` collections are schemaless — per-metric document shapes genuinely vary |

**Module layout** (per [RFC 0000](0000-architecture.md#3117-module-structure-inside-a-service); `models/` holds Motor collection accessors and index declarations instead of SQLAlchemy models):

```
services/analytics/app/
├── api/v1/          router.py (every /analytics/* route) + handler files — parse, validate, call, serialise. No rules
├── services/
│   ├── metrics.py       the metric definitions; the only place a formula lives
│   ├── rollups.py       apply_<event_type>() — pure functions: envelope → list of Mongo update ops
│   ├── scoping.py       merchant scope from the verified claim, injected into every pipeline
│   ├── conversions.py   assistant interaction ↔ order correlation
│   ├── reconcile.py     rebuild_daily_rollups logic (called by the Celery task)
│   └── backfill.py      raw_events → projection replay
├── models/          collection handles, index and TTL declarations, ensure_indexes()
├── schemas/         Pydantic request/response contracts
├── consumers/       one thin handler per topic: decode → services.rollups → txn
├── tasks/           Celery task shims (rebuild_daily_rollups) — call services/reconcile.py
├── cli/             backfill, bootstrap-compacted
└── core/            settings, security dependency, Mongo client, telemetry
```

`services/rollups.py` returns *operations*, not writes, so the same function is used by the live consumer, the reconciliation job and the backfill — one formula, three callers, one set of tests.

##### 3.1.1.2. Domain model

`analytics` has no aggregate in the transactional sense: it enforces no cross-row business invariant with a lock. Its entities are **rollup documents**, and the invariant that makes it a service is **"counters monotonic per event"** — each event's effect is applied at most once and at least once. "Monotonic" here is per event, not per value: `total_products` is decremented by `catalog.product.unpublished`, but every event still moves it exactly once.

**The metrics.** Every [Part A](../part-a.md) §5 and [Part B](../part-b.md) metric, where it is computed, which collection answers it and which endpoint serves it.

| Metric | Source events | Computation | Collection / field | Endpoint |
| --- | --- | --- | --- | --- |
| Total Customers | `identity.user.registered` | Counter, filtered on `role=customer` | `counters._id = total_customers` | `GET /analytics/overview` |
| Total Merchants | `identity.user.registered` | Counter, filtered on `role=merchant` | `counters._id = total_merchants` | `GET /analytics/overview` |
| Total Products Published | `catalog.product.published`, `…unpublished` | Counter, incremented and decremented | `counters._id = total_products` | `GET /analytics/overview` |
| Orders Over Time | `order.created`, `order.confirmed` | Daily buckets, both counts, so conversion is derivable | `daily_metrics.orders_created`, `.orders_confirmed` (+ `.orders_failed`) | `GET /analytics/orders` |
| Revenue Trends | `order.confirmed` | Daily bucket sum of `total`, per currency | `daily_metrics.revenue`, `.currency` | `GET /analytics/revenue` |
| Most Popular Products | `order.confirmed` line items | Per-product quantity and revenue counters; ranked on read | `product_stats` | `GET /analytics/products/popular` |
| Average Order Value | `order.confirmed` | Bucket revenue ÷ bucket confirmed-order count | `daily_metrics.revenue` ÷ `.orders_confirmed` | `GET /analytics/revenue` |
| Inventory Stockout Rate | `inventory.stockout`, `order.created` | Stockout events ÷ order attempts in the window | `daily_metrics` (`metric = stockouts`, ANL-5) ÷ `daily_metrics.orders_created` | `GET /analytics/inventory/stockouts` |
| Failed Events / Workflow Issues | `order.failed`, DLQ arrivals, workflow failure events | Counter by `failure_reason` and by source | `failure_stats` | `GET /analytics/failures` |
| AI Assistant Usage | `assistant.interaction` | Counter plus daily message and token volume | `counters.assistant_interactions`; `daily_metrics.assistant_messages`, `.assistant_tokens`, `.assistant_cost` | `GET /analytics/assistant` |
| Questions Asked / Answered | `assistant.interaction` | Asked, answered and refused counted **separately**, so the ratio is meaningful; refusals never count as answers (Q11, archive 0005) | `counters.assistant_questions_asked` (ANL-6), `.assistant_questions_answered`, `.assistant_refusals` | `GET /analytics/assistant` |
| Conversion After AI Interaction | `assistant.interaction`, `order.confirmed` | Any `order.confirmed` within 24 hours of an `assistant.interaction` by the same user, reported alongside the non-interacting baseline. Correlation, not causation — labelled as such (Q3, archive 0005) | `assistant_conversions` | `GET /analytics/assistant/conversion` |
| Average AI Response Latency | `assistant.interaction` | Mean and p95 of `latency_ms` per day, computed by the reconciliation job (ANL-7) | `daily_metrics.assistant_avg_latency_ms`, `.assistant_p95_latency_ms` | `GET /analytics/assistant` |

The definitions are stated explicitly so they can be challenged — particularly stockout rate, where "how often items are unavailable when wanted" admits several readings (Q10, archive 0003). They are raised again in §3.3.1.

**Definitions that sit under the table.**

- **Bucket date** is the UTC calendar date of the envelope's `occurred_at`, never of processing time (ANL-3). A replayed or late event lands in the same document it would have landed in live.
- **Week and month** are composed on read from daily buckets: ISO weeks (Monday start, UTC) and calendar months (UTC).
- **Platform revenue** is the sum of the order's `total` (including `shipping_fee`). **Merchant revenue** is the sum of that merchant's `line_total`s in the order. The sum of merchant revenues is therefore not the platform revenue; the difference is shipping, and the API labels which is which.
- **A merchant's order count** is the number of orders containing at least one of the merchant's lines. An order with lines from two merchants counts once for each and once for the platform.
- **Average order value over a window** is `Σ revenue ÷ Σ orders_confirmed` over the window's buckets — never the average of daily averages.
- **Order attempts** for stockout rate are `orders_created`, which includes orders that later fail.
- **Questions asked** are customer chat interactions (`kind = chat`); merchant generation interactions (`kind = generation`) count towards usage only.

**Vocabularies** (`DomainEnum`, [S4](0000-architecture.md#3131-schema-conventions-s1s15)). S4's database mechanics are Postgres-specific; in `analytics` the same `StrEnum` classes validate request parameters and the string values written into documents.

| Vocabulary | Values | Used by |
| --- | --- | --- |
| `CounterName` | `total_customers`, `total_merchants`, `total_products`, `orders_total`, `revenue_total`, `assistant_interactions`, `assistant_questions_asked` (ANL-6), `assistant_questions_answered`, `assistant_refusals` | `counters._id` |
| `MetricKey` | `orders.<ISO4217>` (commerce buckets, ANL-4), `stockouts` (ANL-5), `assistant` | `daily_metrics._id.metric` |
| `Granularity` | `day`, `week`, `month` | `granularity` query parameter |
| `PopularRankBy` | `quantity`, `revenue` | `rank_by` query parameter |
| `FailureSource` | `order`, `payment`, `dlq`, `workflow` | `failure_stats._id.source` |
| `InteractionKind` | `chat`, `generation` | read from `assistant.interaction` payload |

**Invariants.**

1. Each `(group, event_id)` is applied at most once and, once its offset is committed, at least once (ANL-1).
2. Every monetary value is `Decimal128` with an ISO 4217 currency (S2); no document mixes currencies (ANL-4).
3. A merchant-scoped document's figures derive only from lines whose snapshot `merchant_id` equals the scope.
4. A document's bucket is a function of the event alone (`occurred_at`, payload), never of when it was processed (ANL-3).
5. No rollup is ever the only copy of the facts it summarises; the source service holds the truth, and `raw_events` holds the last 7 days of envelopes.

##### 3.1.1.3. State machines

`analytics` has no status-driven entity except one derived lifecycle, `assistant_conversions`. It has no stored status field; state is a function of `converted` and time.

| From | To | Trigger | Guard |
| --- | --- | --- | --- |
| — | `open` | `assistant.interaction` consumed for an authenticated user (`user_id` not null) with no `open` window already running for that user | Anonymous interactions (no `user_id`) create no document — there is no identity to correlate an order with |
| `open` | `converted` | `order.confirmed` consumed with `customer_id = user_id` and `interaction_at ≤ ordered_at ≤ interaction_at + 24h` | First matching order wins; `order_id`, `ordered_at` set, `converted = true` |
| `open` | `expired` | Time: `now > interaction_at + 24h` with no match | Not written; derived at read. `converted = false` stays |
| `expired` | `converted` | `rebuild_daily_rollups` re-correlation finds a matching `order.confirmed` in `raw_events` that arrived out of order | Only within the 48-hour re-correlation lookback (§3.1.1.4) |

A later interaction by the same user while a window is `open` does not open a second window; the window is anchored on the first interaction, so one order cannot convert two windows (ANL-8).

##### 3.1.1.4. Workflows and async jobs

`analytics` runs no Temporal workflow. It owns three kinds of asynchronous work: the live consumer, the scheduled reconciliation task, and the backfill CLI.

###### The consumer — handler logic

One process per replica, one consumer group `analytics`, subscribed to every catalogue topic (Q2, archive 0003: per service, not per topic; fewer containers, and ordering is per key anyway). The base class from `common` supplies decode, trace restoration, retry and DLQ; `analytics` supplies the dedupe implementation for Mongo and the handlers.

```
on message(envelope):
  restore traceparent (C11) → span "analytics.consume {topic}"
  if envelope.occurred_at < now − (PROCESSED_EVENTS_TTL_DAYS − 1)d:      (ANL-2)
      metric outcome="stale"; commit offset; return
  ops = services.rollups.apply(envelope)       # pure: envelope → Mongo update ops
  with mongo.start_session() as s:
      s.with_transaction(lambda s:
          processed_events.insert_one({_id:{group:"analytics", event_id}, processed_at:now}, session=s)
              # DuplicateKeyError → abort txn → outcome="duplicate"
          raw_events.insert_one(envelope, session=s)
          for op in ops: op.execute(session=s)   # $inc / $set / $max / upsert
      , write_concern=majority, read_concern=snapshot)
  commit Kafka offset
  on transient error: base class retries ×3 with backoff (C6), then → <topic>.dlq, commit offset
```

**How the dedupe and the `$inc` are made atomic (ANL-1).** C4 and S13 require the dedupe record to be written "in the same transaction as the effect". MongoDB supports multi-document transactions only on a replica set, so `analytics_db` runs as a **single-node replica set** (`rs0`, one member) in Compose ([RFC 0001](0001-platform-and-shared-infrastructure.md) hosts it). Inside one transaction:

1. The `processed_events` insert is first. The `_id` is `{group, event_id}`, so a duplicate raises `DuplicateKeyError` immediately and the transaction aborts with no effect. Two replicas racing on the same event after a rebalance collide on the same `_id`: one commits, the other receives a write conflict, retries, and then sees the duplicate.
2. `raw_events` is inserted (unique on `event_id`).
3. The rollup operations run — `$inc` on counters and buckets, `$max` on `last_sold_at`, `$set` on snapshots, `upsert: true` everywhere.
4. Commit. Only after a successful commit is the Kafka offset committed.

| Crash point | What happens on restart | Result |
| --- | --- | --- |
| Before commit | Transaction never committed; offset not committed; event redelivered | Applied once |
| After commit, before offset commit | Event redelivered; `processed_events` insert raises duplicate; offset committed | Applied once |
| After offset commit | Nothing to redo | Applied once |

**Why it cannot be done without a transaction**, and what the fallback is. MongoDB's single-document atomicity is not enough: an event touches up to five documents in three collections (a platform bucket, one bucket per merchant, a product document per line, a counter, the dedupe record). Writing the dedupe record first and the effect second gives at-most-once (a crash between them loses the increment); the reverse gives at-least-once (a crash between them doubles it). `$inc` is not idempotent, so neither order is correct on its own. If the replica-set configuration is ever unavailable (for example, a reviewer's machine running a stand-alone `mongod`), the consumer refuses to start rather than silently degrading — `ANALYTICS_REQUIRE_TRANSACTIONS=true` by default. Setting it to `false` selects **dedupe-first, at-most-once** ordering, and the drift it can cause is bounded to what `rebuild_daily_rollups` repairs (buckets within the `raw_events` window); counters are not repaired. That mode exists for local experimentation only and is logged as a warning at every start.

**Why the dedupe window is safe (ANL-2).** `processed_events` expires at 30 days; Kafka retains 7 days. Any redelivery or offset reset on a delete-retention topic is therefore always inside the dedupe window. The exception is the two **compacted** topics, `identity.merchant.status_changed` and `catalog.product.published`, which retain the latest event per key indefinitely: resetting offsets to earliest on those after 30 days would re-deliver events whose dedupe record has expired, and `total_products` would be incremented a second time. So an event whose `occurred_at` is older than `PROCESSED_EVENTS_TTL_DAYS − 1` days is **not applied to rollups** by the live consumer; it is counted as `outcome="stale"` and skipped. Rebuilding state from compacted history is the bootstrap CLI's job, which *recomputes* rather than increments (below).

**Per-topic effects.** The exact update each handler emits is in [§3.1.2.4](#3124-events-consumed).

###### `rebuild_daily_rollups` — the scheduled reconciliation job (Q8, archive 0000)

Q8 (archive 0000) asked whether rollups are computed on ingest, on read, or both. The answer carried forward: **on ingest for counters, on read for time-window queries, with a scheduled reconciliation job.** The job is:

| Property | Value |
| --- | --- |
| Task | `rebuild_daily_rollups` |
| Queue | `analytics` |
| Schedule | Celery Beat, hourly |
| Host | `celery-worker` ([RFC 0001](0001-platform-and-shared-infrastructure.md)); logic in `services/analytics/app/services/reconcile.py` |
| Credential | `analytics`' Mongo user (R1, R3) |
| Idempotent | Yes — it replaces documents with recomputed values; running it twice changes nothing |
| Safe on two workers | Yes — each bucket is replaced by an `$set` of the full recomputed document keyed by `_id`; concurrent runs converge on the same value |

What it does, in order:

1. **Re-derive commerce and stockout buckets.** For each UTC date `D` in `[today − RECONCILE_LOOKBACK_DAYS, today]` — the days fully covered by `raw_events`' 7-day TTL — run the same `services.rollups` functions over `raw_events` for that date, aggregate in memory, and compare with the stored `daily_metrics` and `failure_stats` documents. A difference is corrected with a full `$set` of the recomputed fields and counted in `analytics_reconcile_drift_total{collection}`. Drift should be zero; a non-zero value is a bug signal, not a routine correction.
2. **Compute assistant latency statistics (ANL-7).** `assistant_avg_latency_ms` and `assistant_p95_latency_ms` cannot be maintained with `$inc` — a mean needs a sum and count, a p95 needs the distribution. The job computes both per `(date, merchant scope)` from `raw_events` (`$percentile` in MongoDB 7.0's aggregation) and `$set`s them. Days older than the lookback keep their last computed values, which are final because the day is complete.
3. **Re-correlate assistant conversions.** For interactions in the last 48 hours still `converted = false`, look up `order.confirmed` events in `raw_events` for the same user within 24 hours after `interaction_at`. This repairs the out-of-order case where an `order.confirmed` was consumed before the `assistant.interaction` that preceded it (the two topics are on different partitions and keys).
4. **Emit** `analytics_reconcile_runs_total{outcome}` and `analytics_reconcile_duration_seconds`.

The job never touches `counters` (they are all-time, and `raw_events` holds only 7 days) or `product_stats` (all-time totals, same reason). Both are protected by ANL-1 instead of by reconciliation.

###### The backfill path from `raw_events`

`raw_events` exists so a new metric can be backfilled without replaying Kafka (Q4, archive 0003). The path:

```
make analytics-backfill PROJECTION=<name> [SINCE=<date>]
  → python -m app.cli backfill --projection <name> --since <date>
      for envelope in raw_events.find({occurred_at >= since}).sort(occurred_at):
          same transaction shape as the live consumer, but with
          processed_events _id.group = "analytics.backfill.<name>"
          and ops = the new projection's apply() only
```

- The distinct `group` keeps the backfill's dedupe records separate from the live consumer's, so a backfill can be re-run after a crash and applies each event once, and the live consumer's effects are never repeated.
- Once the projection's handler is also deployed in the live consumer, events arriving after the backfill's start are applied by the live consumer; the backfill stops at the deploy timestamp to avoid double application (the CLI takes `--until`, defaulting to "now at start").
- **Reach is 7 days.** Beyond `raw_events`' TTL there is no backfill source. Kafka's retention is also 7 days, so the two escape hatches have the same horizon; this is the stated cost of Q4's bounded storage.

###### Bootstrapping from compacted topics

`python -m app.cli bootstrap-compacted` recomputes `total_products` from the latest event per key on `catalog.product.published` / `.unpublished` (count of products whose latest event is `published`) and `$set`s the counter. It is the only correct way to seed a fresh `analytics_db` against a platform that has been running for longer than 30 days, and the only place compacted history is read from offset zero.

##### 3.1.1.5. Dependencies and degradation

**Synchronous calls in:** none from any service. Only the gateway routes admin and merchant HTTP reads to it.
**Synchronous calls out:** none. JWT verification uses `identity`'s JWKS, fetched and cached; no call on the request path (A4).

| Dependency | Needed for | Behaviour when it is down |
| --- | --- | --- |
| MongoDB (`analytics_db`) | Ingest and reads | Consumer pauses: transactions fail, no offset is committed, lag grows, events wait in Kafka. Reads return `503` problem details. `/health/ready` fails. **Checkout is unaffected** |
| Kafka + Schema Registry | Ingest | Nothing to consume; reads continue from existing data (R7). Producers' outbox rows accumulate in Postgres ([RFC 0001](0001-platform-and-shared-infrastructure.md)); nothing is lost |
| RabbitMQ / Celery Beat | `rebuild_daily_rollups` | Reconciliation skipped. Ingest and reads unaffected; latency statistics (ANL-7) and out-of-order conversions go stale until the next run |
| `identity` JWKS | Token verification | Cached key keeps verifying; a key rotation during an outage fails closed with `401` |
| OTel Collector | Traces, metrics export | Telemetry dropped; service unaffected |
| Any producer being down | Its topic's metrics | Its metrics stop moving; everything else continues |

**It must never affect checkout.** The guarantee is structural rather than tuned:

1. Nothing on the order path calls `analytics` — the order workflow's only analytics seam is an outbox row ([RFC 0005](0005-order-service.md)).
2. `analytics` calls nothing, so it cannot hold a lock, a connection or a thread in another service.
3. It uses a different store, so it cannot contend for the Postgres connection pool or lock manager that checkout uses ([archive 0000](archive/0000-system-architecture.md) §3.2, "Why analytics gets a different database").
4. Kafka is the buffer: when `analytics` is slow or down, the cost is consumer lag, not backpressure onto a producer.

ANL-G5 asserts it by stopping the service and its database during a checkout E2E.

##### 3.1.1.6. Configuration

Settings are Pydantic settings from environment variables. Values marked *(ANL)* are defaults proposed here; the rest are fixed by the sources.

| Variable | Default | Meaning |
| --- | --- | --- |
| `ANALYTICS_PORT` | `8006` | HTTP port |
| `ANALYTICS_MONGO_URI` | `mongodb://analytics:<secret>@mongo:27017/analytics_db?replicaSet=rs0` | Single-node replica set (ANL-1); credential from `.env` |
| `ANALYTICS_MONGO_DB` | `analytics_db` | Database name |
| `ANALYTICS_REQUIRE_TRANSACTIONS` | `true` *(ANL)* | Refuse to start without replica-set transactions (ANL-1) |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Broker ([RFC 0001](0001-platform-and-shared-infrastructure.md)) |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` | Avro schemas |
| `ANALYTICS_CONSUMER_GROUP` | `analytics` | Kafka group id and `processed_events._id.group` |
| `ANALYTICS_TOPICS` | every catalogue topic | Subscription list |
| `ANALYTICS_DLQ_PATTERN` | `^smartretail\..*\.dlq$` *(ANL)* | DLQ observer subscription (ANL-9) |
| `CONSUMER_MAX_ATTEMPTS` | `3` | In-process attempts before DLQ (C6) |
| `CONSUMER_BACKOFF_BASE_MS` | `200` *(ANL)* | Exponential backoff base |
| `RAW_EVENTS_TTL_DAYS` | `7` | `raw_events` TTL (Q4, archive 0003) |
| `PROCESSED_EVENTS_TTL_DAYS` | `30` | `processed_events` TTL |
| `RECONCILE_LOOKBACK_DAYS` | `6` *(ANL)* | Days re-derived by `rebuild_daily_rollups`; must be `< RAW_EVENTS_TTL_DAYS` |
| `RECONCILE_SCHEDULE` | hourly | Beat schedule for `rebuild_daily_rollups` |
| `CONVERSION_WINDOW_HOURS` | `24` | Q3, archive 0005 |
| `CONVERSION_RECORRELATE_HOURS` | `48` *(ANL)* | Lookback for out-of-order conversion repair |
| `ANALYTICS_REPORTING_CURRENCY` | `USD` *(ANL)* | Currency of `counters.revenue_total` and `product_stats.revenue` (ANL-4) |
| `ANALYTICS_RAW_WINDOW_MAX_DAYS` | `7` *(ANL)* | Longest `from`/`to` window answerable from `raw_events` for windowed popularity |
| `API_LIMIT_MAX` | `100` | Hard `limit` ceiling ([API conventions](0000-architecture.md#3122-api-conventions)) |
| `JWKS_URL` | `http://identity:8001/api/v1/auth/jwks` | Public key source (A4) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4317` | Telemetry (C10) |
| `OTEL_SERVICE_NAME` | `analytics` | |

##### 3.1.1.7. Observability

**Metrics** (Prometheus, via the OTel Collector, exposed on `/metrics`).

| Name | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `kafka_consumer_lag` | gauge | `group="analytics"`, `topic`, `partition` | **Consumer lag** — high-water mark minus committed offset. Emitted by the `common` consumer base ([RFC 0001](0001-platform-and-shared-infrastructure.md)); dashboarded, not alerted (Q7, archive 0003) |
| `analytics_events_ingested_total` | counter | `topic`, `outcome = applied \| duplicate \| stale \| dlq` | Every consumed event's fate. `duplicate` should be rare and rises after a rebalance or replay |
| `analytics_ingest_duration_seconds` | histogram | `topic` | Decode to commit |
| `analytics_txn_retries_total` | counter | `reason = write_conflict \| transient` | MongoDB transaction retries |
| `analytics_freshness_seconds` | gauge | `topic` | `now − occurred_at` of the last applied event — lag expressed in time, which is what "a dashboard lags a checkout by seconds" means |
| `analytics_dlq_observed_total` | counter | `source_topic`, `reason` | DLQ arrivals seen by the observer (ANL-9) |
| `analytics_reconcile_runs_total` | counter | `outcome = ok \| failed` | `rebuild_daily_rollups` runs |
| `analytics_reconcile_duration_seconds` | histogram | — | Run time |
| `analytics_reconcile_drift_total` | counter | `collection` | Buckets the job had to correct. Expected zero |
| `analytics_counter` | gauge | `name` | Current value of each `counters` document, refreshed every 30s; feeds the business-metrics dashboard |
| `http_server_*` (RED) | — | `route`, `method`, `status` | Standard RED set from `common` |

**Spans.** `analytics.consume {topic}` (child of the producer's span via the restored `traceparent`, C11), `analytics.apply` with `event_type` and op count, MongoDB driver spans (PyMongo instrumentation), `analytics.reconcile` per run and per date, and FastAPI request spans for reads.

**Log fields** (structured JSON, every line): `trace_id`, `span_id`, `event_id`, `event_type`, `topic`, `partition`, `offset`, `consumer_group`, `outcome`; on reads `route`, `role`, `merchant_scope` (the id, never figures).

**Dashboards** (provisioned as code, [RFC 0001](0001-platform-and-shared-infrastructure.md)): the *event pipeline health* dashboard carries the analytics consumer-lag panel, the outcome breakdown, DLQ arrivals and freshness; the *business metrics* dashboard (archive 0003 T11) carries the `analytics_counter` gauges and links to the analytics API for series.

**Alerts.** None (Q7, archive 0003).

**Diagnosis walkthrough.** "A metric did not move": `analytics_events_ingested_total{topic}` → was the event consumed at all, and with which outcome → `processed_events` for its `event_id` → `raw_events` for the envelope → the DLQ observer's count for that topic → the trace of the producing request.

#### 3.1.2. API changes

##### 3.1.2.1. Endpoints

All routes are under `/api/v1`, reached through the gateway at `:8000`, bearer-authenticated, read-only, and return RFC 9457 problem details on error (A7). None creates business state, so none takes an `Idempotency-Key`, and none returns `202`. Dates are ISO 8601 UTC; `from` is inclusive and `to` is exclusive; defaults are the last 30 days. Every response carries `as_of` — the `occurred_at` of the newest event applied — so a reader knows how fresh the figure is.

**Common parameters and errors.**

| Parameter | Type | Rule |
| --- | --- | --- |
| `from`, `to` | date | `from < to`; window ≤ 366 days; else `422` |
| `granularity` | `day` \| `week` \| `month` | default `day` |
| `merchant_id` | uuid | **admin only**, optional — narrows an admin query to one merchant. A merchant supplying it gets `422` if it differs from their own claim, never a silent override |
| `currency` | ISO 4217 | optional filter on commerce series; default: all currencies, reported separately |
| `limit` | int | 1–100, default 10 |

| Status | When |
| --- | --- |
| `200` | Success |
| `401` | Missing, invalid or expired token |
| `403` | Role not permitted on this route (§3.1.2.2) |
| `422` | Invalid parameters (`violations[]`) |
| `503` | MongoDB unavailable — the only dependency a read has |

---

**`GET /api/v1/analytics/overview`** — every counter in one payload; the dashboard's home. `admin`.

```json
{
  "as_of": "2026-09-29T10:14:03Z",
  "total_customers": 1204, "total_merchants": 37, "total_products": 812,
  "orders_total": 5310,
  "revenue_total": {"amount": "412330.50", "currency": "USD"},
  "assistant_interactions": 9120, "assistant_questions_asked": 7004,
  "assistant_questions_answered": 6120, "assistant_refusals": 311
}
```

Answers: Total Customers, Total Merchants, Total Products Published.

---

**`GET /api/v1/analytics/orders`** — orders over time. `admin`, `merchant`. `from`, `to`, `granularity ∈ {day, week, month}`. Merchant scope applied from the token.

```json
{
  "as_of": "…", "scope": {"merchant_id": null}, "granularity": "day",
  "series": [
    {"period": "2026-09-28", "orders_created": 212, "orders_confirmed": 190,
     "orders_failed": 14, "confirmation_rate": "0.8962"}
  ]
}
```

`confirmation_rate = orders_confirmed ÷ orders_created` — "both counts, so conversion is derivable". Answers: Orders Over Time.

---

**`GET /api/v1/analytics/revenue`** — revenue trends and average order value, same parameters as `/orders` plus `currency`.

```json
{
  "as_of": "…", "scope": {"merchant_id": "…"}, "basis": "merchant_line_total",
  "series": [
    {"period": "2026-W39", "currency": "USD", "revenue": "10412.00",
     "orders_confirmed": 81, "average_order_value": "128.54"}
  ],
  "window": {"currency": "USD", "revenue": "…", "orders_confirmed": 0, "average_order_value": "…"}
}
```

`basis` is `order_total` for the platform scope and `merchant_line_total` for a merchant scope (§3.1.1.2). Money is serialised as a string, never a JSON number. Answers: Revenue Trends, Average Order Value.

---

**`GET /api/v1/analytics/products/popular`** — ranked by quantity or revenue. `admin`, `merchant`. `rank_by ∈ {quantity, revenue}` (default `quantity`), `limit`, `from`, `to`.

- **No `from`/`to`** → all-time ranking from `product_stats`, using the `{merchant_id, quantity_sold: -1}` index for a merchant, a collection sort for admin.
- **`from` within the last `ANALYTICS_RAW_WINDOW_MAX_DAYS`** → the ranking is computed on read by aggregating `order.confirmed` line items in `raw_events` over the window (`$unwind` lines, `$match` scope, `$group` by `product_id`, `$sort`, `$limit`).
- **`from` older than that** → `422` with a detail naming the retention bound. `product_stats` holds all-time totals only, so an arbitrary historical window has no source (Q-ANL-3).

```json
{"as_of": "…", "rank_by": "quantity", "window": null,
 "items": [{"product_id": "…", "name": "Trail Runner 2", "merchant_id": "…",
            "quantity_sold": 431, "revenue": {"amount": "38790.00", "currency": "USD"},
            "order_count": 402, "last_sold_at": "…"}]}
```

Answers: Most Popular Products.

---

**`GET /api/v1/analytics/inventory/stockouts`** — stockout rate and the offending variants. `admin`, `merchant`. `from`, `to`, `granularity`, `limit` (variants returned).

```json
{"as_of": "…", "scope": {"merchant_id": null},
 "series": [{"period": "2026-09-28", "stockouts": 9, "order_attempts": 212, "stockout_rate": "0.0425"}],
 "window": {"stockouts": 31, "order_attempts": 1480, "stockout_rate": "0.0209"},
 "top_variants": [{"variant_id": "…", "product_id": "…", "stockouts": 6}]}
```

`stockout_rate = Σ stockouts ÷ Σ orders_created` over the window (Q10, archive 0003). Answers: Inventory Stockout Rate.

---

**`GET /api/v1/analytics/failures`** — failed events and workflow issues, grouped by reason. `admin`. `from`, `to`, `group_by ∈ {reason, source}` (default both).

```json
{"as_of": "…", "items": [{"source": "order", "reason": "payment_declined", "count": 14},
                         {"source": "dlq", "reason": "smartretail.order.confirmed:SchemaError", "count": 1}]}
```

Answers: Failed Events / Workflow Issues.

---

**`GET /api/v1/analytics/assistant`** — usage volume, questions asked versus answered, average latency. `admin`. `from`, `to`, `granularity`.

```json
{"as_of": "…",
 "totals": {"interactions": 9120, "questions_asked": 7004, "questions_answered": 6120,
            "refusals": 311, "answer_rate": "0.8738"},
 "series": [{"period": "2026-09-28", "assistant_messages": 402, "assistant_tokens": 611204,
             "assistant_cost": "4.812300", "assistant_avg_latency_ms": 3120,
             "assistant_p95_latency_ms": 8804}]}
```

`answer_rate = questions_answered ÷ questions_asked`; refusals are reported beside it and never inside it (Q11, archive 0005). Latency figures for today and the lookback days are as of the last reconciliation run (ANL-7). Answers: AI Assistant Usage, Questions Asked / Answered, Average AI Response Latency.

---

**`GET /api/v1/analytics/assistant/conversion`** — orders placed within 24 h of an assistant interaction, against the baseline. `admin`. `from`, `to` (by `interaction_at`).

```json
{"as_of": "…", "label": "correlational — not causal",
 "window_hours": 24,
 "interacting": {"windows": 1810, "converted": 212, "conversion_rate": "0.1171"},
 "baseline":    {"orders_confirmed": 5310, "orders_after_interaction": 212,
                 "orders_without_interaction": 5098, "share_after_interaction": "0.0399"}}
```

The baseline is "confirmed orders not preceded by an interaction in the prior 24 h". A true non-interacting *conversion rate* would need the number of non-interacting shoppers, which the platform does not record (no browse events); this limitation is stated on the endpoint and in Q-ANL-4. Answers: Conversion After AI Interaction.

---

**Operational endpoints** (every service, [RFC 0001](0001-platform-and-shared-infrastructure.md)).

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Liveness |
| `GET` | `/health/ready` | Readiness: MongoDB ping and replica-set primary, broker connectivity, **consumer lag** per topic |
| `GET` | `/metrics` | Prometheus exposition |
| `GET` | `/docs` | OpenAPI, aggregated by the gateway |

##### 3.1.2.2. Role matrix

| Endpoint | anonymous | customer | merchant | admin | internal |
| --- | --- | --- | --- | --- | --- |
| `GET /analytics/overview` | — | — | — | ✓ | — |
| `GET /analytics/orders` | — | — | ✓ own scope | ✓ all, or `merchant_id` | — |
| `GET /analytics/revenue` | — | — | ✓ own scope | ✓ all, or `merchant_id` | — |
| `GET /analytics/products/popular` | — | — | ✓ own scope | ✓ all, or `merchant_id` | — |
| `GET /analytics/inventory/stockouts` | — | — | ✓ own scope | ✓ all, or `merchant_id` | — |
| `GET /analytics/failures` | — | — | — | ✓ | — |
| `GET /analytics/assistant` | — | — | — | ✓ | — |
| `GET /analytics/assistant/conversion` | — | — | — | ✓ | — |
| `/health`, `/health/ready`, `/metrics` | Compose network only | | | | ✓ |

**Scoping rule.** A merchant's queries are filtered by the `merchant_id` in their verified token, **in the aggregation pipeline, not in the response** (archive 0003 §6.2). `services/scoping.py` builds the `$match` stage from the claim and every read path takes it as a mandatory argument, so a query without a scope cannot be constructed. A merchant must not be able to infer another merchant's revenue from a total, which is why merchants never see platform-scope documents, the `overview` counters, or `failures` (whose reasons can name other merchants' orders). A suspended merchant cannot obtain a token ([RFC 0003](0003-identity-service.md)), so suspension needs no analytics-side check.

**The gateway's coarse check.** [Archive 0000](archive/0000-system-architecture.md) §3.1.1 gives "`/api/v1/analytics/*` is `admin`-only" as the gateway's example, while archive 0003 grants `merchant` on four routes. This RFC resolves it: the gateway's route table allows `admin` on all of `/api/v1/analytics/*` and `admin` or `merchant` on the four merchant-scoped routes above; every other role is rejected early. The gateway remains an optimisation (A5) — `analytics` re-verifies the token and applies the matrix itself. The corresponding row belongs in [RFC 0002](0002-gateway-service.md#3122-role-matrix).

##### 3.1.2.3. Events produced

**None.** `analytics` has no `outbox` and produces no event. A metric crossing a threshold is not a domain fact, and nothing downstream needs to react to it.

##### 3.1.2.4. Events consumed

Consumer group `analytics`, every topic in the [event catalogue](0000-architecture.md#3123-event-catalogue); envelope per [RFC 0000](0000-architecture.md#3124-event-envelope). **Dedupe** for every row: `processed_events` (Mongo variant) keyed `{group: "analytics", event_id}`, in the same transaction as the effect (ANL-1). **Every** consumed event is also inserted into `raw_events`. Retry and DLQ per C6.

Payload schemas are owned by the producer. The "Fields read" column is the contract `analytics` depends on (ANL-10); a producer removing one is a breaking change for this consumer, and the Avro `BACKWARD` rule (C5) is what stops it silently.

| Topic | Producer | Fields read (payload) | Effect |
| --- | --- | --- | --- |
| `identity.user.registered` | `identity` | `user_id`, `role` | `role=customer` → `counters.total_customers` `$inc 1`. `role=merchant` → `counters.total_merchants` `$inc 1`. `role=admin` → no counter |
| `identity.merchant.status_changed` | `identity` | `user_id`, `from_status`, `to_status` | `raw_events` only. No rollup — "Total Merchants" is defined on registration (Q-ANL-1). Compacted topic; ANL-2 applies |
| `catalog.product.published` | `catalog` | `product_id`, `merchant_id` | `counters.total_products` `$inc 1`. Compacted topic; ANL-2 applies |
| `catalog.product.updated` | `catalog` | `product_id` | `raw_events` only. `product_stats.name` is a snapshot from order lines (A14) and is not refreshed from catalog |
| `catalog.product.unpublished` | `catalog` | `product_id`, `merchant_id` | `counters.total_products` `$inc -1` |
| `order.created` | `order` | `order_id`, `customer_id`, `currency`, `lines[].merchant_id` | `daily_metrics` `{D, orders.<cur>, null}` `orders_created` `$inc 1`; and for each distinct `merchant_id` in lines, `{D, orders.<cur>, merchant_id}` `orders_created` `$inc 1`. These are the stockout-rate denominator |
| `order.confirmed` | `order` | `order_id`, `customer_id`, `currency`, `total`, `lines[]{product_id, variant_id, merchant_id, product_name, quantity, line_total}` | `counters.orders_total` `$inc 1`; `counters.revenue_total` `$inc total` (reporting currency only, ANL-4). Platform bucket: `orders_confirmed` `$inc 1`, `revenue` `$inc total`, `currency` `$set`. Per merchant bucket: `orders_confirmed` `$inc 1`, `revenue` `$inc Σ line_total`. Per product line: `product_stats` upsert — `quantity_sold` `$inc`, `revenue` `$inc line_total`, `order_count` `$inc 1` (once per product per order), `last_sold_at` `$max`, `name`/`merchant_id` `$set`. Then conversion: the customer's `open` `assistant_conversions` window with `interaction_at ∈ [occurred_at − 24h, occurred_at]` → `converted=true`, `order_id`, `ordered_at` |
| `order.failed` | `order` | `order_id`, `currency`, `failure_reason`, `lines[].merchant_id` | `daily_metrics` platform and per-merchant `orders_failed` `$inc 1`; `failure_stats` `{D, "order", failure_reason}` `count` `$inc 1` (a workflow failure: the OrderWorkflow ended in compensation) |
| `order.cancelled` | `order` | `order_id` | `raw_events` only (Q-ANL-2: gross versus net revenue) |
| `payment.succeeded` | `order` | `order_id` | `raw_events` only |
| `payment.failed` | `order` | `order_id`, `reason` | `failure_stats` `{D, "payment", reason}` `$inc 1` |
| `shipment.status_changed` | `order` | `order_id`, `status` | `raw_events` only |
| `inventory.reserved` | `inventory` | `variant_id` | `raw_events` only |
| `inventory.released` | `inventory` | `variant_id` | `raw_events` only |
| `inventory.stockout` | `inventory` | `variant_id`, `product_id`, `merchant_id` | `daily_metrics` `{D, stockouts, null}` and `{D, stockouts, merchant_id}`: `stockouts` `$inc 1`, `variants.<variant_id>` `$inc 1` (ANL-5) |
| `assistant.interaction` | `assistant` | `conversation_id`, `user_id` (nullable), `kind`, `finish_reason`, `prompt_tokens`, `completion_tokens`, `cost_usd`, `latency_ms`, `merchant_id` (for `kind=generation`) | `counters.assistant_interactions` `$inc 1`. If `kind=chat`: `assistant_questions_asked` `$inc 1`; `finish_reason=refused` → `assistant_refusals` `$inc 1`; `finish_reason=stop` → `assistant_questions_answered` `$inc 1`; other reasons (`length`, `error`, `budget_exceeded`) count as asked only. `daily_metrics` `{D, assistant, null}` (and `{D, assistant, merchant_id}` for generation): `assistant_messages` `$inc 1`, `assistant_tokens` `$inc prompt+completion`, `assistant_cost` `$inc cost_usd`. If `user_id` not null and no `open` window for the user → insert `assistant_conversions` |
| `^smartretail\..*\.dlq$` (ANL-9) | consumer base classes | Kafka headers only: original topic, consumer group, error class | Separate group `analytics-dlq-observer`. `failure_stats` `{D, "dlq", "<original_topic>:<error_class>"}` `$inc 1`. The payload is never deserialised — it is poison by definition |

Handler failures on the DLQ observer are logged and skipped, never routed to a DLQ of their own, so a DLQ can never feed itself.

**Workflow failure events.** Archive 0003 lists "workflow failure events" among the failure sources. The only workflow failure in the catalogue is `order.failed`. A failed `ProductPublishingWorkflow` is recorded in `catalog`'s `product_publish_failures` table but produces no event, so `analytics` cannot see it without breaking C8. The `workflow` value of `FailureSource` is reserved for it; see Q-ANL-5.

#### 3.1.3. Data/schema changes

###### `analytics_db`

Owned by `analytics` · **MongoDB 7.x**, single-node replica set `rs0` (ANL-1) · accessed with Motor. The one genuinely schemaless workload in the platform, and the deliberate break from S8: per-metric document shapes vary, which is what justifies the NoSQL requirement ([Part A](../part-a.md) tech stack, "NoSQL DB") with a real workload rather than a token one.

`analytics` is a **pure consumer**. It never calls another service, and its failure is invisible to checkout.

| Collection | Purpose |
| --- | --- |
| [`counters`](#counters) | Monotonic totals |
| [`daily_metrics`](#daily_metrics) | Time series per day per metric per scope |
| [`product_stats`](#product_stats) | Popularity and revenue ranking |
| [`failure_stats`](#failure_stats) | Failed events and workflow issues |
| [`raw_events`](#raw_events) | Backfill escape hatch, TTL 7d |
| [`assistant_conversions`](#assistant_conversions) | Assistant → order correlation |
| [`processed_events`](#processed_events-mongo-variant) | S13, TTL 30d |

**Markers**, as defined in [archive 0006](archive/0006-service-decomposition-and-data-model.md) §3.1.3: *(none)* — stated explicitly in the source RFC; `†` — the field **is named** in a source RFC but its **type was not stated** there, and the type shown is inferred from S1–S15; `‡` — the field is **not named in any source RFC** and exists because a convention requires it. Both markers are proposals for review. Fields and indexes added by this RFC are listed separately under each collection as **ANL additions**, so the copied definitions stay exactly as consolidated.

**Shared tables carried.** `processed_events` in its **Mongo variant**, defined below. The Postgres DDL in [RFC 0000](0000-architecture.md#3132-shared-tables--outbox-and-processed_events) does not apply to `analytics_db`. No `outbox` — `analytics` produces nothing.

###### `counters`

`{_id: "<counter_name>", value, updated_at}` — monotonic per event (the invariant that makes `analytics` a service).

Counters: `total_customers`, `total_merchants`, `total_products`, `orders_total`, `revenue_total`, and the assistant counters `assistant_interactions`, `assistant_questions_answered`, `assistant_refusals`.

Index: `_id` only. **ANL additions:** counter `assistant_questions_asked` (ANL-6), because "questions asked" is not derivable from the other three once errors and budget refusals are excluded from both answered and refused. `value` is `Int64` for counts and `Decimal128` for `revenue_total`, which additionally carries `currency` (the reporting currency, ANL-4).

###### `daily_metrics`

`{_id: {date, metric, merchant_id}, …}` — one document per day per metric per scope. The source lists the value fields with an ellipsis; the set below is the full list as used by the commerce and assistant read APIs.

| Field | Type | Notes |
| --- | --- | --- |
| `_id.date` | `date` | |
| `_id.metric` | `string` | |
| `_id.merchant_id` | `uuid` \| `null` | null is the platform-wide scope |
| `orders_created`, `orders_confirmed`, `orders_failed` | `int` † | |
| `revenue` | `decimal` † | S2 — `Decimal128`, never a float |
| `currency` | `string` † | ISO 4217 (S2) |
| `assistant_messages`, `assistant_tokens` | `int` | assistant metrics |
| `assistant_cost` | `decimal` | assistant metrics |
| `assistant_avg_latency_ms`, `assistant_p95_latency_ms` | `int` | assistant metrics |

Index: `{date, metric}`.

**ANL additions.**

| Field / index | Type | Notes |
| --- | --- | --- |
| `_id.metric` values | `string` | `orders.<ISO4217>` for commerce documents (ANL-4), `stockouts` (ANL-5), `assistant`. Putting currency in the key is what makes "per currency" (archive 0003) and the one-currency-per-document rule (S2) both hold without widening `_id` |
| `stockouts` | `int` ‡ | ANL-5 — only on `metric = stockouts` |
| `variants` | `object` ‡ | ANL-5 — `{<variant_id>: int}`, only on `metric = stockouts`; the "offending variants" source |
| `updated_at` | `date` ‡ | S3 — set on every write |
| Index `{_id.merchant_id, _id.metric, _id.date}` | — | Merchant-scoped range reads; the `{date, metric}` index serves platform reads |

The source's index key is written `{date, metric}` while `_id` is `{date, metric, merchant_id}`; the source's table index row calls the key `{date, metric, scope}`. They are the same thing — `scope` is `merchant_id`, with `null` as platform scope.

###### `product_stats`

`{_id: product_id, merchant_id, name, quantity_sold, revenue, order_count, last_sold_at}`. Index: `{merchant_id, quantity_sold: -1}`.

`name` is a snapshot; `revenue` is `Decimal128` (S2).

**ANL additions:** `currency` ‡ (the reporting currency, ANL-4; lines in other currencies update `quantity_sold` and `order_count` but not `revenue`); index `{merchant_id, revenue: -1}` for `rank_by=revenue`; `updated_at` ‡ (S3). `last_sold_at` is maintained with `$max`, so an out-of-order event never moves it backwards.

###### `failure_stats`

`{_id: {date, source, reason}, count}`.

**ANL additions:** `source` takes a `FailureSource` value (`order`, `payment`, `dlq`, `workflow`); for `dlq`, `reason` is `<original_topic>:<error_class>` (ANL-9). Index `{_id.date: 1}` for window reads. `updated_at` ‡ (S3).

###### `raw_events`

The full event envelope as received, TTL 7 days. The escape hatch that lets a new metric be backfilled without replaying Kafka. The source says only "the full envelope"; that envelope is fixed in [RFC 0000](0000-architecture.md#3124-event-envelope) and is reproduced here so the collection is specified:

| Field | Type | Notes |
| --- | --- | --- |
| `event_id` | `uuid` † | unique |
| `event_type` | `string` † | |
| `event_version` | `int` † | |
| `occurred_at` | `date` † | **TTL index, 7 days** — named by the source's index note |
| `producer` | `string` † | |
| `correlation_id`, `causation_id` | `string` † | |
| `actor_id` | `uuid` \| `null` † | |
| `payload` | `object` † | the domain payload, shape per topic |

Hard-deleted by TTL rather than soft-deleted — derived, rebuildable, TTL-bounded (the S5 break).

**ANL additions.**

| Field / index | Type | Notes |
| --- | --- | --- |
| `client_id` | `string` \| `null` ‡ | The envelope gained `client_id` ([RFC 0000](0000-architecture.md#3124-event-envelope)); the consolidated field list predates it. Stored because the collection is "the full envelope as received". Never read by any metric — it is a device correlator and is not reused for analytics |
| `topic`, `partition`, `offset` | `string`, `int`, `Int64` ‡ | Kafka coordinates, for diagnosis |
| `received_at` | `date` ‡ | Processing time; distinct from `occurred_at` |
| Index `{event_type: 1, occurred_at: 1}` | — | Reconciliation and backfill scans by type and date |
| Index `{payload.customer_id: 1, event_type: 1, occurred_at: 1}` (partial: `event_type = "order.confirmed"`) | — | Conversion re-correlation (§3.1.1.4) |

TTL is on `occurred_at`, so an event replayed late with an `occurred_at` already older than 7 days is deleted by the next TTL monitor pass (MongoDB runs it every 60 s). That is correct: such an event is outside every window `raw_events` serves.

###### `assistant_conversions`

`{user_id, interaction_at, order_id?, ordered_at?, converted}` — populated by correlating `assistant.interaction` with `order.confirmed` within 24 hours.

**ANL additions:** `_id` `uuid` ‡ (the table index names `uuid` as the key; S1); `conversation_id` ‡ (the first interaction's conversation, for tracing); `created_at`, `updated_at` ‡ (S3). Indexes: `{user_id: 1, interaction_at: -1}` for "the user's open window"; partial unique `{order_id: 1}` where `order_id` exists, so one order converts at most one window (ANL-8); `{interaction_at: 1}` for window reads. No TTL — the documents are the conversion history and are small (one per user per 24 h at most).

###### `processed_events` (Mongo variant)

`{_id: {group, event_id}, processed_at}`, TTL index at 30 days on `processed_at`. Note the key is `group`, not `consumer_group` — the Postgres and Mongo shapes are deliberately not identical because the stores are not. See [RFC 0000 §3.1.3.2](0000-architecture.md#3132-shared-tables--outbox-and-processed_events).

**ANL additions:** `event_type` ‡ — matches the Postgres variant's column and makes "which topics does the dedupe hold" answerable. `group` values: `analytics` (live consumer), `analytics-dlq-observer` (ANL-9), `analytics.backfill.<projection>` (backfill). The `_id` index is the uniqueness check that makes the transaction in ANL-1 abort on a duplicate.

###### Indexes, TTLs and migrations

| Collection | Indexes | TTL |
| --- | --- | --- |
| `counters` | `_id` | — |
| `daily_metrics` | `_id`; `{date, metric}`; `{_id.merchant_id, _id.metric, _id.date}` (ANL) | — |
| `product_stats` | `_id`; `{merchant_id, quantity_sold: -1}`; `{merchant_id, revenue: -1}` (ANL) | — |
| `failure_stats` | `_id`; `{_id.date}` (ANL) | — |
| `raw_events` | unique `event_id`; TTL `occurred_at`; `{event_type, occurred_at}` (ANL); partial conversion index (ANL) | 7 days on `occurred_at` |
| `assistant_conversions` | `_id`; `{user_id, interaction_at: -1}`; partial unique `order_id`; `{interaction_at}` (ANL) | — |
| `processed_events` | `_id` (`{group, event_id}`); TTL `processed_at` | 30 days on `processed_at` |

**Migrations.** MongoDB has no Alembic, and S14's intent — independent, reversible, additive changes — is met differently:

- Indexes and TTLs are declared in `models/` and created by an idempotent `ensure_indexes()` at start-up and by `make analytics-indexes` (the "collections and indexes" step in §3.1.5). `createIndex` on an existing identical index is a no-op.
- A TTL change is `collMod` with a new `expireAfterSeconds`, never drop-and-recreate.
- Every schema change is an additive field. Readers treat a missing field as zero (`$ifNull`), so old documents need no backfill.
- Rollback is the previous image; added fields are ignored by older code, and added indexes are harmless.

| Step | Change |
| --- | --- |
| `a001_initial` | The six commerce collections (`counters`, `daily_metrics`, `product_stats`, `failure_stats`, `raw_events`, `processed_events`) with their indexes and TTLs |
| `a002_assistant` | `assistant_conversions` and its indexes; assistant counters and `daily_metrics` assistant fields (additive, no index change) |

#### 3.1.4. Breaking changes

`analytics` is a new service, so it breaks no client. Three contracts elsewhere become load-bearing because of it:

| Change | Impact | Handling |
| --- | --- | --- |
| The gateway's `/api/v1/analytics/*` rule is `admin` **or** `merchant` on four routes, not `admin`-only | A gateway implemented to the archive 0000 example would reject merchants | Resolved in §3.1.2.2; row belongs in [RFC 0002](0002-gateway-service.md#3122-role-matrix) |
| Producer payloads must carry the "Fields read" in §3.1.2.4 (ANL-10) | A producer dropping `lines[].merchant_id` or `role` silently breaks merchant scoping or customer counts | Avro `BACKWARD` compatibility (C5) plus a contract test per topic in `analytics` (§7) |
| MongoDB must run as a replica set (ANL-1) | A stand-alone `mongod` makes the consumer refuse to start | Compose configuration in [RFC 0001](0001-platform-and-shared-infrastructure.md); `rs.initiate()` in the Mongo init script |

#### 3.1.5. Action Plan

Ordered so that the dedupe-plus-effect transaction is proven on one topic before handlers are widened, and so the metrics' tests exist before their endpoints. Original archive task IDs are given for traceability.

| # | Task | Output | Depends on |
| --- | --- | --- | --- |
| ANL-T1 | MongoDB single-node replica set in Compose; `analytics` skeleton on :8006; settings; `/health`, `/health/ready`, `/metrics`; Mongo user with `readWrite` on `analytics_db` only (archive 0003 T17) | Connectivity; transactions available; R1 grant test green | Platform Kafka and Mongo ([RFC 0001](0001-platform-and-shared-infrastructure.md)) |
| ANL-T2 | `models/`: collections, indexes and TTLs via `ensure_indexes()`; `a001_initial` (archive 0003 T17) | Index creation idempotent; TTLs present | ANL-T1 |
| ANL-T3 | Mongo dedupe adapter for the `common` consumer base; the ANL-1 transaction; ANL-2 stale guard | One topic (`identity.user.registered`) end to end; crash-point tests green (ANL-G4) | ANL-T2, `common` consumer base |
| ANL-T4 | `services/rollups.py` handlers for every commerce topic with ingest-time aggregation (C9) (archive 0003 T18) | Counters and buckets move on consumption; replay test green (ANL-G3) | ANL-T3 |
| ANL-T5 | DLQ observer group (ANL-9) | DLQ arrivals appear in `failure_stats` | ANL-T3 |
| ANL-T6 | The nine [Part A](../part-a.md) §5 metrics in `services/metrics.py` and their read APIs, merchant-scoped in the pipeline (archive 0003 T19) | One test per metric (ANL-G1); merchant isolation (ANL-G7) | ANL-T4 |
| ANL-T7 | `rebuild_daily_rollups` on the `analytics` Celery queue, hourly Beat | Drift test and idempotency test green | ANL-T4, platform Celery |
| ANL-T8 | Backfill CLI and `bootstrap-compacted`; `make analytics-backfill` | Backfill equals live ingestion (ANL-G9) | ANL-T4 |
| ANL-T9 | Observability: lag, outcome, freshness, reconcile metrics; spans with restored trace context; business-metrics and pipeline panels (archive 0003 T10, T11) | Panels live; consumer spans join producer traces | ANL-T4 |
| ANL-T10 | Gateway route rule for `/api/v1/analytics/*` (§3.1.2.2) | Merchant allowed on four routes, rejected on four | ANL-T6, [RFC 0002](0002-gateway-service.md) |
| ANL-T11 | Checkout isolation test (ANL-G5) and the analytics steps of the platform E2E (archive 0003 T25) | `make test` green cold | ANL-T6 |
| ANL-T12 | `assistant.interaction` handler, assistant counters and `daily_metrics` fields, `a002_assistant`, `assistant_conversions` correlation (archive 0005 T20) | Events on the topic, consumed by analytics | ANL-T4, `assistant` outbox ([RFC 0009](0009-assistant-service.md)) |
| ANL-T13 | The four [Part B](../part-b.md) metrics and `GET /analytics/assistant`, `/assistant/conversion`; latency statistics in the reconciliation job (ANL-7) (archive 0005 T21) | One test per metric (ANL-G2) | ANL-T12, ANL-T7 |
| ANL-T14 | End-to-end: ask → order → conversion metric (archive 0005 T25) | `make test` green cold | ANL-T13 |

**Priority if time is short** (archive 0003 risk table; archive 0000 risk table): the event pipeline outranks analytics metrics, which outrank tracing and notifications. Within this service, ANL-T1–T4 and T6 are the milestone's named outcome ("analytics pipeline initialized", "basic analytics metrics"); the read APIs can be reduced to the minimum needed to show each metric, and dashboards (ANL-T9) can be thinner.

**Delivery phasing.**

| Component | Milestone |
| --- | --- |
| MongoDB replica set, `analytics` skeleton, `analytics_db` with `counters`, `daily_metrics`, `product_stats`, `failure_stats`, `raw_events`, `processed_events` (Mongo variant) (ANL-T1, T2) | M3 |
| Consumer with the ANL-1 transaction, handlers for every commerce topic, DLQ observer (ANL-T3–T5) | M3 |
| The nine Part A metrics and their read APIs, merchant scoping (ANL-T6, T10) | M3 |
| `rebuild_daily_rollups`, backfill CLI, bootstrap (ANL-T7, T8) | M3 |
| Observability and isolation tests (ANL-T9, T11) | M3 |
| Review of metric definitions (Q10, archive 0003), Mongo transaction choice (ANL-1), `†`/`‡` ratification | M4 |
| `assistant.interaction` handling, assistant counters and `daily_metrics` fields, `assistant_conversions` (ANL-T12) | M6 |
| The four Part B metrics and their endpoints, latency statistics (ANL-T13, T14) | M6 |
| Review of the Part B metric definitions (Q3, Q11 of archive 0005) | M7 |

### 3.2. Rationale

**Why a separate service with MongoDB.** Its access pattern is genuinely different from every other service: append-only writes from event consumers, aggregate reads over time windows, and per-metric documents whose shape varies — a revenue series and a stockout rate share no columns. It must also never be able to slow down a checkout, which is easiest to guarantee when it cannot contend for the same connection pool or lock manager ([archive 0000](archive/0000-system-architecture.md) §3.2). Part A requires a NoSQL store; this gives the requirement a real workload rather than a token table. Deferring the store until this workload existed was a deliberate choice ([archive 0001](archive/0001-foundation-and-core-services.md): "a technology in search of a requirement" otherwise).

**Why analytics never queries the source (C8).** If `analytics` could call `order`, a slow analytics query could contend with checkout, and an analytics outage could become a checkout outage. Consuming events makes it structurally impossible for the reporting path to affect the transaction path — which is why it is a consumer rather than a caller, and why `raw_events` exists as its own escape hatch for backfilling a metric nobody thought of yet.

**Why counters on ingest but ranking on read (C9, Q8 of archive 0000).** "Total customers" has one query shape forever, so computing it once per event and storing the number is strictly cheaper than scanning. "Most popular products" is parameterised by window, scope and ranking key, so pre-aggregating every combination is impossible; keeping per-product totals and sorting on read is the right split. Daily buckets sit between the two: pre-aggregated per day, composed on read into whatever window is asked for. The reconciliation job then covers the two things neither path can: statistics that are not additive (p95 latency) and corrections that need a second look at the raw envelopes (out-of-order conversion correlation, bucket drift).

**Why a MongoDB transaction for the dedupe (ANL-1).** At-least-once delivery makes the dedupe record a correctness mechanism, not an optimisation (S13), and it is only correct if it commits with the effect. The alternatives are all worse: per-event contribution documents summed on read abandon C9 for counters; `$addToSet` of event ids inside each rollup document grows without bound and still spans documents; making every update idempotent by `$set` of absolute values requires reading the previous value, which is a race. A single-node replica set costs one line of Compose configuration and an `rs.initiate()`.

**Why bucket by `occurred_at` (ANL-3).** Processing time moves under replay, lag and backfill; event time does not. Bucketing by `occurred_at` is what makes the replay test meaningful — the same event always lands in the same document — and what makes the reconciliation job's recomputation comparable with live ingestion.

**Why keep `raw_events` at all (Q4, archive 0003).** Yes, with a 7-day TTL. Backfilling a new metric without replaying Kafka is worth the storage, and the storage is bounded.

**Why conversion is reported with a baseline and labelled correlational (Q3, archive 0005).** A metric that flatters the feature is worse than no metric at the review. Refusals are counted separately from answers (Q11, archive 0005) for the same reason.

### 3.3. Tradeoffs

| Trade being made | The cost | Why it is acceptable |
| --- | --- | --- |
| Analytics eventually consistent | A dashboard can lag a checkout by seconds, and can disagree with an order list for that long | Correct for reporting, and the isolation it buys is the reason the service exists. `as_of` on every response makes the lag visible |
| MongoDB for analytics | A second database technology to run and learn | Required by Part A, and it has a genuine workload here rather than a token table |
| Replica set required for transactions (ANL-1) | A slightly less trivial Mongo container; the consumer refuses a stand-alone server | The alternative is a counter that is either lossy or double-counting under crash, which is the one thing the service must not be |
| One consumer process for every topic (Q2, archive 0003) | A slow handler for one topic delays the others in the same process | Fewer containers; ordering is per key anyway; handlers are small and bounded |
| All-time `product_stats`, windowed popularity only within 7 days | A popularity ranking for an arbitrary past window cannot be answered | The all-time ranking and the recent window cover the dashboard's use; the alternative is a per-product-per-day collection (Q-ANL-3) |
| `raw_events` bounded at 7 days | A metric added later can only be backfilled 7 days | Q4: bounded storage; Kafka has the same horizon |
| Counters are not rebuildable past 7 days | Losing `analytics_db` loses all-time totals except `total_products` (bootstrap from compaction) | Stated as a known issue (§4). Operationally, a backup is the answer; querying the source would break C8 |
| Latency statistics are hourly, not live (ANL-7) | Today's average latency is up to an hour old | A p95 cannot be maintained with `$inc`; an hour is adequate for a reporting metric |
| Stale-event guard on compacted topics (ANL-2) | A replay of compacted history older than 29 days does not move counters | Replay beyond the dedupe window is exactly when an increment would be wrong; bootstrap recomputes instead |
| Merchant revenue excludes shipping | Merchant revenues do not sum to platform revenue | The alternative — apportioning shipping — invents a number. `basis` in the response says which is which |

#### 3.3.1. Challenges and Open Questions

| # | Question | Current position | Needs a decision by |
| --- | --- | --- | --- |
| Q8 (archive 0000) | Does `analytics` compute rollups on ingest, on read, or both? | On ingest for counters, on read for time-window queries, with a scheduled reconciliation job (`rebuild_daily_rollups`, §3.1.1.4) | Decided |
| Q4 (archive 0003) | Should analytics keep `raw_events` at all? | Yes, 7-day TTL. Backfilling a new metric without replaying Kafka is worth the storage, and it is bounded | Decided |
| Q10 (archive 0003) | Are the metric definitions in §3.1.1.2 what the reviewer expects? | Stated explicitly so the definitions can be challenged — particularly stockout rate, where "how often items are unavailable when wanted" admits several readings | Part A review |
| Q2 (archive 0003) | One consumer process per service, or per topic? | Per service, subscribing to several topics, with the dedupe keyed by consumer group | Decided |
| Q7 (archive 0003) | Do we need consumer lag alerting? | Metrics and a dashboard panel, no alerting. There is nothing to alert into | Not required |
| Q3 (archive 0005) | How is "conversion after AI interaction" attributed? | Any `order.confirmed` within 24 hours of an `assistant.interaction` by the same user, reported alongside the non-interacting baseline. Correlation, not causation — and the metric is labelled as such | Before the assistant metrics ship (archive 0005 T21) |
| Q11 (archive 0005) | Do refusals count as "questions answered" in the Part B metric? | No. Asked, answered and refused are counted separately, so the ratio is meaningful | Before the assistant metrics ship (archive 0005 T21) |
| Q-ANL-1 | Is "Total Merchants" registrations with `role=merchant`, or approved merchants? | Registrations, per archive 0003. `identity.merchant.status_changed` is consumed and kept in `raw_events`, so an "active merchants" figure is a backfillable projection if the reviewer reads "onboarded" as "approved" | Part A review |
| Q-ANL-2 | Is revenue gross (confirmed) or net of cancellations and refunds? | Gross, per archive 0003 ("sum of `total`" on `order.confirmed`). `order.cancelled` is retained in `raw_events`; a net series is an additive `daily_metrics` field if required | Part A review |
| Q-ANL-3 | Should popularity support arbitrary historical windows? | Not now. All-time from `product_stats`, recent windows from `raw_events`. The fix, if required, is a per-product-per-day collection — a new collection, so a schema decision rather than a code change | Part A review |
| Q-ANL-4 | What is the right conversion baseline? | Share of confirmed orders not preceded by an interaction. A true non-interacting conversion rate needs a count of non-interacting shoppers, which no event carries | Part B review |
| Q-ANL-5 | Should failed product publishing appear in "Failed Events / Workflow Issues"? | It should, but no event exists — the failure is only a `catalog` table row. Proposed: `catalog` emits `catalog.product.publish_failed` via its outbox, which is a catalogue change for [RFC 0000](0000-architecture.md) and [RFC 0004](0004-catalog-service.md). Until then the `workflow` source is empty and the gap is stated on the endpoint | Part A review |
| Q-ANL-6 | Multi-currency: is one reporting currency for all-time totals acceptable (ANL-4)? | Yes for the project — the platform is not multi-currency in practice. Daily series are already per currency | Part A review |
| Q-ANL-7 | Should `raw_events` store `client_id`? | Yes, as part of "the full envelope as received", never read by a metric (§3.1.3). Flagged because the consolidated field list omitted it | Part A review |

**New decisions made in this RFC.**

| # | Decision |
| --- | --- |
| ANL-1 | `analytics_db` runs as a single-node replica set; the `processed_events` insert, the `raw_events` insert and every rollup update for one event commit in one MongoDB transaction; the Kafka offset is committed after. Without transactions the consumer refuses to start unless explicitly configured otherwise |
| ANL-2 | Events whose `occurred_at` is older than `PROCESSED_EVENTS_TTL_DAYS − 1` days are not applied to rollups by the live consumer; compacted history is recomputed by `bootstrap-compacted` |
| ANL-3 | Buckets are keyed by the UTC date of `occurred_at` |
| ANL-4 | One currency per `daily_metrics` document via `metric = orders.<ISO4217>`; all-time `revenue_total` and `product_stats.revenue` hold the configured reporting currency |
| ANL-5 | Stockouts are bucketed in `daily_metrics` under `metric = stockouts`, with a per-variant map for the offending variants |
| ANL-6 | An `assistant_questions_asked` counter exists alongside answered and refused |
| ANL-7 | Assistant average and p95 latency are computed by `rebuild_daily_rollups` from `raw_events`, not on ingest |
| ANL-8 | One conversion window per user per 24 h, anchored on the first interaction; one order converts at most one window |
| ANL-9 | A separate group, `analytics-dlq-observer`, counts DLQ arrivals from headers only and never re-DLQs |
| ANL-10 | The payload fields `analytics` reads (§3.1.2.4) are a consumer contract, contract-tested per topic |

---

## 4. Dependencies/blockers/known issues

**Dependencies.**

| Dependency | Version | Used for |
| --- | --- | --- |
| MongoDB | 7.x | `analytics_db`; 7.0 for `$percentile` (ANL-7) and transactions on a replica set (ANL-1) |
| Motor | 3.6.x | Async MongoDB driver (A6) |
| `confluent-kafka[avro]` | 2.6.x | Consumer, via the `common` base |
| Celery | 5.4.x | `rebuild_daily_rollups` |
| OpenTelemetry SDK + instrumentations (FastAPI, PyMongo) | 1.28.x | Traces and metrics |
| `common` | — | JWT verification, consumer base class, problem details, telemetry ([RFC 0001](0001-platform-and-shared-infrastructure.md)) |
| Every producer's topic and payload | — | §3.1.2.4 |

**Blockers.** None. The assistant half depends on `assistant` emitting `assistant.interaction` through its outbox ([RFC 0009](0009-assistant-service.md), E13).

**Known issues.**

- Analytics is eventually consistent, so a dashboard can disagree with an order list for a few seconds.
- Counters other than `total_products` cannot be rebuilt after loss of `analytics_db` beyond 7 days: `raw_events` and Kafka retention are both 7 days, and `identity.user.registered` and `order.*` are not compacted. [Archive 0006](archive/0006-service-decomposition-and-data-model.md)'s "every projection is reconstructible from events or a backfill" is therefore true only within that horizon for analytics' all-time counters.
- One MongoDB node, local credential, no backup in Compose.
- Popularity over an arbitrary historical window is not answerable (Q-ANL-3).
- Failed product publishing is invisible to failure statistics until an event exists (Q-ANL-5).
- Assistant latency statistics are up to an hour old (ANL-7).
- The conversion baseline is a share of orders, not a conversion rate of non-interacting shoppers (Q-ANL-4).
- Anonymous assistant interactions count towards usage but can never convert, because there is no user to correlate.

---

## 5. Alternate solutions

**Alternative 1 — PostgreSQL for analytics instead of MongoDB.** One less technology; SQL aggregation is excellent.

*Rejected.* Part A requires a NoSQL store, and this is the one workload in the system with a genuine document shape — per-metric documents whose fields differ, and a raw-event archive with no fixed schema. Adding Mongo for a token table elsewhere would be worse than using it here (archive 0003 Alternative 5).

**Alternative 2 — Query the source services (or their read replicas) on demand.** No consumer, no duplication, always exact.

*Rejected* by C8. It couples the reporting path to the transaction path: a slow analytics query contends with checkout, and an analytics outage becomes a dependency problem for every service it reads. It also violates R1 and R4.

**Alternative 3 — Kafka transactions for exactly-once.**

*Rejected.* Exactly-once in Kafka is exactly-once *within* Kafka; it does not extend to a MongoDB write. Consumer idempotency is needed regardless (archive 0003 §3.2).

**Alternative 4 — Kafka Streams, ksqlDB or Flink for the aggregation.**

*Rejected.* Consumers in Python are adequate for the metric set; a stream processor is a large new concept for no requirement (archive 0003 §2.2).

**Alternative 5 — Compute everything on read from `raw_events`.** No rollups, no dedupe for counters, perfect flexibility.

*Rejected.* `raw_events` holds 7 days, so all-time totals would be impossible; and every dashboard load would scan the event archive, which is the cost C9 exists to avoid. It survives in one place — windowed popularity over recent days.

**Alternative 6 — Dedupe and effect without a transaction, with `$addToSet` of event ids on each rollup document.** No replica set needed.

*Rejected.* Unbounded arrays on hot documents (every counter would carry every event id it has ever seen), still multi-document per event, and still not atomic across documents.

**Alternative 7 — RabbitMQ or Celery as analytics' input.**

*Rejected.* RabbitMQ deletes a message once consumed, so there is no replay, no offset reset and no backfill — the replay test (ANL-G3) would be impossible to run (archive 0003 Alternatives 1 and 4).

---

## 6. Production readiness

### 6.1. Scalability and Fault tolerance

- **Blast radius.** `analytics` or its MongoDB being down means events accumulate in Kafka and are processed on recovery — checkout is unaffected. This follows directly from nothing synchronous being downstream of the order path.
- **No double counting, no lost increments** under crash, rebalance or replay (ANL-1, ANL-G4).
- **Poison messages cannot stall a partition.** Three retries with backoff, then DLQ, then offset commit (C6); the DLQ observer counts it (ANL-9).
- **Consumer lag is visible and bounded.** Exported and dashboarded; the consumer scales horizontally up to the partition count (3 locally). Replicas share the group, and the transaction's `_id` collision makes a rebalance duplicate harmless.
- **Hot documents.** The platform-scope `daily_metrics` document for today and `counters.orders_total` take a write per order. At local volume this is far below MongoDB's per-document update rate; if it ever were not, the fix is sharding the counter into N sub-documents summed on read, a change inside `services/rollups.py` only.
- **Idempotent everywhere.** The consumer dedupes, the reconciliation job replaces rather than increments, and the backfill has its own dedupe group.
- **Rebuildable within the horizon.** Buckets within 6 days are recomputed hourly; `total_products` is recomputable from compacted history.
- **Known ceilings.** One Mongo node, three partitions, one consumer group. None is a design decision that blocks scaling.

### 6.2. Security considerations

- **Merchant scoping** is applied from the verified token in the aggregation pipeline, never in the response (§3.1.2.2). A merchant must not be able to infer another merchant's revenue from a total.
- **Re-verification.** Every request's JWT is verified locally (A4, A5); the gateway's role check is not trusted.
- **Minimal payloads.** `analytics` stores envelopes that carry identifiers and the minimum to aggregate. `raw_events` inherits whatever producers put in payloads, so the producer-side rule — no password hashes, no card data, no full addresses beyond what a shipment notification requires — is the control, and the 7-day TTL bounds exposure.
- **`client_id` is not reused for analytics.** It is stored in `raw_events` only because the envelope is stored whole, and is never an input to a metric.
- **No customer-identifying output.** No endpoint returns a `user_id` or `customer_id`; `assistant_conversions` is only ever returned as aggregates.
- **MongoDB is not exposed outside the Compose network.** Its port is never published; `analytics` connects with a user whose role is `readWrite` on `analytics_db` only.
- **Accepted gaps** — a single local Mongo credential, no TLS to Mongo, and unauthenticated Kafka on the internal network. All are local-environment decisions, listed so they read as choices.

### 6.3. Deployment strategy

- **Compose.** `analytics` and MongoDB run in the `events` profile ([RFC 0001](0001-platform-and-shared-infrastructure.md)). The Mongo init script runs `rs.initiate()` for the single-member replica set and creates the `analytics` user.
- **Start-up ordering.** Health-gated `depends_on` on Kafka, Schema Registry and MongoDB (replica set primary elected). The consumer retries broker connection with backoff rather than crash-looping.
- **Indexes before traffic.** `ensure_indexes()` runs at start-up before the consumer subscribes.
- **The consumer deploys independently of every producer.** A restart of `analytics` never interrupts an API elsewhere.
- **Rollback.** Previous image; added fields and indexes are ignored by older code. A bad handler is rolled back and, if it applied wrong effects, the affected buckets within 6 days are corrected by `rebuild_daily_rollups`; counters require offset rewind plus a targeted backfill with a new dedupe group — a deliberate operator action, documented in the README.
- **The backfill and bootstrap are CLI only** (`make analytics-backfill`, `make analytics-bootstrap`), never an API — replaying arbitrary events through an endpoint is a footgun with no operator to use it (the same reasoning as the platform's DLQ replay decision).

---

## 7. Test Plan

**Unit** (no infrastructure).

- `services/rollups.py`: for each topic, a fixed envelope produces the exact expected list of update operations — including the `role=admin` no-op, the per-merchant fan-out of a two-merchant order, and `order_count` incremented once for two variants of one product in one order.
- Metric aggregation functions, given a synthetic event sequence (archive 0003 §7).
- Bucket key: `occurred_at` at 23:59:59.999 UTC and 00:00:00 UTC land in different documents; a week composes Monday–Sunday.
- AOV over a window is `Σ revenue ÷ Σ confirmed`, not the average of daily averages.
- Scope builder: a merchant claim always yields a `$match` on `merchant_id`; a merchant passing another `merchant_id` gets `422`; an admin without `merchant_id` gets platform scope.
- ANL-2 stale guard boundaries.
- Assistant classification: every `finish_reason` × `kind` combination maps to the right counters; refusals never increment answered (Q11, archive 0005).

**Integration** — real Kafka, real Schema Registry, real MongoDB replica set.

*Idempotency and replay*
- **The same event consumed twice moves a counter once** (archive 0003 §7).
- **Replay: reset offsets to earliest, re-consume everything, assert every counter and document is unchanged** (ANL-G3, archive 0003 G3). An acceptance criterion, not an optional extra — the risk it covers is idempotency implemented shallowly and only failing on replay.
- **Crash inside the transaction** (fault injected after the `$inc`, before commit) → restart → counter moved once (ANL-G4).
- **Crash after commit, before offset commit** → restart → `outcome="duplicate"`, counter moved once (ANL-G4).
- **Rebalance race**: two replicas handed the same event → one `applied`, one `duplicate`.
- **Transactions unavailable**: a stand-alone `mongod` → the consumer refuses to start with a clear error.
- **Compacted replay beyond the window**: a `catalog.product.published` with `occurred_at` 40 days ago → `stale`, `total_products` unchanged; `bootstrap-compacted` then sets the correct value.
- **TTL**: `raw_events` and `processed_events` carry TTL indexes with 7 and 30 days; a document with an expired timestamp is removed by the TTL monitor (asserted with a short test TTL).

*Metrics — one test per metric* (ANL-G1, ANL-G2), each asserting an exact value after a scripted event sequence:
- Three `user.registered` with `role=customer` → `total_customers == 3`; two with `role=merchant` → `total_merchants == 2`.
- Two `product.published`, one `product.unpublished` → `total_products == 1`.
- Orders over time: 3 `order.created`, 2 `order.confirmed` on one day → `orders_created=3`, `orders_confirmed=2`, `confirmation_rate=0.6667`.
- Two confirmed orders of 100 and 200 → revenue 300 and average order value 150.
- Popularity: product A 5 units, product B 3 units → A ranks first by quantity; by revenue per their prices.
- Stockout rate: 2 `inventory.stockout`, 40 `order.created` → `0.05`, offending variants listed.
- Failures: `order.failed` with two reasons and one DLQ arrival → grouped counts by source and reason.
- Assistant usage, questions asked/answered/refused, and average/p95 latency after a reconciliation run.
- Conversion: interaction at T, order at T+23h → converted; order at T+25h → not converted; baseline counts both.
- **Out-of-order conversion**: `order.confirmed` consumed before the preceding `assistant.interaction` → not converted on ingest, converted after `rebuild_daily_rollups`.
- **One order, two windows**: an order cannot convert two windows (partial unique `order_id`).

*Scoping and bucketing*
- **Merchant scoping: merchant A's revenue query excludes merchant B's orders entirely** (archive 0003 §7); the same for orders, popularity and stockouts. A merchant calling `overview`, `failures` or either assistant route → `403`.
- **Daily bucketing across a date boundary lands in the right documents** (archive 0003 §7).

*Reconciliation and backfill*
- `rebuild_daily_rollups` on consistent data → zero drift, documents unchanged; run twice → unchanged.
- Deliberately corrupt a bucket within the lookback → the job restores it and `analytics_reconcile_drift_total` increments.
- **Backfill equals live** (ANL-G9): a new projection backfilled over `raw_events` equals the same projection applied live to the same events; a re-run after a mid-backfill crash applies each event once.

*Contracts* (ANL-10)
- For each consumed topic, the registered Avro schema contains every field in §3.1.2.4's "Fields read"; a schema lacking one fails CI.

*DLQ*
- A malformed event goes to DLQ; the next event is processed normally; the DLQ observer increments `failure_stats` without deserialising the payload.

**Observability**
- Consumer lag, outcome counts and freshness are exported and non-null.
- A consumed event's span is a child of the producer's trace (C11) — asserted via the Jaeger API.

**Isolation** (ANL-G5)
- Stop `analytics` and MongoDB; run the checkout E2E → green. Restart → lag drains to zero, and every counter equals the value an uninterrupted run produces.
- The R1 test is extended: the `analytics` Mongo user cannot read any other database, and no service's configuration contains the `analytics` Mongo URI.

**End-to-end** — through the gateway, cold start, full stack.
1. Register a customer and a merchant → `user.registered` consumed and `total_customers` incremented.
2. Publish a product → `product.published` consumed and `total_products_published` incremented.
3. Place and confirm an order → revenue and popularity updated, and the analytics consumer's span in the checkout trace.
4. Place an order that fails payment → the failure counter incremented by reason.
5. Order a product to exhaustion → `inventory.stockout` reflected in the stockout rate.
6. Place an order after an assistant interaction → the conversion metric registers it within 24 hours (archive 0005 G10).

**Explicitly not tested** — load beyond functional concurrency; MongoDB failover (single node); alerting.

---

## 8. Impact Analysis

**On other services.** None at runtime: nothing calls `analytics`, and it calls nothing. The contractual impacts are the three in §3.1.4 — the gateway route rule, the producer payload fields, and the Mongo replica set.

**On other RFCs.**

| RFC | Change | Why it cannot live in this RFC |
| --- | --- | --- |
| [0000](0000-architecture.md) | The event catalogue lists `analytics` as a consumer of every topic; `*.dlq` observation by `analytics-dlq-observer` (ANL-9); a proposed `catalog.product.publish_failed` topic (Q-ANL-5) | The catalogue is defined once there |
| [0001](0001-platform-and-shared-infrastructure.md) | MongoDB as a single-node replica set with init script; `analytics` queue on Celery with the hourly `rebuild_daily_rollups` Beat entry; consumer base exposes a pluggable dedupe store | Hosting belongs to Platform |
| [0002](0002-gateway-service.md) | `/api/v1/analytics/*` route roles (§3.1.2.2) | The gateway's route table lives there |
| [0004](0004-catalog-service.md), [0005](0005-order-service.md), [0006](0006-inventory-service.md), [0003](0003-identity-service.md), [0009](0009-assistant-service.md) | Payloads carry the fields in §3.1.2.4 | Each producer owns its schema |

**On reviewers.** A review of this service should be able to check: replaying a topic changes nothing; a crash at any point never double-counts; each of the nine Part A and four Part B metrics has a defensible definition and a test; a merchant cannot see another merchant's figures; and checkout passes with `analytics` stopped.

**Risks.**

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Consumer idempotency implemented shallowly and only failing on replay | Medium | High | The replay and crash-point tests are acceptance criteria (ANL-G3, ANL-G4) |
| Metric definitions disagreeing with the reviewer's reading | Medium | Low | Definitions stated explicitly (§3.1.1.2) and raised as Q10 and Q-ANL-1–6 |
| A producer silently drops a field analytics depends on | Medium | Medium | Contract test per topic (ANL-10) and Avro `BACKWARD` compatibility |
| Replica-set setup forgotten on a fresh machine | Medium | Medium | Consumer refuses to start with a named error; init script in Compose |
| Analytics work crowding out the rest of the event pipeline in the schedule | High | High | Priority order in §3.1.5; read APIs reducible to the minimum |
| A conversion metric that flatters the assistant | Medium | Medium | Labelled correlational, reported beside a baseline (Q3, archive 0005) |

---

## 9. Useful Links/References

**Project documents**

- [RFC 0000 — Architecture](0000-architecture.md): [ownership test](0000-architecture.md#3112-the-ownership-test), [R1–R8](0000-architecture.md#3113-boundary-rules-r1r8), [A1–A14](0000-architecture.md#3115-cross-cutting-decisions-a1a14), [event catalogue](0000-architecture.md#3123-event-catalogue), [envelope](0000-architecture.md#3124-event-envelope), [S1–S15](0000-architecture.md#3131-schema-conventions-s1s15), [shared tables](0000-architecture.md#3132-shared-tables--outbox-and-processed_events), [store selection](0000-architecture.md#3137-store-selection)
- [RFC 0001 — Platform and Shared Infrastructure](0001-platform-and-shared-infrastructure.md) · [RFC 0002 — `gateway`](0002-gateway-service.md) · [RFC 0009 — `assistant`](0009-assistant-service.md)
- [Part A](../part-a.md) §4, §5, §6 · [Part B](../part-b.md) Analytics Metrics · [Milestones](../milestones.md)
- Archived sources: [0000](archive/0000-system-architecture.md), [0003](archive/0003-events-and-observability.md), [0005](archive/0005-ai-assistant-and-streaming.md), [0006](archive/0006-service-decomposition-and-data-model.md)

**Technical references**

- MongoDB transactions (replica-set requirement) — https://www.mongodb.com/docs/manual/core/transactions/
- MongoDB TTL indexes — https://www.mongodb.com/docs/manual/core/index-ttl/
- MongoDB `$percentile` — https://www.mongodb.com/docs/manual/reference/operator/aggregation/percentile/
- MongoDB `Decimal128` — https://www.mongodb.com/docs/manual/reference/bson-types/#decimal128-bson-data-type
- Motor — https://motor.readthedocs.io/
- Kafka delivery semantics — https://kafka.apache.org/documentation/#semantics
- Kafka log compaction — https://kafka.apache.org/documentation/#compaction

---

## 10. Related tickets/issues

| ID | Title | Maps to |
| --- | --- | --- |
| *(to be created)* | ANL-T1 … ANL-T14 | The tasks in §3.1.5, one ticket each |
| — | Archive 0003 T17, T18, T19 | ANL-T1–T6 |
| — | Archive 0005 T20, T21, T25 | ANL-T12–T14 |
| — | Q-ANL-5 | `catalog.product.publish_failed` — needs [RFC 0004](0004-catalog-service.md) and [RFC 0000](0000-architecture.md) |

---

## 11. Notes

- **On what "basic" means.** The delivery plan asks for "basic analytics metrics" ([milestones](../milestones.md)). Basic here means all nine of Part A's metrics computed correctly with a test each — not a subset.
- **On sequencing.** ANL-T3 — one topic, end to end, with the crash-point tests — is the most important task. Every later handler is the same transaction with a different list of operations.
- **On honesty in the metrics.** "Conversion after AI interaction" is correlational and reported next to a baseline (Q3, archive 0005); refusals are counted separately from answers (Q11, archive 0005). A metric that flatters the feature is worse than no metric at the review.
- **On the `†` / `‡` cells.** Every inferred type in the copied collection definitions, and every ANL addition, is a proposal for ratification at review, not a settled decision.

---

## 12. Comments

*Reviewer comments go here. Please raise objections against the numbered decisions (C8, C9, ANL-1–ANL-10), the open questions (Q8 of archive 0000, Q4/Q10 of archive 0003, Q3/Q11 of archive 0005, Q-ANL-1–7), the metric definitions in §3.1.1.2, or the rejected alternatives in §5, so the discussion attaches to something specific.*

| Reviewer | Section | Comment | Resolution |
| --- | --- | --- | --- |
| | | | |
