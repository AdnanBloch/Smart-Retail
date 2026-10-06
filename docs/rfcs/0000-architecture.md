# RFC 0000 — System Architecture

| | |
| --- | --- |
| **Owner** | @muhammad.adnan |
| **Collaborators** | SarwanAhmed |
| **Status** | **DRAFT** · IN-REVIEW · DONE |
| **Created** | Sep 29, 2026 |
| **Related Epic or story** | All milestones ([milestones.md](../milestones.md)), Parts A and B ([part-a.md](../part-a.md), [part-b.md](../part-b.md)) |
| **Supersedes** | [archive/0000 — System Architecture](archive/0000-system-architecture.md) (in full) · [archive/0006 — Service Decomposition & Data Model](archive/0006-service-decomposition-and-data-model.md) (the cross-cutting half; per-table columns moved to the service RFCs) · the cross-cutting conventions of [archive/0001](archive/0001-foundation-and-core-services.md), [archive/0002](archive/0002-core-business-logic-and-workflows.md) and [archive/0003](archive/0003-events-and-observability.md) |
| **Depends on** | — (the root of the RFC set). Implemented through [RFC 0001 — Platform & Shared Infrastructure](0001-platform-and-shared-infrastructure.md) and the service RFCs [0002](0002-gateway-service.md)–[0009](0009-assistant-service.md) |

> **How to read this RFC.** This is the single cross-cutting architecture document for SmartRetail. It holds only what must be consistent *across* services: the service inventory and the test that produced it, the boundary rules (R1–R8), the cross-cutting decisions (A1–A14), the authentication/authorisation split, the three critical flows, the API conventions, the event catalogue and envelope, the schema conventions (S1–S15), the shared `outbox` and `processed_events` DDL, the cross-service entity model, the index of every table, the reference kinds and the ownership map. Everything about one service — its endpoints, state machines, workflows, configuration, metrics and the full column list of every table it owns — lives in that service's RFC (0002–0009). Hosting, runtime and infrastructure — `common`, the workers, Kafka, Redis, Compose — live in [RFC 0001](0001-platform-and-shared-infrastructure.md). Where this RFC fixes a convention, RFC 0001 says how it is implemented and the service RFCs say how they apply it; none of them may silently contradict it.

---

## Contents

1. [Title](#1-title)
2. [Overview](#2-overview)
   - 2.1. [Goals](#21-goals)
   - 2.2. [Non-goals](#22-non-goals)
3. [Solution Overview](#3-solution-overview)
   - 3.1. [Specification](#31-specification)
     - 3.1.1. [Architectural changes](#311-architectural-changes)
       - 3.1.1.1. [Service inventory](#3111-service-inventory)
       - 3.1.1.2. [The ownership test](#3112-the-ownership-test)
       - 3.1.1.3. [Boundary rules (R1–R8)](#3113-boundary-rules-r1r8)
       - 3.1.1.4. [Authentication and authorisation split](#3114-authentication-and-authorisation-split)
       - 3.1.1.5. [Cross-cutting decisions (A1–A14)](#3115-cross-cutting-decisions-a1a14)
       - 3.1.1.6. [Critical flows](#3116-critical-flows)
       - 3.1.1.7. [Module structure inside a service](#3117-module-structure-inside-a-service)
     - 3.1.2. [API changes](#312-api-changes)
       - 3.1.2.1. [Surface map](#3121-surface-map)
       - 3.1.2.2. [API conventions](#3122-api-conventions)
       - 3.1.2.3. [Event catalogue](#3123-event-catalogue)
       - 3.1.2.4. [Event envelope](#3124-event-envelope)
     - 3.1.3. [Data/schema changes](#313-dataschema-changes)
       - 3.1.3.1. [Schema conventions (S1–S15)](#3131-schema-conventions-s1s15)
       - 3.1.3.2. [Shared tables — `outbox` and `processed_events`](#3132-shared-tables--outbox-and-processed_events)
       - 3.1.3.3. [Cross-service entity model](#3133-cross-service-entity-model)
       - 3.1.3.4. [Table index](#3134-table-index)
       - 3.1.3.5. [Cross-boundary reference kinds](#3135-cross-boundary-reference-kinds)
       - 3.1.3.6. [Ownership map](#3136-ownership-map)
       - 3.1.3.7. [Store selection](#3137-store-selection)
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

**SmartRetail system architecture — the service inventory and the method that produced it, the boundary rules, the cross-cutting decisions and conventions, the event catalogue, and the platform-wide data model (conventions, shared tables, entity model, table index and ownership map) for Parts A and B.**

This RFC is the umbrella design for the whole platform. It answers "how many services are there, what does each one own, how do they talk to each other, and what rules does every schema follow" once, so that each service RFC can confine itself to the detail of that one service.

Nothing here is implemented directly. Every component named below is specified by its own RFC, listed in §3.1.1.1 and in the reading order in §3.1.5.

---

## 2. Overview

[Part A](../part-a.md) specifies an event-driven commerce backend: catalog, orders, inventory, fulfilment, notifications, and analytics, built on FastAPI, PostgreSQL, Redis, Celery/RabbitMQ, Kafka, and Temporal, with OpenTelemetry-based observability. [Part B](../part-b.md) adds a GenAI layer on top: semantic product retrieval and a conversational shopping assistant.

Those two documents describe the system by requirement. Neither says how many services there are or where a boundary falls, and neither specifies a table. Both describe capabilities. How those capabilities are grouped into deployable units, and how their data is modelled, is our design work — and it is the work the reviews will examine hardest, because everything else follows from it. Making those decisions piecemeal, one piece of work at a time, produces boundaries that are artefacts of the order the work happened in rather than of what the data actually looks like.

Two questions determine more about this platform than any other: **what is a service**, and **who owns which fact**. They are the same question asked twice, because a service boundary that does not coincide with a data-ownership boundary is not a boundary at all — it is a shared database with extra network hops.

This RFC therefore fixes the target shape up front: **eight services**, each justified by a distinct piece of state it owns, plus four non-HTTP worker components and one shared library. It states the test a candidate service must pass and applies it to every plausible candidate, including the ones rejected. It fixes the cross-cutting conventions — authentication, error format, correlation IDs, event envelope, idempotency, schema rules — that every service has to agree on, because retrofitting a convention across eight services is exactly the kind of work that gets skipped. And it holds the platform-wide view of the data model — the conventions, the two shared tables, the entity relationships across boundaries and an index of every table — while each table's full column list is defined once, in the RFC of the service that owns it. A type that appears twice is a type that will eventually disagree with itself.

### 2.1. Goals

The two archived RFCs each had a G1–G7 list; they are merged here. The *Archive* column keeps each goal traceable.

| # | Archive | Goal | How it is verified |
| --- | --- | --- | --- |
| G1 | 0000 G1, 0006 G2 | A complete service inventory, each entry justified by the state it owns and the invariant it protects | §3.1.1.1–§3.1.1.2; every service in the inventory has a database or store that nothing else writes |
| G2 | 0006 G1 | A stated, repeatable test for whether something should be a service | §3.1.1.2; applied to every candidate, including rejections |
| G3 | 0006 G3 | Every rejected candidate recorded with its reason | §3.1.1.2; a reviewer can disagree with a specific row |
| G4 | 0000 G2, 0006 G7 | A data ownership map with zero shared tables and zero cross-service foreign keys | §3.1.3.6; enforced by database grants, not convention; grep for cross-database connection strings finds nothing; §7 boundary tests |
| G5 | 0006 G4 | Every table and every column in the platform defined exactly once | Each column list lives in exactly one service RFC; the table index in §3.1.3.4 is the completeness check |
| G6 | 0006 G5 | A cross-service entity model showing every reference and its kind | §3.1.3.3, §3.1.3.5 |
| G7 | 0006 G6 | Schema conventions fixed once, applied by every service | §3.1.3.1; deviations are review failures |
| G8 | 0000 G3 | A named event catalogue with a fixed envelope and versioning rule | §3.1.2.3–§3.1.2.4; every event any service produces appears in the catalogue |
| G9 | 0000 G4 | End-to-end flows for the three critical paths — publish, checkout, assistant query | §3.1.1.6; each flow is traceable across services by a single correlation ID once tracing is in place |
| G10 | 0000 G5 | Cross-cutting conventions agreed once: auth, errors, pagination, config, idempotency, tracing | §3.1.2.2; implemented in `common` ([RFC 0001](0001-platform-and-shared-infrastructure.md)), reused rather than reimplemented per service |
| G11 | 0000 G6 | A component → delivery-phase map, so no phase is surprised by what it has to stand up | §3.1.5, Delivery phasing |
| G12 | 0000 G7 | Every deferral recorded with the RFC that owns it, so scope boundaries are decisions and not omissions | Non-goals table below and each RFC's §2.2 |

### 2.2. Non-goals

| Excluded | Why / where it lives |
| --- | --- |
| Per-service implementation detail — endpoint bodies, per-table column definitions, state-machine transition tables, test cases | The owning service RFC ([0002](0002-gateway-service.md)–[0009](0009-assistant-service.md)). This document stops at the boundary and the contract. The permitted enum *values* are summarised here (S4); the permitted *transitions* live with the behaviour, in each service RFC's §3.1.1.3 |
| Kafka topic configuration, Redis keyspaces, object-storage layout, ORM implementation detail, `common` internals | Not cross-service contracts. [RFC 0001](0001-platform-and-shared-infrastructure.md) |
| Query-plan tuning and index sizing against real data | Per service, against real query shapes. Indexes that are part of the *design* — the HNSW parameters, every unique constraint, the outbox partial index — are specified with their tables |
| Kubernetes, cloud deployment, managed services, CI/CD pipelines | The target environment for the whole assignment is Docker Compose on a developer machine ([Execution Guidelines](../execution-guidelines.md)) |
| Multi-region, multi-tenancy beyond merchant scoping, disaster recovery, physical sharding, partitioning, read replicas | No requirement asks for them, they would distort every boundary below, and nothing here prevents them |
| A frontend | The API and its OpenAPI documentation are the deliverable |
| Real payment or carrier integrations | Both are simulated. See decision A9 |
| Performance targets and load testing | No traffic figure is specified anywhere in Parts A or B. Capacity claims in §6.1 are structural, not measured |

---

## 3. Solution Overview

### 3.1. Specification

#### 3.1.1. Architectural changes

##### 3.1.1.1. Service inventory

**The eight services.**

| # | Service | Port | Owns (state nothing else writes) | Store | RFC |
| --- | --- | --- | --- | --- | --- |
| 1 | `gateway` | 8000 | Nothing — stateless edge | — | [RFC 0002](0002-gateway-service.md) |
| 2 | `identity` | 8001 | Users, credentials, roles, merchant profiles, refresh sessions | `identity_db` (PostgreSQL) | [RFC 0003](0003-identity-service.md) |
| 3 | `catalog` | 8002 | Products, variants, categories, media, catalog audit trail, publish failures | `catalog_db` (PostgreSQL) | [RFC 0004](0004-catalog-service.md) |
| 4 | `order` | 8003 | Orders, order lines, order state, payment and shipment sub-state, idempotency keys | `order_db` (PostgreSQL) | [RFC 0005](0005-order-service.md) |
| 5 | `inventory` | 8004 | Stock levels, reservations, stock movement ledger | `inventory_db` (PostgreSQL) | [RFC 0006](0006-inventory-service.md) |
| 6 | `notification` | 8005 | Notification records, delivery attempts, push subscriptions, per-device stream cursors | `notification_db` (PostgreSQL) | [RFC 0007](0007-notification-service.md) |
| 7 | `analytics` | 8006 | Event-derived rollups and metric series | `analytics_db` (MongoDB) | [RFC 0008](0008-analytics-service.md) |
| 8 | `assistant` | 8007 | Chunks, embeddings, index state, query log, conversations, messages, generations, budgets, prompt templates | `assistant_db` (PostgreSQL + pgvector) | [RFC 0009](0009-assistant-service.md) |

**Non-HTTP components.** These are deployable containers or packages but not services in the sense above — they hold no API surface and own no schema of their own. Their *hosting and runtime* are specified in [RFC 0001](0001-platform-and-shared-infrastructure.md); the *business logic* they run belongs to the service whose rules it is, and is specified in that service's RFC.

| Component | Role | Hosting / runtime | Business logic specified in |
| --- | --- | --- | --- |
| `temporal-worker` | Hosts `ProductPublishingWorkflow` and `OrderWorkflow`; activities call service-layer functions (B1) | [RFC 0001](0001-platform-and-shared-infrastructure.md) | [RFC 0004](0004-catalog-service.md#3114-workflows-and-async-jobs) (publishing), [RFC 0005](0005-order-service.md#3114-workflows-and-async-jobs) (order) |
| `celery-worker` (+ Beat) | Background tasks off RabbitMQ — notification dispatch, media processing, scheduled rollups and sweeps, bulk generation | [RFC 0001](0001-platform-and-shared-infrastructure.md) | Each task's owning service RFC, §3.1.1.4 |
| `indexer-worker` | Consumes `catalog.product.*`, chunks and embeds product text, writes `assistant_db` | [RFC 0001](0001-platform-and-shared-infrastructure.md) | [RFC 0009](0009-assistant-service.md#3114-workflows-and-async-jobs) |
| `outbox-relay` | Reads each producing service's transactional outbox and publishes to Kafka (C1, C2) | [RFC 0001](0001-platform-and-shared-infrastructure.md) | — (no business logic; the outbox DDL is §3.1.3.2) |
| `common` | Shared non-domain code: auth, config, logging, errors, pagination, DB session, `DomainEnum` / `DomainEnumText` (S4), outbox and idempotency helpers, service-token mint/verify, event envelope, Avro serialiser, consumer base class, tracing setup, Temporal client factory. **Contains no business rule and no vocabulary** (R8) | [RFC 0001](0001-platform-and-shared-infrastructure.md) | — |

Infrastructure: one PostgreSQL 16 instance (one logical database and role per service, A2), MongoDB, Redis, Kafka (KRaft) + Schema Registry, RabbitMQ, MinIO, Temporal server + UI, and the OTel Collector → Jaeger / Prometheus → Grafana stack. All are specified in [RFC 0001](0001-platform-and-shared-infrastructure.md).

**Target topology.**

```
                                 ┌──────────────────────────────┐
     client ─────────────────────▶│  gateway  (:8000)            │
     HTTP / JSON / SSE            │  JWT verify · routing        │
                                  │  correlation ID · CORS       │
                                  │  rate limit                  │
                                  └──┬────┬────┬────┬────┬───┬───┘
              ┌──────────────────────┘    │    │    │    │   └──────────────┐
              ▼                   ┌───────▼┐ ┌─▼────┐ ┌▼──────┐        ┌────▼─────┐
        ┌───────────┐             │ order  │ │inven-│ │analyt-│        │assistant │
        │ identity  │             │(:8003) │ │tory  │ │ics    │        │ (:8007)  │
        │ (:8001)   │             │        │ │(:8004)│ │(:8006)│       │ RAG/SSE  │
        └─────┬─────┘             └───┬────┘ └──┬───┘ └───┬───┘        └────┬─────┘
              │      ┌───────────┐    │         │         │                 │
              │      │ catalog   │    │         │         │            ┌────▼─────┐
              │      │ (:8002)   │    │         │         │            │  vector  │
              │      └─────┬─────┘    │         │         │            │  store   │
              │            │          │         │         │            └────┬─────┘
       ┌──────▼────────────▼──────────▼─────────▼──┐      │                 │
       │        PostgreSQL 16 — one instance,      │      │            ┌────▼─────┐
       │        one logical database per service   │      │            │assistant │
       └───────────────────────────────────────────┘      │            │   _db    │
                            │                             │            └──────────┘
                    ┌───────▼────────┐             ┌──────▼──────┐
                    │ outbox-relay   │             │  MongoDB    │
                    └───────┬────────┘             └──────▲──────┘
                            │                             │
       ┌────────────────────▼─────────────────────────────┴──────────────┐
       │   Kafka + Schema Registry   — the event backbone                │
       └───┬───────────────────┬──────────────────────┬─────────────┬────┘
           │                   │                      │             │
   ┌───────▼──────┐   ┌────────▼───────┐    ┌─────────▼──────┐  ┌───▼────────┐
   │ notification │   │ indexer-worker │    │  analytics     │  │ inventory  │
   │   (:8005)    │   │                │    │  consumers     │  │ + catalog  │
   └───────┬──────┘   └────────────────┘    └────────────────┘  │ consumers  │
           │                                                    └────────────┘
   ┌───────▼──────┐   ┌────────────────┐   ┌──────────────────────────────────┐
   │ RabbitMQ +   │   │   Temporal     │   │ OTel Collector → Jaeger          │
   │ celery-worker│   │   + worker     │   │               → Prometheus/Grafana│
   └──────────────┘   └────────────────┘   └──────────────────────────────────┘
        Redis (rate limits · idempotency fast path · caches · notification
        pub/sub and SSE tickets · Celery results) and MinIO (media binaries)
        sit beside the services and are specified in RFC 0001.
```

`notification` additionally holds open SSE connections and fans out over Redis Pub/Sub so any replica can serve any client ([RFC 0007](0007-notification-service.md)). `catalog` consumes `identity.merchant.status_changed` to project merchant approval state ([RFC 0004](0004-catalog-service.md#3124-events-consumed)).

##### 3.1.1.2. The ownership test

**The decomposition method.** Three candidate methods were available. The one used, and why:

| Method | What it splits by | Why not / why |
| --- | --- | --- |
| By technical layer | api / logic / data | Produces a distributed monolith. Every feature touches every service; nothing can be deployed alone |
| By team | Conway's law, made deliberate | There is one developer. Meaningless here |
| **By bounded context and data ownership** | A model boundary where terms have one unambiguous meaning, and one writer per fact | **Used.** It is the only one of the three that produces boundaries which survive a change in requirements, because it follows the domain rather than the org chart or the stack |

**The four-part test.** A service exists here only if there is a piece of state that it alone writes and that has a different consistency, volume, or lifecycle profile from its neighbours. Formally, a candidate becomes a service only if it passes **all four**:

1. **Owned state.** There is a set of facts it alone writes. If two candidates would write the same fact, they are one service, or the fact is misplaced.
2. **An invariant worth protecting.** It enforces a rule that must hold transactionally — "reserved never exceeds on-hand", "one order per idempotency key". A service with no invariant is a table with an HTTP interface.
3. **An independent reason to change.** Its requirements move for reasons unrelated to its neighbours'.
4. **A distinct operational profile** — different load shape, consistency need, store type, or availability requirement — or a boundary that meaningfully contains failure.

Failing any one means it belongs inside another service, as a module.

**Applying the test — all eighteen candidates.**

| # | Candidate | State it would own | Invariant | Independent change | Distinct profile | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `gateway` | none | — | yes | edge concerns | **Service, by exception** — see below |
| 2 | `identity` | users, credentials, roles, sessions | unique email; only identity mints tokens | yes | auth load; highest security boundary | **Service** |
| 3 | `catalog` | products, variants, categories, media | SKU unique per merchant; status machine | yes | read-heavy, high cardinality | **Service** |
| 4 | `order` | orders, lines, payment and shipment state, idempotency keys | one order per key; valid state transitions | yes | write-heavy bursts; strongest consistency need | **Service** |
| 5 | `inventory` | stock, reservations, movement ledger | `on_hand ≥ reserved ≥ 0` | yes | hot-row contention; the highest-contention writes in the system | **Service** |
| 6 | `notification` | notifications, delivery attempts | at-most-one per (event, user, channel) | yes | bursty, fully async, failure-tolerant | **Service** |
| 7 | `analytics` | rollups, series | counters monotonic per event | yes | append-heavy, document store, must never affect checkout | **Service** |
| 8 | `assistant` | conversations, chunks, embeddings, generations | chunks match indexed catalog version | yes | GPU/provider-bound, unbounded latency, external cost | **Service** |
| 9 | `payment` | payment intents | — (would be order state) | no | no | **Rejected → `order`** |
| 10 | `shipment` | shipment records | — (would be order state) | no | no | **Rejected → `order`** |
| 11 | `cart` | carts | would have one | no requirement | no | **Rejected — not built** |
| 12 | `search` | none — index is derived | none | no | no | **Rejected → worker + `assistant`** |
| 13 | `media` | none — `catalog` owns metadata | none | no | no | **Rejected → Celery task** |
| 14 | `pricing` | none — price is a product attribute | none | no | no | **Rejected → `catalog`** |
| 15 | `review` | reviews | would have one | not in scope | yes | **Rejected — no requirement** |
| 16 | `recommendation` | none | none | no | no | **Rejected → generation in `assistant`** |
| 17 | `merchant` | merchant profiles | subset of user invariants | no | no | **Rejected → `identity`** |
| 18 | `audit` | audit rows | must be transactional with the change it records | no | no | **Rejected — see below** |

**The reasoning behind the most-discussed verdicts.**

| Candidate | Verdict | Reasoning |
| --- | --- | --- |
| `payment` | **Folded into `order`** | It would own no state a real provider does not own. With a simulated provider it is an empty box. Payment intent and status live as an aggregate on the order; the call is a Temporal activity |
| `shipment` / `fulfilment` | **Folded into `order`** | Same. Shipment status is a sub-state of the order's lifecycle, driven by workflow activities and carrier webhooks we simulate |
| `search` | **Not a service** | Indexing is a worker; keyword query is a `catalog` endpoint; semantic query is an `assistant` endpoint. No distinct owned state |
| `media` | **Not a service** | Object storage plus a Celery task. `catalog` owns the media metadata |
| `cart` | **Not built** | Neither Part A nor Part B mentions a cart. Orders are placed directly. Noted here so its absence reads as a decision |
| `review` / `recommendation` | **Not built** | Not in scope. Part B's "cross-sell suggestions" is a generation task in `assistant`, not a recommender system |
| `inventory` | **Kept separate** | Different write profile entirely — high-contention row-level updates under flash sale load, and the multi-writer consistency problem Part A names in its opening paragraph. Putting it in `catalog` recreates the bug we are hired to fix |
| `analytics` | **Kept separate** | Read-mostly, append-heavy, different store (document), different query shape, and it must not be able to slow a checkout down |

**The three rejections most worth defending**, because they are where a reviewer is most likely to push back:

- **`payment` and `shipment`.** In a real system, a payment service exists for a *compliance* boundary — PCI scope isolation — and to own a provider's reconciliation, refund, and dispute lifecycle. Neither exists here: the provider is simulated (A9) and no cardholder data enters the system at any point. What would remain is a table of payment intents keyed one-to-one to orders, updated only by the order's own workflow. Splitting it would create a distributed transaction across two databases to record one fact, which is strictly worse than a column — more failure modes, no isolation gained. The *interface* is kept separate so a real provider, or a real service, is a later extraction rather than a rewrite. Owned by [RFC 0005](0005-order-service.md#3111-responsibility-and-boundaries).
- **`audit`.** Tempting, because audit data is append-only and grows differently from the data it describes. Rejected because an audit row must be written in the *same transaction* as the change it records — otherwise there exists a state where the change happened and the trail did not, which defeats the purpose. Cross-service transactions do not exist here. So each service owns its own audit trail (S7), and centralised querying, if ever needed, is a consumer of events.
- **`gateway`.** It fails test 1 — it owns no state — and is a deliberate exception. Cross-cutting edge concerns (routing, early token rejection, correlation IDs, CORS, rate limiting, aggregated OpenAPI) must live in exactly one place; the alternatives are duplicating them into eight services or hiding them in a proxy configuration nobody reviews. The exception is bounded by a rule: **the gateway holds no business logic and no database**, and any authorisation decision requiring domain knowledge stays in the service that owns the resource. Owned by [RFC 0002](0002-gateway-service.md#3111-responsibility-and-boundaries).

Decided and recorded rather than open: `identity` is not split into `auth` and `user-profile` (they share the `users` aggregate); one Postgres instance with eight logical databases is a real boundary (separate roles make a cross-database query fail at connection time); `assistant` is not split into retrieval and generation (D12, [RFC 0009](0009-assistant-service.md)); a projection that disagrees with its source loses and is rebuilt; categories get no shared reference-data service; multi-location inventory changes `stock_items`' unique key, not the decomposition.

##### 3.1.1.3. Boundary rules (R1–R8)

These hold for every service at all times and are what the reviews check.

| # | Rule | Enforced by |
| --- | --- | --- |
| R1 | A service reads and writes only its own database | Separate Postgres roles with grants on one database each — it fails at connection time, not at review time |
| R2 | No foreign key crosses a service boundary | Schema review; cross-service references are plain indexed UUIDs |
| R3 | A fact has exactly one writer | The ownership map (§3.1.3.6) |
| R4 | Cross-service reads go through an API or an event, never a join | Code review; R1 makes the alternative impossible |
| R5 | Synchronous cross-service calls only where the caller cannot proceed without the answer | One such write-path call exists: `order → catalog` at creation. See §3.3.1 ARC-Q2 for the assistant's read-path hydration |
| R6 | Data crossing a boundary is snapshotted or projected, never referenced live | §3.1.3.5 reference kinds |
| R7 | A service may not require another to be up in order to serve reads it already has data for | Local JWT verification; events over calls |
| R8 | `common` contains no business rule | Review check, plus the vocabulary grep test in §7 |

##### 3.1.1.4. Authentication and authorisation split

These are two different jobs and they live in two different places, so they are stated separately here rather than left to be inferred from A4 and A5.

*Authentication is owned by `identity`, alone.* It is the only component that holds credentials, the only one that signs a token, and the only one that can revoke a session. No other service reads `identity_db`, and no other service has the private key.

| Concern | Owner | Detail |
| --- | --- | --- |
| Registration, password hashing, credential storage | `identity` | Argon2id; `identity_db` only |
| Login, token issuance, refresh, logout | `identity` | RS256; 15-minute access token, rotating refresh token with reuse detection |
| Signing key custody and rotation | `identity` | Private key mounted into `identity` only; public key (JWKS) served at `/api/v1/auth/jwks` and cached by every verifier |
| Role assignment, merchant approval | `identity` | Roles and merchant status are claims, set here and nowhere else |
| Service tokens for callers with no user in context | `identity` | A short-lived service JWT from a client-credentials grant, carrying `sub: "service:<component>"` (e.g. `service:temporal-worker`) and a `scope` claim (internal authentication, [archive/0002 §3.1.2](archive/0002-core-business-logic-and-workflows.md); issued per [RFC 0003](0003-identity-service.md)) |
| Token **verification** | every service, plus `gateway` | Local signature and claims check against the cached public key. No network call to `identity` on the request path (A4, A5) |

Full detail: [RFC 0003](0003-identity-service.md#3121-endpoints).

*Authorisation is owned by the service that owns the resource.* There is no central policy service and no authorisation call across a service boundary — a service that owns a row is the only component that knows what it means to be allowed to touch it.

| Layer | What it decides | What it must not do |
| --- | --- | --- |
| `gateway` | Is there a valid, unexpired token? Coarse route-level role check (e.g. `/api/v1/analytics/*` is `admin`-only) | Be the only check. It is an optimisation that rejects early, never the security boundary |
| Owning service | Does *this* subject, with *these* claims, get to perform *this* action on *this* row? Role check plus ownership check plus merchant scoping | Trust a role, user id, or merchant id from the request body or a header — claims come from the verified token only |
| `common` | Supplies the verification, the claim model, and the dependency that services declare their requirements with | Contain domain rules. It answers "who is this", not "may they" |

Concretely: `identity` says a caller is merchant `M` with role `merchant`; `catalog` decides whether merchant `M` may edit product `P`, by checking `P.merchant_id`. Neither decision is made anywhere else.

**Internal callers.** Two kinds exist, and they are treated differently:

| Caller | Has a user in context? | Presents | Why |
| --- | --- | --- | --- |
| `order` validating and snapshotting products at creation (A13) | Yes — it is inside the customer's request | The **end user's token**, forwarded | `catalog` applies exactly the same rule it applies at the edge; no separate trust model |
| Temporal activities calling `inventory` (reserve / release / commit) and `catalog` | No — the workflow outlives the request | A **service token** with the required `scope` | There is no user token to forward. Internal routes require that scope and **reject user tokens**; user-facing routes **reject service tokens**. Sharing a role for both would mean a stolen customer token could call `/internal/v1/inventory/reservations` |

The service-token path is the "call with no user in context" that the archived text reserved for an amendment to this section; it is recorded in the amendment log (§11). A new internal caller must pick one of these two rows explicitly; a third trust model is an amendment here, not a local decision. A service's API is its only public interface — the internal routes are the same HTTP surface with a scoped token, not a back door (§3.1.2.2).

##### 3.1.1.5. Cross-cutting decisions (A1–A14)

These bind every service. Service RFCs may add decisions (with their own prefixes) but may not silently contradict these.

- **A1 — Service per owned aggregate, database per service.** Boundaries are enforced by deployment topology, not developer discipline. Implemented by the per-service database and role created by the Postgres init script ([RFC 0001](0001-platform-and-shared-infrastructure.md)).
- **A2 — One PostgreSQL instance, one logical database and role per service.** Real isolation at schema level without paying for eight containers on a laptop that also has to host Kafka, Temporal, and Grafana. Splitting into separate instances later is a configuration change, not a code change.
- **A3 — No foreign keys across service boundaries.** `order.order_items.product_id` is a plain indexed UUID. Referential integrity across services is an eventual property maintained by events, not a constraint.
- **A4 — RS256 JWTs; `identity` is the only signer.** Every other component holds the public key and verifies locally, with no network call on the request path.
- **A5 — Every service re-verifies the token.** The gateway rejects early; it is not trusted. Services must be safe to call directly, because there are internal callers (§3.1.1.4).
- **A6 — Async all the way down.** FastAPI `async def`, SQLAlchemy 2.0 async, `asyncpg`. No sync I/O inside the event loop.
- **A7 — RFC 9457 problem details, one shape everywhere**, produced by one handler in `common`.
- **A8 — `X-Request-ID` in, W3C `traceparent` out.** A correlation ID is generated at the gateway if absent, propagated over HTTP headers and in the event envelope, and attached to every log line. With tracing in place it is the OpenTelemetry trace ID (C12).
- **A9 — Payments and carriers are simulated, behind an interface.** A `PaymentProvider` and `ShippingProvider` protocol with a deterministic fake implementation that can be told to fail, time out, or succeed. The point is the workflow's behaviour under those failures, not the integration.
- **A10 — Transactional outbox for every state-change event.** A service writes its state change and its outbox row in one transaction; `outbox-relay` publishes. Dual-writing to Postgres and Kafka in the same handler is the standard way to lose events, and it is not done anywhere.
- **A11 — Consumers are idempotent and keyed.** Every consumer records processed `event_id`s and is safe to replay, because Kafka delivery is at-least-once.
- **A12 — Temporal owns multi-step business processes; Kafka owns notifications of fact; Celery owns fire-and-forget tasks.** Three tools, three distinct jobs — see §3.2.
- **A13 — Synchronous cross-service calls only where the caller cannot proceed without the answer.** In practice that is exactly one write path: `order` validating and snapshotting product data at creation. Everything else is an event.
- **A14 — Data is snapshotted at the boundary, not joined across it.** An order line stores the product name, SKU, and price as they were at purchase. This is correct domain modelling, not a workaround.

**Cross-cutting decisions from the archived milestone RFCs that bind every service.** They keep their original IDs; each is restated where it applies in this RFC and implemented in [RFC 0001](0001-platform-and-shared-infrastructure.md).

| ID | Decision (in brief) | Applied in this RFC |
| --- | --- | --- |
| B1 | Temporal activities are thin adapters over the service layer | §3.1.1.7 |
| B2 | Workflows hold identifiers and step results, never business data | §3.1.1.6 |
| B3 | Every activity is idempotent, keyed by a deterministic token from `(workflow_id, activity_name, attempt-invariant input)` | §3.1.2.2 Idempotency |
| B7 | Idempotency keys are claimed by a unique insert, never read-then-write | §3.1.2.2 Idempotency |
| B9 | Endpoints that start a workflow return `202`, not `201` | §3.1.2.2 Async acceptance |
| B11 | The outbox is written in the state-change transaction, in every producing service | §3.1.3.2 |
| C3 | Ordering is per aggregate; the Kafka key is the aggregate id | §3.1.2.3 |
| C4 | At-least-once delivery with idempotent consumers, not Kafka transactions | §3.1.2.4, §3.1.3.2 |
| C5 | Avro with Schema Registry, `BACKWARD` compatibility | §3.1.2.4 |
| C6 | Three in-process retries with backoff, then `<topic>.dlq`; a poison message must not stop a partition | §3.1.2.4 |
| C11 | Trace context propagates over HTTP *and* over Kafka | §3.1.2.4 |
| C12 | The trace ID replaces the request ID as the correlation identifier | §3.1.2.2, §3.1.2.4 |
| C21 | Delivery is scoped by the originating client (`X-Client-Id` → outbox `client_id` → envelope) | §3.1.2.2, §3.1.2.4 |

**Where each archived decision series now lives.** For traceability:

| Series | Archived in | Principal home now |
| --- | --- | --- |
| A1–A14 | archive/0000 | This RFC |
| A1–A8 *(a separate, overlapping numbering)* | archive/0001 | See the mapping below |
| B1–B3 (Temporal activities and workflows) | archive/0002 | Runtime: [RFC 0001](0001-platform-and-shared-infrastructure.md); workflow logic: [RFC 0004](0004-catalog-service.md), [RFC 0005](0005-order-service.md) |
| B4–B6 (reservations, expiry, ledger) | archive/0002 | [RFC 0006](0006-inventory-service.md) |
| B7–B10 (idempotency, snapshots, `202`, simulated providers) | archive/0002 | [RFC 0005](0005-order-service.md); B7 and B9 conventions here |
| B11 (outbox everywhere) | archive/0002 | This RFC §3.1.3.2; relay in [RFC 0001](0001-platform-and-shared-infrastructure.md) |
| B12 (media upload) | archive/0002 | [RFC 0004](0004-catalog-service.md) |
| C1–C7, C10–C14 (relay, Kafka, Celery, observability, Redis, profiles) | archive/0003 | [RFC 0001](0001-platform-and-shared-infrastructure.md); C3–C6, C11–C12 conventions here |
| C8–C9 (analytics) | archive/0003 | [RFC 0008](0008-analytics-service.md) |
| C15–C23 (push delivery, SSE, scoping, cursors, ack) | archive/0003 | [RFC 0007](0007-notification-service.md); C21's `X-Client-Id` convention here |
| D1–D12 (retrieval) | archive/0004 | [RFC 0009](0009-assistant-service.md) |
| E1–E14 (assistant, streaming, budgets) | archive/0005 | [RFC 0009](0009-assistant-service.md) |
| R1–R8, S1–S15 | archive/0006 | This RFC |

**archive/0001's A-numbering.** archive/0001 used its own A1–A8, which overlap with, but do not match, the A1–A14 above. They are mapped here so a citation of "0001 A5" is not read as this RFC's A5:

| archive/0001 | Equivalent here |
| --- | --- |
| A1 — separate services with a database each, from day one | A1 |
| A2 — one PostgreSQL instance, multiple logical databases | A2 |
| A3 — no foreign keys across service boundaries | A3 |
| A4 — asymmetric JWT signing (RS256) | A4 |
| A5 — stateless access tokens (15 min), stateful refresh tokens (7 days, hashed, revocable) | No equivalent here — an `identity` decision, [RFC 0003](0003-identity-service.md) |
| A6 — the gateway verifies; each service re-verifies | A5 |
| A7 — async all the way down | A6 |
| A8 — per-service Alembic migration history | S14 |

##### 3.1.1.6. Critical flows

*Flow 1 — Product publishing.*

```
merchant          catalog           temporal          workers/consumers
   │                 │                  │                    │
   │ POST /products/{id}/status         │                    │
   │ {"status":"published"}             │                    │
   ├────────────────▶│                  │                    │
   │                 │ start workflow   │                    │
   │                 ├─────────────────▶│                    │
   │◀────────────────┤ 202 + workflow_id│                    │
   │                 │                  │ 1 validate fields  │
   │                 │                  │ 2 process media ───┼──▶ celery
   │                 │                  │ 3 build search doc │
   │                 │                  │ 4 sync downstream  │
   │                 │◀─────────────────┤ 5 mark published   │
   │                 │   + outbox row   │   (one txn)        │
   │                 ├──── outbox-relay ┼───▶ Kafka: catalog.product.published
   │                 │                  │        ├──▶ indexer-worker  → embeddings
   │                 │                  │        ├──▶ analytics       → products_published
   │                 │                  │        └──▶ inventory       → create stock row
   │ GET /products/{id}  status: published                   │
```

| Step | What happens | Specified in |
| --- | --- | --- |
| Request | Owner merchant or admin posts `{"status":"published"}`; `catalog` guards (owner? `draft` or `inactive`? not already running?), sets `publishing`, records `workflow_id`, returns `202` | [RFC 0004 §3.1.2.1](0004-catalog-service.md#3121-endpoints), [§3.1.1.3](0004-catalog-service.md#3113-state-machines) |
| 1–4 | `validate_product`, `process_media` (enqueued to Celery), `build_search_document`, `sync_downstream` — Temporal activities with retry policies | [RFC 0004 §3.1.1.4](0004-catalog-service.md#3114-workflows-and-async-jobs) |
| 5 | `mark_published`: status, `published_at`, audit row and outbox row in one transaction — the only state write | [RFC 0004 §3.1.1.4](0004-catalog-service.md#3114-workflows-and-async-jobs), §3.1.3.2 |
| Publish | `outbox-relay` publishes `catalog.product.published` | [RFC 0001](0001-platform-and-shared-infrastructure.md), [RFC 0004 §3.1.2.3](0004-catalog-service.md#3123-events-produced) |
| React | `indexer-worker` chunks and embeds; `analytics` counts; `inventory` creates the zero-quantity stock row (Q4) | [RFC 0009 §3.1.2.4](0009-assistant-service.md#3124-events-consumed), [RFC 0008 §3.1.2.4](0008-analytics-service.md#3124-events-consumed), [RFC 0006 §3.1.2.4](0006-inventory-service.md#3124-events-consumed) |

Partial failure never leaves a half-published product: steps 1–4 are Temporal activities with retry policies, step 5 is the only state write, and a workflow that fails permanently runs compensations and leaves the product in `draft` with a recorded failure reason (`product_publish_failures`).

*Flow 2 — Checkout.*

```
customer        order          inventory      temporal        catalog     kafka
   │              │                │             │               │          │
   │ POST /orders │                │             │               │          │
   │ Idempotency-Key: <uuid>       │             │               │          │
   ├─────────────▶│                │             │               │          │
   │              │ claim key (unique insert)    │               │          │
   │              │ snapshot product data ───────┼──────────────▶│          │
   │              │ persist order = pending      │               │          │
   │              │ start OrderWorkflow ────────▶│               │          │
   │◀─────────────┤ 202 {order_id, status}       │               │          │
   │              │                │  reserve ───┤               │          │
   │              │                │◀────────────┤               │          │
   │              │                │  charge (simulated)         │          │
   │              │                │  commit reservation         │          │
   │              │                │  create shipment            │          │
   │              │◀───────────────┼─ confirm ───┤               │          │
   │              ├─ outbox ───────┼─────────────┼───────────────┼─────────▶│
   │              │                │             │               │   order.confirmed
   │              │                │             │               │     ├─▶ notification
   │              │                │             │               │     └─▶ analytics
```

| Step | What happens | Specified in |
| --- | --- | --- |
| Request | Customer posts with `Idempotency-Key`; the key is claimed by unique insert (B7) | [RFC 0005 §3.1.2.1](0005-order-service.md#3121-endpoints), §3.1.2.2 Idempotency |
| Snapshot | `order` reads each product from `catalog` with the user's token (A13, R5) and snapshots name, SKU, price (A14, B8) | [RFC 0005 §3.1.1.4](0005-order-service.md#3114-workflows-and-async-jobs), [RFC 0004 §3.1.2.1](0004-catalog-service.md#3121-endpoints) |
| Persist and start | Order `pending` + items + outbox `order.created` in one transaction; `OrderWorkflow` started with `workflow_id = order-{order_id}`; `202` | [RFC 0005 §3.1.1.4](0005-order-service.md#3114-workflows-and-async-jobs) |
| Reserve / commit / release | Workflow activities call `inventory`'s internal routes with a service token (B4, B5) | [RFC 0006 §3.1.2.1](0006-inventory-service.md#3121-endpoints), [§3.1.1.4](0006-inventory-service.md#3114-workflows-and-async-jobs) |
| Charge, ship | Simulated providers behind `PaymentProvider` / `ShippingProvider` (A9, B10) | [RFC 0005 §3.1.1.4](0005-order-service.md#3114-workflows-and-async-jobs) |
| Confirm | `confirmed` + outbox `order.confirmed` in one transaction | [RFC 0005 §3.1.1.3](0005-order-service.md#3113-state-machines), [§3.1.2.3](0005-order-service.md#3123-events-produced) |
| React | `notification` records and pushes; `analytics` counts | [RFC 0007 §3.1.2.4](0007-notification-service.md#3124-events-consumed), [RFC 0008 §3.1.2.4](0008-analytics-service.md#3124-events-consumed) |

Any step failing runs its compensation in reverse — release reservation, refund, mark order `failed` — which is why this is a workflow and not a chain of event handlers.

*Flow 3 — Assistant query.*

```
customer      gateway     assistant      vector store   catalog/inventory    LLM
   │             │            │                │               │              │
   │ POST /assistant/chat (SSE)                │               │              │
   ├────────────▶├───────────▶│                │               │              │
   │             │            │ embed query ───┼───────────────┼─────────────▶│
   │             │            │ top-k search ─▶│               │              │
   │             │            │◀── chunks ─────┤               │              │
   │             │            │ hydrate live price/stock ─────▶│              │
   │             │            │ assemble prompt + history      │              │
   │             │            ├────────────────┼───────────────┼─────────────▶│
   │◀══ token stream (SSE) ═══┤◀═══════════════┼═══════════════┼══════════════┤
   │             │            │ persist message, emit assistant.interaction   │
```

| Step | What happens | Specified in |
| --- | --- | --- |
| Request | SSE chat request through the gateway's streaming pass-through | [RFC 0009 §3.1.2.1](0009-assistant-service.md#3121-endpoints), [RFC 0002](0002-gateway-service.md) |
| Retrieve | Embed, hybrid top-k over `product_chunks` with SQL pre-filters | [RFC 0009 §3.1.1.4](0009-assistant-service.md#3114-workflows-and-async-jobs) |
| Hydrate | Live price and availability at read time, never embedded (D9); availability from `GET /api/v1/inventory/{variant_id}` | [RFC 0009 §3.1.1.5](0009-assistant-service.md#3115-dependencies-and-degradation), [RFC 0006 §3.1.2.1](0006-inventory-service.md#3121-endpoints) |
| Generate and stream | Prompt assembly, provider stream → SSE typed events | [RFC 0009 §3.1.1.4](0009-assistant-service.md#3114-workflows-and-async-jobs) |
| Record | Persist the message; outbox `assistant.interaction` → `analytics` | [RFC 0009 §3.1.2.3](0009-assistant-service.md#3123-events-produced), [RFC 0008 §3.1.2.4](0008-analytics-service.md#3124-events-consumed) |

*Beneath all three — the event path.* Every state-change event in the platform travels the same way:

```
 producing service              outbox-relay              Kafka            consumer
      │                              │                      │                    │
      │ BEGIN                        │                      │                    │
      │  update aggregate            │                      │                    │
      │  insert outbox(event, correlation, client_id)       │                    │
      │ COMMIT ──────────────────────┼──────────────────────┼────────────────────┤
      │                              │ SELECT … WHERE       │                    │
      │                              │  published_at IS NULL│                    │
      │                              │  FOR UPDATE SKIP LOCKED                   │
      │                              │ serialise Avro ─────▶│ key=aggregate_id   │
      │                              │ on ack: set published_at                  │
      │                              │                      ├───────────────────▶│
      │                              │                      │  restore trace ctx │
      │                              │                      │  seen(event_id)?   │
      │                              │                      │   yes → commit,skip│
      │                              │                      │   no  → BEGIN      │
      │                              │                      │     insert processed│
      │                              │                      │     apply effect   │
      │                              │                      │    COMMIT          │
      │                              │                      │  commit offset     │
```

If the relay crashes after producing but before setting `published_at`, the row is re-published on restart — a duplicate `event_id` that the consumer's dedupe absorbs. That is the at-least-once trade (C4), and it is why the outbox, the envelope's `event_id` and `processed_events` are a single design rather than three. Relay and consumer base class: [RFC 0001](0001-platform-and-shared-infrastructure.md).

##### 3.1.1.7. Module structure inside a service

Boundaries are recursive: the same reasoning applies below the service line, which is what keeps a service from becoming a small monolith. Every service uses the same internal layering, so moving between them is not a re-orientation:

```
  main.py            create_application(): settings, logging, DB, router
                     mounting, /health. Run as `uvicorn --factory main:create_application`.
  app/
  ├── api/
  │   ├── router.py      Every route: path, method, tags, response_model,
  │   │                  status_code, route dependencies. Prefix /api/v<N>.
  │   └── handlers/      HTTP only — parse, validate, call, serialise. No rules.
  │       ├── <res>.py   `router = APIRouter()` plus plain handler functions,
  │       │              no decorators. One file per resource.
  │       ├── health.py  Readiness checks for /health/ready.
  │       └── utils.py   FastAPI dependencies (session, principal, repos).
  ├── services/      <entity>_repo.py — the only layer permitted business rules.
  │                  Called by API handlers, Temporal activities, event consumers, CLI.
  ├── models/        <entity>.py — SQLAlchemy ORM and the Pydantic request/response
  │                  models for that entity (the contract; never the ORM object).
  │                  enums.py — this service's value sets (S4); response.py — shared bases.
  ├── configs/       env.py — the service's settings class.
  ├── infrastructure/ db.py, logger.py, security.py, seed.py.
  ├── exception_handlers/ This service's DomainError subclasses.
  ├── middleware/    ASGI middleware (when the service has its own).
  ├── clients/       Outbound calls to other services (when it makes them).
  ├── constants/     Fixed values (when it has them).
  ├── utils/         state machines (the transition table) and small pure helpers.
  ├── consumers/     Event handlers — thin, like API handlers.
  ├── workflows/     Temporal workflows and activities.
  └── tasks/         Celery tasks.
  alembic/           This service's own migration history (S14).
  tests/
```

The layout matches the EVA backends, so engineers moving between the two codebases find the same folders in the same places.

**Routes and handlers.** `api/router.py` is the only file that declares routes. Each handler module owns one `router = APIRouter()`; `router.py` registers every handler on it (`products.router.get("/{product_id}", response_model=ProductDetail)(products.get_product)`) and includes it in the root `router` under the `/api/v<N>` prefix with its resource prefix and tag (`router.include_router(products.router, prefix="/products", tags=["products"])`). `main.py` mounts the root router. Handler functions carry no decorators; a handler module may also hold a small request-parsing dependency its handlers share. Registration order in `router.py` is significant — a literal segment such as `/me` is registered before `/{id}`.

The rule that matters: **handlers do not touch the ORM, and the service layer does not construct HTTP responses.** That single seam is why a Temporal activity (B1) and an event consumer can reuse the same business rule as an endpoint, with one implementation and one set of tests. Each service RFC's §3.1.1.1 shows its own instance of this layout; repository layout and the shared base images are in [RFC 0001](0001-platform-and-shared-infrastructure.md).

#### 3.1.2. API changes

##### 3.1.2.1. Surface map

Every route is versioned under `/api/v1` and reached through the gateway at `:8000`.

| Prefix | Service | RFC |
| --- | --- | --- |
| `/api/v1/auth/*`, `/api/v1/users/*`, `/api/v1/merchants/*` | `identity` | [RFC 0003 §3.1.2.1](0003-identity-service.md#3121-endpoints) |
| `/api/v1/products/*`, `/api/v1/categories/*`, `/api/v1/variants/*`, `/api/v1/media/*` | `catalog` | [RFC 0004 §3.1.2.1](0004-catalog-service.md#3121-endpoints) |
| `/api/v1/orders/*`, `/api/v1/webhooks/shipment` | `order` | [RFC 0005 §3.1.2.1](0005-order-service.md#3121-endpoints) |
| `/api/v1/inventory/*` | `inventory` | [RFC 0006 §3.1.2.1](0006-inventory-service.md#3121-endpoints) |
| `/internal/v1/*` (not routed by the gateway) | the owning service: `catalog` (`/internal/v1/products/*`, `/internal/v1/media/*`), `inventory` (`/internal/v1/inventory/*`) | [RFC 0004](0004-catalog-service.md#3121-endpoints), [RFC 0006](0006-inventory-service.md#3121-endpoints) |
| `/api/v1/notifications/*` | `notification` | [RFC 0007 §3.1.2.1](0007-notification-service.md#3121-endpoints) |
| `/api/v1/analytics/*` | `analytics` | [RFC 0008 §3.1.2.1](0008-analytics-service.md#3121-endpoints) |
| `/api/v1/search/*` (semantic, similar, index status and reindex) | `assistant` | [RFC 0009 §3.1.2.1](0009-assistant-service.md#3121-endpoints) |
| `/api/v1/assistant/*` | `assistant` | [RFC 0009 §3.1.2.1](0009-assistant-service.md#3121-endpoints) |
| `/health`, `/health/ready`, `/metrics`, `/docs`, `/openapi.json` | all (the gateway also serves an aggregated OpenAPI view) | [RFC 0001](0001-platform-and-shared-infrastructure.md), [RFC 0002](0002-gateway-service.md#3121-endpoints) |

The archived surface map listed `/api/v1/search/semantic` only; the retrieval RFC added `similar` and `index/*` under the same prefix, so the whole `/api/v1/search/*` prefix routes to `assistant`. A new prefix is an amendment to this table.

##### 3.1.2.2. API conventions

Fixed here, once, for all services. Implemented in `common` ([RFC 0001](0001-platform-and-shared-infrastructure.md)); each service RFC's §3.1.2.1 applies them.

- *Errors* — RFC 9457 problem details (A7): `{"type", "title", "status", "detail", "instance", "request_id"}` plus `violations[]` on validation failures. Example: `{"type": "…/errors/product-not-found", "title": "Product not found", "status": 404, "detail": "…", "instance": "/api/v1/products/…", "request_id": "…"}`. One error shape across every service means one error handler in `common`. Common statuses carry fixed meaning everywhere: `409` for an optimistic-concurrency loser (S6) or an illegal state transition, `422` for semantic validation failures, `429` for rate or budget limits (always with `Retry-After` or a reset time), `501` for a route whose behaviour is not yet provided.
- *Pagination* — `{"items": [...], "total": N, "limit": L, "offset": O}`, hard `limit` ceiling of 100. Limit/offset is chosen over cursors because listings are filtered and sorted by arbitrary columns, where a cursor buys little; a cursor variant may be added for a high-volume path, stated in that service's RFC (the notification list's `since_seq` forward paging is one).
- *Correlation* — `X-Request-ID` accepted or generated at the gateway, echoed on every response, present on every log line, carried into every event and workflow (A8). With tracing on, every response also carries `traceparent`, and `X-Request-ID` echoes the OTel trace ID (C12) — header name and behaviour unchanged, only the value's provenance differs, so a log line, a trace and an event all carry the same string. Trace context crosses Kafka in the envelope (C11, §3.1.2.4).
- *Device correlation* — an optional `X-Client-Id` header on mutating requests (and on `/notifications/stream-ticket`), minted by the client per browser profile or app install. The gateway passes it through — it never generates one — and validates its shape only (`^[A-Za-z0-9_-]{8,64}$`), **stripping** a malformed value rather than rejecting the request: a cosmetic header must never fail a checkout. The producing service records it in the outbox row's `client_id`; it is carried into the event envelope and used to scope notification delivery to the originating device (C21, [RFC 0007](0007-notification-service.md)). It is never an identity and never an authorisation input — the fifth reference kind in §3.1.3.5.
- *Idempotency* — an `Idempotency-Key` header on every non-idempotent POST that creates business state. `POST /api/v1/orders` requires it ([RFC 0005](0005-order-service.md#3121-endpoints)); the mechanism lives in `common` so later services reuse it. The contract:
  - The key is **claimed by a unique insert**, `INSERT … ON CONFLICT DO NOTHING` on `(user_id, key)`, never by a read-then-write (B7). A read-check-write has a race that only appears under exactly the burst load Part A describes.
  - Keys are **scoped per user**, so one caller cannot probe or collide with another's.
  - The request body is canonicalised and hashed; **same key with a different body is a client bug → `422`**.
  - A retry of a completed request receives the **stored reply, replayed verbatim** (same status and body).
  - An **in-flight duplicate receives `409`** (retry later), not a second object.
  - Keys are retained 24 hours, then swept. Redis holds a fast path (`idem:{user_id}:{key}`, 24h) ahead of the Postgres claim, which remains authoritative ([RFC 0001](0001-platform-and-shared-infrastructure.md)).
  - Endpoints that are naturally idempotent (`PATCH`, `PUT` of an absolute value) or protected by a unique constraint (e.g. `(merchant_id, sku)`) do not need the header.
  - Behind the API, **every Temporal activity is idempotent**, keyed by a deterministic token from `(workflow_id, activity_name, attempt-invariant input)` (B3), and a workflow started for an aggregate uses a deterministic `workflow_id` (e.g. `order-{order_id}`) so Temporal rejects a duplicate start — a second line of defence behind the key. Behind Kafka, consumers dedupe on `event_id` (A11, C4).
  - The table that carries the claim is [`idempotency_keys`](0005-order-service.md#idempotency_keys).
- *Async acceptance* — an endpoint that starts a workflow or a background job returns `202` with a resource URL to poll (`_links.self`, or a job id), never a blocking wait on completion (B9). The resource exists and is addressable; its lifecycle is asynchronous.
- *Streaming* — server-to-client streams are Server-Sent Events, not WebSockets (E4, C15): plain HTTP, so the gateway, JWT verification, rate limiting, tracing and problem-detail errors apply unchanged. The gateway must not buffer `text/event-stream` (no compression, extended read timeout). Where `EventSource` cannot set `Authorization`, the stream authenticates with a short-lived single-use ticket, never a JWT in the query string (C19). Contracts: [RFC 0007](0007-notification-service.md), [RFC 0009](0009-assistant-service.md); pass-through: [RFC 0002](0002-gateway-service.md).
- *Rate limiting* — at the gateway, `429` with `Retry-After`, with tighter limits on `/auth/login`, `/orders` and assistant endpoints. Limits: [RFC 0002](0002-gateway-service.md).
- *Internal routes* — every internal route is mounted under **`/internal/v1/<resource>/…`** on the owning service (never under `/api/v1`), and the gateway never routes `/internal/*` ([RFC 0002](0002-gateway-service.md) GW-4). An internal route is part of the owning service's normal HTTP surface, requires a service token with the named `scope`, and rejects user tokens; user-facing routes reject service tokens (§3.1.1.4). **A service's API is its only public interface**: there is no internal access path to another service's data that bypasses it.
- *Configuration* — twelve-factor, validated at startup by `pydantic-settings`; a missing variable fails the container immediately ([RFC 0001](0001-platform-and-shared-infrastructure.md)).
- *Operational endpoints* — `/health` (liveness) and `/health/ready` (readiness, including a store round-trip; consumers also report broker connectivity and lag) on every service, `/metrics` (Prometheus exposition) and `/docs`, `/openapi.json` (per-service OpenAPI).
- *Versioning* — additive changes only within `v1`. A breaking change means `v2` mounted alongside.

##### 3.1.2.3. Event catalogue

Fixed here so producers and consumers are designed against the same list. A producer may not invent a topic outside the catalogue without amending this RFC.

**Naming.** The catalogue name of an event is `<domain>.<event>` (for example `order.confirmed`, `catalog.product.published`) and is the envelope's `event_type`. The Kafka topic is that name under the platform prefix: `smartretail.<domain>.<event>`. One schema per topic in the Schema Registry, Avro-encoded, `BACKWARD` compatibility enforced (C5). Each topic has a dead-letter topic `<topic>.dlq` (C6). The key is the aggregate id, so all events for one aggregate land on one partition in order; ordering is per aggregate, never global (C3). Partitions, retention and compaction are platform configuration ([RFC 0001](0001-platform-and-shared-infrastructure.md)): locally 3 partitions, replication factor 1, 7-day retention, `compact` on `identity.merchant.status_changed` and `catalog.product.published`, where the latest state per key is what a new consumer needs.

| Topic | Producer | Key | Consumers | Payload and trigger |
| --- | --- | --- | --- | --- |
| `identity.user.registered` | `identity` | `user_id` | `analytics`, `notification` | [RFC 0003](0003-identity-service.md#3123-events-produced) |
| `identity.merchant.status_changed` | `identity` | `user_id` | `catalog`, `analytics` | [RFC 0003](0003-identity-service.md#3123-events-produced) |
| `catalog.product.published` | `catalog` | `product_id` | `inventory`, `analytics`, `indexer-worker` | [RFC 0004](0004-catalog-service.md#3123-events-produced) |
| `catalog.product.updated` | `catalog` | `product_id` | `indexer-worker`, `analytics`; `catalog` itself for browse-cache invalidation | [RFC 0004](0004-catalog-service.md#3123-events-produced) |
| `catalog.product.unpublished` | `catalog` | `product_id` | `indexer-worker`, `analytics` | [RFC 0004](0004-catalog-service.md#3123-events-produced) |
| `order.created` | `order` | `order_id` | `analytics` | [RFC 0005](0005-order-service.md#3123-events-produced) |
| `order.confirmed` | `order` | `order_id` | `notification`, `analytics` | [RFC 0005](0005-order-service.md#3123-events-produced) |
| `order.failed` | `order` | `order_id` | `notification`, `analytics` | [RFC 0005](0005-order-service.md#3123-events-produced) |
| `order.cancelled` | `order` | `order_id` | `notification`, `analytics`, `inventory` | [RFC 0005](0005-order-service.md#3123-events-produced) |
| `payment.succeeded` / `payment.failed` | `order` | `order_id` | `notification`, `analytics` | [RFC 0005](0005-order-service.md#3123-events-produced) |
| `shipment.status_changed` | `order` | `order_id` | `notification`, `analytics` | [RFC 0005](0005-order-service.md#3123-events-produced) |
| `inventory.reserved` / `inventory.released` | `inventory` | `variant_id` | `analytics` | [RFC 0006](0006-inventory-service.md#3123-events-produced) |
| `inventory.stockout` | `inventory` | `variant_id` | `analytics`, `notification` | [RFC 0006](0006-inventory-service.md#3123-events-produced) |
| `assistant.interaction` | `assistant` | `conversation_id` | `analytics` | [RFC 0009](0009-assistant-service.md#3123-events-produced) |

**Count.** The table has fourteen rows — the "fourteen topics" the archived events RFC refers to. Two rows name a pair (`payment.succeeded` / `payment.failed`, `inventory.reserved` / `inventory.released`), so the number of distinct topics, and of registered schemas and DLQs, is **sixteen**. The `catalog` consumer on `catalog.product.updated` comes from the archived events RFC's cache-invalidation rule and was not listed in the archived catalogue; it is added here so the table is complete.

**Carrying tables.** A producer carries `outbox`; a consumer carries `processed_events` — tabulated per database in §3.1.3.2.

##### 3.1.2.4. Event envelope

Every message on every topic, regardless of domain:

```json
{
  "event_id":      "uuid",          // unique; consumers dedupe on this
  "event_type":    "order.confirmed",
  "event_version": 1,
  "occurred_at":   "RFC3339 timestamp",
  "producer":      "order",
  "correlation_id":"the X-Request-ID that caused it",
  "causation_id":  "the event_id that caused it, if any",
  "actor_id":      "uuid | null",
  "client_id":     "opaque client token | null",   // the X-Client-Id that caused it
  "payload":       { }
}
```

| Field | Meaning | Source |
| --- | --- | --- |
| `event_id` | Unique per event; the consumer dedupe key (A11, C4) | `outbox.event_id` |
| `event_type` | The catalogue name (§3.1.2.3) | `outbox.event_type` |
| `event_version` | The payload schema version for this `event_type` | Registered schema |
| `occurred_at` | When the state change committed, RFC 3339, UTC (S3) | `outbox.created_at` |
| `producer` | The producing service | The source database |
| `correlation_id` | The `X-Request-ID` (= OTel trace ID, C12) of the request that caused it | `outbox.correlation_id` |
| `causation_id` | The `event_id` that caused this event, if any | `outbox.causation_id` |
| `actor_id` | The authenticated user who caused it, or null for a workflow, scheduler or webhook | Payload context |
| `client_id` | The `X-Client-Id` that caused it, or null (C21) | `outbox.client_id` |
| `payload` | The domain payload, shape per topic, defined in the producer's §3.1.2.3 | `outbox.payload` |

The fields not held as outbox columns (`event_version`, `producer`) are supplied at publish time by the relay from the registered schema and the source database; see [RFC 0001](0001-platform-and-shared-infrastructure.md).

**Schema rules.**

- **Avro with Schema Registry, `BACKWARD` compatibility (C5).** Schemas are checked at registration, so an incompatible change fails in CI rather than in a consumer at runtime. JSON Schema was the alternative; Avro is what Part A's "Schema Registry" implies and gives compact payloads with real evolution rules. Schemas live in `common/events/schemas/` and are registered by a script ([RFC 0001](0001-platform-and-shared-infrastructure.md)).
- **Additive evolution only.** A new field is optional with a default. `client_id` is the worked example: `{"name": "client_id", "type": ["null", "string"], "default": null}` — `null` must be first in the union for the default to be legal, and with the default present the change is `FULL`, not merely the `BACKWARD` that C5 requires: a v2 reader resolves a v1 record through the default, and a v1 reader skips the unknown field. The compatibility test asserts both directions.
- **Minimum payload.** Events carry ids and the minimum payload a consumer needs — no credentials, no password hashes, no card data, no full PII beyond what a consumer requires.

**Consumer contract.** Every consumer — service or worker — uses the `common` consumer base class ([RFC 0001](0001-platform-and-shared-infrastructure.md)):

- **At-least-once, idempotent (C4).** Record `(consumer_group, event_id)` in the consumer's own `processed_events` (§3.1.3.2) in the same transaction as the effect; a seen `event_id` is committed and skipped. Kafka transactions are not used: exactly-once in Kafka does not extend to a Postgres write or an email.
- **Retry then DLQ (C6).** Three in-process attempts with exponential backoff, then the event goes to `<topic>.dlq` with its error and the consumer commits the offset and moves on. A poison message must not stop a partition. Replay from the DLQ is a deliberate `make` CLI action, never automatic.
- **Trace context restored (C11).** W3C `traceparent` travels with the event and is restored by the consumer, so a trace does not end at the producer. See §3.3.1 ARC-Q1 for where the field is carried.
- **One consumer process per service**, subscribing to several topics, with dedupe keyed by consumer group.

#### 3.1.3. Data/schema changes

**What this section holds.** Eight stores and 40 tables (38 distinct definitions, 46 instances counting the shared tables per database). This section fixes the conventions every table follows (§3.1.3.1), defines the two shared tables once (§3.1.3.2), draws the cross-service picture (§3.1.3.3), indexes every table (§3.1.3.4), names the kinds of cross-boundary reference (§3.1.3.5), and records who owns each fact (§3.1.3.6) and why each store was chosen (§3.1.3.7). **Every other table's full column list is defined exactly once, in the RFC of the service that owns it**, under a heading `###### \`table_name\`` that the index links to. Every table is specified in its complete form; there is no per-phase variant.

**How to read it.** §3.1.3.1 fixes the conventions that make every type legible, and must be read first.

**Markers — inferred and added cells.** Column tables in this RFC and in every service RFC use two markers, and they mean different things. Both are **proposals for review**, not decisions already taken — a reviewer who disagrees with a marked cell is disagreeing with the consolidation that produced it, not with the archived source.

| Mark | Meaning |
| --- | --- |
| *(none)* | Stated explicitly in an archived source RFC, named in the Notes cell where it matters |
| `†` | The column **is named** in an archived source RFC, but its **type was not stated** there. The type shown is inferred from the conventions in §3.1.3.1 |
| `‡` | The column or table is **not named in any archived source RFC**. It exists because a convention requires it — S1 a primary key, S3 timestamps, S12 an outbox for a producer, S13 a dedupe table for a consumer |

Every marked cell is counted in the census at the end of §3.1.3.4.

##### 3.1.3.1. Schema conventions (S1–S15)

Fixed once, followed by every service. A deviation is a review failure, not a style preference.

| # | Convention | Rule | Why |
| --- | --- | --- | --- |
| S1 | **Primary keys** | One test: **does the id ever leave the database?** Yes → `uuid` v4, generated by the database (`gen_random_uuid()`). No, and the table is high-volume and append-only → `bigserial`. The `bigserial` set is exactly `*_audit_log`, `order_status_history`, `stock_movements`, `outbox` and `notifications.seq` | **`uuid` where the id leaves the database** — URLs, API responses, event payloads, analytics joins. There a sequential integer leaks volume (`/orders/84215`), collides when analytics merges ids from separate databases, and cannot be generated before the insert, which idempotency and correlation ids require. **`bigserial` where it never does** — none of those three benefits apply, so only UUID's cost is left, and these are the highest-insert tables in the system. A random UUID lands each insert on a random B-tree leaf (page splits, cache misses, extra WAL); `bigserial` appends to the rightmost leaf, is 8 bytes instead of 16, and gives a built-in order that `ORDER BY id` and cursor reads rely on (the outbox relay, notification catch-up). `notifications` shows both: `id` is a `uuid` because clients see it and use it to remove duplicates, while `seq` is a `bigserial` used only for internal ordering |
| S2 | **Money** | `numeric(12,2)` plus a `char(3)` ISO 4217 currency column, always adjacent. Never `float` | Binary floating point cannot represent 0.10. A stored total that disagrees with the sum of its lines by a cent is a defect with no defence |
| S3 | **Timestamps** | `timestamptz`, UTC, never naive. `created_at` and `updated_at` on every mutable table | A naive timestamp is ambiguous the first time anything runs in another zone, and the ambiguity is unrecoverable |
| S4 | **Enums** | A plain `text` column. No Postgres `ENUM` type and no `CHECK` constraint. The permitted set is a `DomainEnum` (a `StrEnum`) in the owning service's `app/models/enums.py`, enforced on write by a `DomainEnumText` type decorator and at the API edge by Pydantic — which also publishes the set into OpenAPI. A state machine is additionally constrained by an explicit transition table in the service layer | Adding a value becomes a one-line edit rather than an Alembic revision, and — the decisive reason — `ALTER TYPE … ADD VALUE` is irreversible, so a native enum cannot satisfy S14's mandatory `downgrade`. `text` with a `CHECK` is a migration too, so it solves nothing. Self-documentation moves from `\d` to OpenAPI, which is where clients actually read it. For a state machine the transition table is *stricter* than the type it replaces: a Postgres enum accepts `delivered → pending`, a transition table does not |
| S5 | **Soft delete** | A terminal status value (`archived`, `is_active = false`), never a hard `DELETE`, on anything referenced across a boundary | A hard delete breaks snapshots and projections that no foreign key protects. Exception: derived data (`product_chunks`) is hard-deleted, because it is rebuildable |
| S6 | **Optimistic concurrency** | `version integer`, checked and incremented on update, on every aggregate root with concurrent writers | Lost-update prevention without holding locks across a request. The loser gets `409` |
| S7 | **Audit** | Per-service audit table, written by the service layer in the same transaction as the change, carrying actor and correlation id | A trigger cannot see the authenticated actor or the request id. Atomicity means no change can exist without its trail |
| S8 | **`jsonb` use** | Permitted for genuinely open-ended data (`attributes`, `variant_options`, `shipping_address` snapshots, event payloads). Never for anything filtered, joined, or constrained across rows | Rigid columns would be wrong within a week for merchant-defined attributes; but a `jsonb` field that gets filtered is an index and a validation problem waiting to happen |
| S9 | **Naming** | `snake_case`; plural tables; singular columns; `<entity>_id` for references; `is_`/`has_` for booleans; no abbreviations | Eight services written over five weeks; consistency is the only thing that makes them read as one system |
| S10 | **Nullability** | `NOT NULL` by default. A nullable column must have a documented meaning for null | "Unknown", "not yet", and "not applicable" are three different things and should not share a representation |
| S11 | **Constraints in the database** | Business invariants that can be expressed as constraints are (`CHECK (on_hand >= reserved)`, unique `(merchant_id, sku)`, unique `(user_id, idempotency_key)`) | Application-level checks have races. The database is the only thing that can arbitrate concurrent writers |
| S12 | **Outbox** | Identical DDL in every producing service (§3.1.3.2) | One relay implementation, one schema to reason about (B11) |
| S13 | **Dedupe** | `processed_events (consumer_group, event_id)` in every consuming service, written in the same transaction as the effect (§3.1.3.2) | At-least-once delivery makes this the correctness mechanism, not an optimisation |
| S14 | **Migrations** | Per-service Alembic history, explicit step, every migration has a working `downgrade`, additive wherever possible | Services deploy independently; a rollback of one must not require a rollback of another. This rule is why S4 forbids native Postgres enums: `ALTER TYPE … ADD VALUE` has no inverse, so such a revision cannot have a `downgrade` |
| S15 | **No cross-service FK** | R2, restated as a schema rule | The database cannot enforce integrity it cannot see |

**Where the conventions are deliberately broken**, with the reason, so each reads as a decision:

| Break | Where | Why |
| --- | --- | --- |
| `bigserial` instead of `uuid` (S1) | `product_audit_log`, `order_status_history`, `stock_movements`, `outbox`, `notifications.seq` | The id never leaves the database, so UUID's benefits don't apply. At this insert volume, cheap appends, half-size keys and built-in order are what matter (S1) |
| Duplicated data (S-normal-form) | `order_items` snapshots, `product_chunks` metadata | Snapshots are correct domain modelling; projections are rebuildable derived data |
| Hard delete (S5) | `product_chunks`, `embedding_cache`, `raw_events` | Derived, rebuildable, and TTL-bounded |
| Schemaless (S8) | `analytics_db` collections | A document store with genuinely varying per-metric shape — the workload that justifies the NoSQL requirement |
| No `updated_at` (S3) | Append-only tables | Nothing is ever updated |
| Validation outside the database (S11) | Every enum-valued column (S4) | S11 exists because application checks have races. An enum value is not a concurrent-write invariant like `on_hand >= reserved` — it is a spelling check, and a spelling check has no race to lose. R1 already gives each database one service and one role, so the application is the only writer there is to constrain |

**Vocabularies (S4), by owning service.** Each is a `DomainEnum` in that service's `app/models/enums.py`; every value, and every transition, is listed in the service RFC's §3.1.1.2–§3.1.1.3.

| Service | Vocabularies |
| --- | --- |
| `identity` ([RFC 0003](0003-identity-service.md#3112-domain-model)) | `UserRole`, `MerchantStatus` |
| `catalog` ([RFC 0004](0004-catalog-service.md#3112-domain-model)) | `ProductStatus` (including `publishing`), `MediaProcessingStatus` |
| `order` ([RFC 0005](0005-order-service.md#3112-domain-model)) | `OrderStatus`, `PaymentStatus`, `ShipmentStatus`, `IdempotencyState` |
| `inventory` ([RFC 0006](0006-inventory-service.md#3112-domain-model)) | `ReservationStatus`, `StockMovementReason` |
| `notification` ([RFC 0007](0007-notification-service.md#3112-domain-model)) | `NotificationType`, `NotificationChannel`, `DeliveryStatus`, `NotificationDeliveryScope` |
| `assistant` ([RFC 0009](0009-assistant-service.md#3112-domain-model)) | `ChunkType`, `IndexStatus`, `SearchMode`, `MessageRole`, `Intent`, `FinishReason`, `ContentType`, `BulkJobStatus` |

That is twenty-two vocabularies; `analytics` has none (its store is schemaless, S8). The archived rationale for rejecting lookup tables (§5, alternative 13) spoke of "seventeen vocabularies"; the list above is the authoritative count.

###### S4 — enums as text

**How S4 is implemented.** The mechanism is small and belongs in `common`; the vocabularies belong to each service. That split is what keeps S4 compatible with R8 — the library knows *how* a closed value set behaves and never *which* values exist. The package itself is specified in [RFC 0001](0001-platform-and-shared-infrastructure.md); the contract every service relies on is fixed here.

`DomainEnum` is a `StrEnum`, so a member compares equal to its own string, serialises to JSON and Avro with no encoder hook, and is interchangeable with the raw string as a dict key.

```python
# common/smartretail_common/enums.py
class DomainEnum(StrEnum):
    """A closed set of string values, enforced by the application, not by Postgres."""

    @classmethod
    def values(cls) -> tuple[str, ...]:
        return tuple(m.value for m in cls)

    @classmethod
    def has(cls, raw: object) -> bool:
        return isinstance(raw, str) and raw in cls._value2member_map_

    @classmethod
    def parse(cls, raw: object, *, default: "Self | None" = None) -> "Self | None":
        if isinstance(raw, cls):
            return raw
        if isinstance(raw, str):
            return cls._value2member_map_.get(raw, default)
        return default


class UnknownEnumValueError(ValueError):
    """A value outside the permitted set. Names the set, so the error is actionable."""
```

`DomainEnumText` maps such a set onto a `TEXT` column. Its two paths are deliberately asymmetric:

```python
# common/smartretail_common/db_types.py
class DomainEnumText(TypeDecorator[Any]):
    impl = Text
    cache_ok = True          # the cache key reflects enum_class; two instances over
                             # the same enum compare equal, so this is safe

    def __init__(self, enum_class, *, on_unknown="passthrough", fallback=None): ...

    def process_bind_param(self, value, dialect):      # WRITE — strict
        if value is None:
            return None
        if isinstance(value, self.enum_class) or self.enum_class.has(value):
            return str(value)
        raise UnknownEnumValueError(self.enum_class, value)

    def process_result_value(self, value, dialect):    # READ — tolerant
        if value is None:
            return None
        member = self.enum_class.parse(value)
        if member is not None:
            return member
        if self.on_unknown == "raise":
            raise UnknownEnumValueError(self.enum_class, value)
        if self.on_unknown == "fallback":
            return self.fallback
        return value                                   # passthrough: the raw string
```

**Why the write path is strict and the read path is not.** A value this build does not understand may legitimately *exist*; this build has no business *creating* one. Concretely: `catalog` v1.5 writes four hundred rows with `status = 'publishing'`, then is rolled back to v1.4 for an unrelated defect, and v1.4 now reads them. Under a strict read that is four hundred uncaught 500s on a *list* endpoint — the rollback that was meant to restore service widens the incident instead. S14 says a rollback of one service must not require a rollback of another; the read path extends that to *a rollback of a service must not require a rollback of its own data*. This is the one property a native Postgres enum had for free, and giving it up deliberately, at the least damaging point, is part of the price of S4.

`on_unknown` is chosen per column, and the choice follows from what the value is used for:

| Policy | Use for | Why |
| --- | --- | --- |
| `passthrough` (default) | Display and filter columns — `products.status`, `notifications.channel` | The value is rendered or compared, never branched on. A raw string is harmless |
| `fallback` | Anything a state machine reads — `orders.status`, `payment_status`, `shipment_status` | A state machine must never branch on a bare string. Failing closed to the initial state freezes the row loudly instead of corrupting it quietly |
| `raise` | Nothing, currently | Reserved for a column where reading an unknown value is worse than failing |

**Two failure modes that are easy to get wrong**, both verified against this project's pinned SQLAlchemy 2.0 and Pydantic 2.10:

1. `process_bind_param` runs inside the flush, so SQLAlchemy wraps its exception: the caller sees `sqlalchemy.exc.StatementError` with the `UnknownEnumValueError` on `.orig`. The type decorator is therefore a **backstop, not the primary gate** — it fires late, inside the transaction, with a poor error shape. The primary gates are Pydantic at the edge and `transition()` in the service layer.
2. A tolerant ORM read is cancelled out by a strict Pydantic *response* schema — `model_validate` raises `ValidationError` on the unknown value and the 500 reappears one layer later. Both layers must agree. Request schemas stay strict always: the API never accepts a value this build does not know. Response schemas for `passthrough` columns widen to `ProductStatus | str` via a `BeforeValidator`; response schemas for `fallback` columns stay strict, because the ORM already guarantees a member.

**The API contract does not weaken.** Pydantic v2 renders a `DomainEnum` field as a `$defs` entry carrying the full value list, and FastAPI lifts it into OpenAPI unchanged. The published enum is generated from `app/models/enums.py`, so — unlike a hand-maintained `CHECK` or a `\d` listing — it cannot drift from what the service actually accepts.

**Alembic.** Columns are declared `sa.Column("status", sa.Text(), nullable=False, server_default="draft")` — an ordinary, trivially reversible revision. One hook is mandatory in every service's `alembic/env.py`, because autogenerate otherwise renders `DomainEnumText()` **without its required `enum_class` argument**, producing a migration that raises `TypeError` when run and that imports domain types into a layer where they do not belong:

```python
# services/<svc>/alembic/env.py
def render_item(type_, obj, autogen_context):
    """A migration describes the database. A DomainEnumText column is TEXT."""
    if type_ == "type" and isinstance(obj, DomainEnumText):
        autogen_context.imports.add("import sqlalchemy as sa")
        return "sa.Text()"
    return False

context.configure(..., render_item=render_item, compare_type=True)
```

**One documentation affordance, since `\d` no longer carries the values.** The creating migration adds a comment that points at the source of truth rather than duplicating it, so it cannot go stale:

```python
op.execute("COMMENT ON COLUMN orders.status IS "
           "'DomainEnum OrderStatus — see services/order/app/models/enums.py; "
           "transitions in app/utils/state_machine.py'")
```

**What adding a value costs, end to end.** One line in `app/models/enums.py`, plus an edge in the transition table if the column has one. No Alembic revision, no `alembic upgrade head`, no deploy ordering constraint — the column already accepts the string. The OpenAPI enum, the round-trip test, and the seed script all follow from the same source. CI confirms it was genuinely migration-free: `alembic check` must report no pending diff.

**What is measurably given up.** Storage: an enum label is a fixed 4 bytes, a short text value is its length plus a one-byte header, so the delta is roughly 2–13 bytes per row on the status columns — invisible next to the `uuid`s, `numeric`s and `jsonb` snapshots already in those rows. Index size is about 1.5–2× per entry. Equality filtering is unaffected in practice: for a deterministic collation Postgres short-circuits `texteq` to a length check plus `memcmp` and never consults the collation library, so the familiar worry about expensive text comparison applies to ordering and pattern matching, not to `=`. The one real difference is ordering — `ORDER BY status` on a native enum sorted in declaration order, which was often meaningful; on `text` it sorts alphabetically, which is not. Any query relying on that must sort on a timestamp or an explicit `CASE`. The platform has no such query today, and the indexes S4's columns carry are all equality or composite-prefix indexes, which are unaffected.

The argument for S4 in full is in §3.2, *Why enum values are validated in the model, not the database*.

##### 3.1.3.2. Shared tables — `outbox` and `processed_events`

Two tables appear in more than one database. They are **not** one service's tables that other services copied — they are artifacts of conventions S12 and S13, so they are defined here, once, at the same level as the conventions themselves. Each service RFC lists which of them it carries and links back here rather than restating the DDL.

That placement is not cosmetic. Their DDL previously lived inside `order_db`'s section of an archived RFC, and the direct consequence is the four `‡` rows in the matrix below: databases that need one of these tables and were never given it, because the table's home was somebody else's database.

###### `outbox`

Identical DDL in every service that produces events (S12, B11). Written in the same transaction as the state change it announces (A10); read only by `outbox-relay` ([RFC 0001](0001-platform-and-shared-infrastructure.md)), which is the only writer of `published_at` (C1).

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `bigserial` PK | S1 exception — never crosses a boundary, index locality matters at this volume |
| `event_id` | `uuid` | unique — the consumer dedupe key |
| `event_type` | `text` | from the event catalogue (§3.1.2.3) |
| `aggregate_type`, `aggregate_id` | `text`, `uuid` | the Kafka partition key |
| `payload` | `jsonb` | |
| `correlation_id`, `causation_id` | `text` | |
| `client_id` | `text` | nullable — the `X-Client-Id` of the request that caused the change, or null for an event produced by a workflow, a scheduler, or a webhook. Carried into the envelope and used by `notification` for origin-scoped delivery (C21) |
| `created_at` | `timestamptz` | |
| `published_at` | `timestamptz` | nullable — null means unpublished; the relay sets it on broker ack |

Index: partial on `published_at IS NULL`, ordered by `id`, which is the relay's only query. The relay claims a batch with `FOR UPDATE SKIP LOCKED` (C2), so multiple relay instances can run without double-publishing and a stuck row never blocks the ones behind it.

###### `processed_events`

The consumer dedupe table (S13, C4). Written in the same transaction as the effect it guards — at-least-once delivery makes this a correctness mechanism, not an optimisation.

| Column | Type | Notes |
| --- | --- | --- |
| `consumer_group` | `text` | part of PK |
| `event_id` | `uuid` | part of PK |
| `event_type` | `text` | |
| `processed_at` | `timestamptz` | indexed, for the retention sweep |

`analytics_db` carries a MongoDB variant of the same idea — `{_id: {group, event_id}, processed_at}` with a 30-day TTL index. Note the key is `group`, not `consumer_group`; the shapes are deliberately not identical because the stores are not. It is defined in [RFC 0008](0008-analytics-service.md#processed_events-mongo-variant).

###### Which database carries which

Derived from the event catalogue (§3.1.2.3): a **producer** needs an `outbox`, a **consumer** needs `processed_events`.

| Database | `outbox` (S12) | Produces | `processed_events` (S13) | Consumes |
| --- | --- | --- | --- | --- |
| `identity_db` | **yes** `‡` | `identity.user.registered`, `identity.merchant.status_changed` | no | — |
| `catalog_db` | yes | `catalog.product.published` / `.updated` / `.unpublished` | **yes** `‡` | `identity.merchant.status_changed` (and `catalog.product.updated` for cache invalidation) |
| `order_db` | yes | `order.created` / `.confirmed` / `.failed` / `.cancelled`, `payment.*`, `shipment.status_changed` | no | — |
| `inventory_db` | yes | `inventory.reserved` / `.released` / `.stockout` | **yes** `‡` | `catalog.product.published`, `order.cancelled` |
| `notification_db` | no | — | yes | `order.*`, `payment.*`, `shipment.*`, `inventory.stockout`, `identity.user.registered` |
| `analytics_db` | no | — | yes (Mongo variant) | every topic |
| `assistant_db` | **yes** `‡` | `assistant.interaction` | yes | `catalog.product.published` / `.updated` / `.unpublished` |

**On the four `‡` rows.** The archived events RFC mandated these generically — "all producers", "all consumers" — but no RFC ever enumerated them per database, so each was absent from every table list in the RFC set. They are added because the conventions require them and the event catalogue proves the requirement. **Flagged for the Part A review** (§3.3.1, archive/0006 Q3): the alternative reading is that the blanket statement was always sufficient, in which case nothing here is new and only the bookkeeping changes.

##### 3.1.3.3. Cross-service entity model

Boxes are aggregates; solid lines are real foreign keys inside one database; dashed lines cross a boundary and are *not* foreign keys. The diagram shows shape — §3.1.3.4 is the complete list, and only that list is exhaustive.

```
 ┌── identity_db ────────────┐        ┌── catalog_db ──────────────────────┐
 │                           │        │                                    │
 │  users ──┬── merchant_    │        │  categories ──┐ (self-ref tree)     │
 │    │     │   profiles     │        │      │        │                     │
 │    └── refresh_sessions   │        │      ▼        │                     │
 │  outbox ‡                 │        │  products ────┼── product_variants  │
 └───────┬───────────────────┘        │      │        ├── product_media     │
         ┊ merchant_id                │      │        ├── product_audit_log │
         ┊ (projected via event)      │      │        └── product_publish_  │
         └┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄▶ │      │             failures         │
         ┊ customer_id                │  outbox · processed_events ‡        │
         ┊ (claim in token)           └──────┼──────────────────────────────┘
         ┊                                   ┊ product_id / variant_id
 ┌───────▼─── order_db ──────┐               ┊ (snapshot at write)
 │                           │               ├┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┐
 │  orders ──┬── order_items ┼┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┘                     ┊
 │           ├── order_status_history         ┌── inventory_db ─────▼────┐
 │           │                                │                          │
 │  idempotency_keys                          │  stock_items ──┬── reser-│
 │  outbox                                    │       │        │   vations│
 │                                            │       └── stock_movements │
 └───────────┬───────────────┘                │  outbox · processed_ev. ‡│
             ┊ order_id                       └──────────┬───────────────┘
             ┊ (event payload)                           ┊ order_id
             ├┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┘ (event payload)
             ▼
 ┌── notification_db ────┐  ┌── analytics_db (Mongo) ──┐  ┌── assistant_db ────┐
 │                       │  │                          │  │  (+ pgvector)      │
 │  notifications ──┬──  │  │  counters                │  │                    │
 │   delivery_      │    │  │  daily_metrics           │  │  product_chunks ┄┄┼┄▶
 │   attempts       │    │  │  product_stats           │  │  embedding_cache   │
 │  push_subscriptions   │  │  failure_stats           │  │  index_state       │
 │  stream_cursors       │  │  raw_events              │  │  query_log         │
 │  processed_events     │  │  assistant_conversions   │  │  conversations ──┬─│
 └───────────────────────┘  │  processed_events        │  │   messages       │ │
                            └──────────────────────────┘  │  generations       │
                                                          │  token_budgets     │
        ── solid: FK inside one database                  │  bulk_generation_  │
        ┄┄ dashed: cross-boundary reference, NOT a FK     │    jobs            │
        ‡  required by S12/S13, named in no archived      │  prompt_templates †│
           source (§3.1.3.2)                              │  outbox ‡          │
                                                          │  processed_events  │
                                                          └────────────────────┘
```

##### 3.1.3.4. Table index

**38 distinct table definitions, 46 instances across 7 stores.** The two numbers differ because `outbox` appears in five databases and `processed_events` in five (plus the Mongo variant); both are defined once in §3.1.3.2. The `Cols` column is the completeness check — a column lost in transcription shows up as arithmetic. Each table name links to its single definition.

| Service | Store | Table | Key | Cols | What it holds |
| --- | --- | --- | --- | --- | --- |
| `gateway` | — | *none* | — | — | Stateless edge — the deliberate exception to the ownership test (§3.1.1.2); see [RFC 0002](0002-gateway-service.md#313-dataschema-changes) |
| `identity` | `identity_db` | [`users`](0003-identity-service.md#users) | `uuid` | 10 | Accounts, credentials, role, active flag |
| | | [`merchant_profiles`](0003-identity-service.md#merchant_profiles) | `uuid` → `users.id` | 7 | Business name, slug, approval status |
| | | [`refresh_sessions`](0003-identity-service.md#refresh_sessions) | `uuid` | 8 | Hashed refresh tokens, expiry, revocation |
| | | [`outbox`](#outbox) `‡` | `bigserial` | 11 | S12 — shared DDL |
| `catalog` | `catalog_db` | [`categories`](0004-catalog-service.md#categories) | `uuid` | 6 | Self-referencing category tree |
| | | [`products`](0004-catalog-service.md#products) | `uuid` | 16 | Definition, price, status, attributes, `version` |
| | | [`product_variants`](0004-catalog-service.md#product_variants) | `uuid` | 8 | Option combinations, price override. **No stock column** — that is `inventory`'s fact |
| | | [`product_media`](0004-catalog-service.md#product_media) | `uuid` | 11 | Image urls, ordering, storage and processing state |
| | | [`product_audit_log`](0004-catalog-service.md#product_audit_log) | `bigserial` | 6 | Per-change actor, diff, correlation id (S7) |
| | | [`product_publish_failures`](0004-catalog-service.md#product_publish_failures) | `uuid` | 6 | Failed publish workflows as records, not log lines |
| | | [`outbox`](#outbox) | `bigserial` | 11 | S12 |
| | | [`processed_events`](#processed_events) `‡` | `(group, event_id)` | 4 | S13 — consumes `identity.merchant.status_changed` |
| `order` | `order_db` | [`orders`](0005-order-service.md#orders) | `uuid` | 24 | Order, totals, payment and shipment sub-state, Temporal ids, `version` |
| | | [`order_items`](0005-order-service.md#order_items) | `uuid` | 11 | Line snapshots — name, sku, `unit_price`, variant options |
| | | [`order_status_history`](0005-order-service.md#order_status_history) | `bigserial` | 8 | Append-only timeline; the order audit trail |
| | | [`idempotency_keys`](0005-order-service.md#idempotency_keys) | `(key, user_id)` | 9 | One order per key; stored reply replayed verbatim |
| | | [`outbox`](#outbox) | `bigserial` | 11 | S12 |
| `inventory` | `inventory_db` | [`stock_items`](0006-inventory-service.md#stock_items) | `uuid`, unique `variant_id` | 12 | `on_hand`, `reserved`, generated `available`; the lock target |
| | | [`reservations`](0006-inventory-service.md#reservations) | `uuid` | 8 | Held stock per order, with expiry for the sweeper |
| | | [`stock_movements`](0006-inventory-service.md#stock_movements) | `bigserial` | 10 | Append-only signed ledger of every delta |
| | | [`outbox`](#outbox) | `bigserial` | 11 | S12 |
| | | [`processed_events`](#processed_events) `‡` | `(group, event_id)` | 4 | S13 — consumes `catalog.product.published`, `order.cancelled` |
| `notification` | `notification_db` | [`notifications`](0007-notification-service.md#notifications) | `uuid` + `seq` | 16 | Rendered notification, read state, stream cursor, origin client and delivery scope, delivery report |
| | | [`delivery_attempts`](0007-notification-service.md#delivery_attempts) | `uuid` `‡` | 7 | Per-channel send attempts and errors |
| | | [`push_subscriptions`](0007-notification-service.md#push_subscriptions) | `uuid` `‡` | 8 | Specified, unused — makes the offline channel an adapter |
| | | [`stream_cursors`](0007-notification-service.md#stream_cursors) | `uuid`, unique `(user_id, client_id)` | 8 | Per-device SSE replay floor — what stops a fresh session replaying everything |
| | | [`processed_events`](#processed_events) | `(group, event_id)` | 4 | S13 |
| `analytics` | `analytics_db` (Mongo) | [`counters`](0008-analytics-service.md#counters) | `_id` string | — | Monotonic totals |
| | | [`daily_metrics`](0008-analytics-service.md#daily_metrics) | `{date, metric, scope}` | — | Time series per day per metric |
| | | [`product_stats`](0008-analytics-service.md#product_stats) | `product_id` | — | Popularity and revenue ranking source |
| | | [`failure_stats`](0008-analytics-service.md#failure_stats) | `{date, source, reason}` | — | Failed events and workflow issues |
| | | [`raw_events`](0008-analytics-service.md#raw_events) | envelope, TTL 7d | — | Backfill escape hatch for a new metric |
| | | [`assistant_conversions`](0008-analytics-service.md#assistant_conversions) | `uuid` | — | Assistant interaction → order correlation |
| | | [`processed_events`](0008-analytics-service.md#processed_events-mongo-variant) (Mongo variant) | `{group, event_id}` | 3 | S13, TTL 30d |
| `assistant` | `assistant_db` + pgvector | [`product_chunks`](0009-assistant-service.md#product_chunks) | `uuid` | 16 | Chunk text, `vector(1536)`, `tsvector`, pre-filter snapshots |
| | | [`embedding_cache`](0009-assistant-service.md#embedding_cache) | `content_hash` | 7 | Embeddings keyed by hash; survives a reindex |
| | | [`index_state`](0009-assistant-service.md#index_state) | `product_id` | 8 | Indexed catalog version, model, status — staleness detection |
| | | [`query_log`](0009-assistant-service.md#query_log) | `uuid` | 11 | Every search, for the eval set and latency metrics |
| | | [`conversations`](0009-assistant-service.md#conversations) | `uuid` | 9 | Thread, rolling summary, token totals |
| | | [`messages`](0009-assistant-service.md#messages) | `uuid` | 17 | Turn, intent, grounding `sources`, tokens, cost, latency |
| | | [`generations`](0009-assistant-service.md#generations) | `uuid` | 13 | Merchant content drafts; the AI-authored-copy audit trail |
| | | [`token_budgets`](0009-assistant-service.md#token_budgets) | `(scope, date)` | 6 | Spend caps; Redis is the fast path, this is the durable record |
| | | [`bulk_generation_jobs`](0009-assistant-service.md#bulk_generation_jobs) | `uuid` `‡` | 9 | Batch generation with per-item completed/failed counters |
| | | [`prompt_templates`](0009-assistant-service.md#prompt_templates) | `uuid` `‡` | 7 | Versioned prompts as rows, so a change is attributable |
| | | [`outbox`](#outbox) `‡` | `bigserial` | 11 | S12 — produces `assistant.interaction` |
| | | [`processed_events`](#processed_events) | `(group, event_id)` | 4 | S13 |

**Non-database state**, listed so the inventory is complete and so nothing here is mistaken for a gap: Redis keyspaces (rate limits, the idempotency fast path, caches, notification pub/sub, SSE tickets, connection counts and presence, token-budget counters) are specified in [RFC 0001](0001-platform-and-shared-infrastructure.md); object storage (MinIO) holds media binaries keyed by `product_media.storage_key`; Temporal owns workflow execution state, referenced from `order` and `catalog` by `workflow_id` only.

**Reading the index against the rules.** Three things in it are worth checking directly, because this is where the conventions show up as facts rather than prose:

- **Every `bigserial` is in S1's exception list** — `product_audit_log`, `order_status_history`, `stock_movements`, `outbox`, and `notifications.seq`. All append-only, none crossing a boundary.
- **`outbox` and `processed_events` recur verbatim.** That repetition is the design: one relay implementation and one dedupe mechanism, not per-service variants.
- **No table appears under two services.** That is G4 read off the index — the shared-table count is zero by inspection, not by assertion.

**Inferred and added cells — census.** Every `†` and `‡` across the service RFCs' column tables, in one place, so a reviewer can audit the consolidation without opening eight documents. **39 rows carry an inferred type; 11 rows are columns no archived source names.** Counts are rows, not columns — a row such as `created_at`, `updated_at` covers two columns.

| Table | `†` rows | `‡` rows | What the source actually said |
| --- | --- | --- | --- |
| [`product_media`](0004-catalog-service.md#product_media) | 3 | — | Additions `storage_key`, `content_type`, `size_bytes` given as bare names ([archive/0002](archive/0002-core-business-logic-and-workflows.md)) |
| [`product_publish_failures`](0004-catalog-service.md#product_publish_failures) | 5 | 1 | A five-name prose list, no types, **no primary key** ([archive/0002](archive/0002-core-business-logic-and-workflows.md)) |
| [`stock_items`](0006-inventory-service.md#stock_items) | — | 1 | `available` described in prose as a generated column but never listed as one ([archive/0002](archive/0002-core-business-logic-and-workflows.md)) |
| [`delivery_attempts`](0007-notification-service.md#delivery_attempts) | 5 | 1 | Prose list; only `status` typed ([archive/0003](archive/0003-events-and-observability.md)) |
| [`push_subscriptions`](0007-notification-service.md#push_subscriptions) | 6 | 1 | Prose list, **no types at all** ([archive/0003](archive/0003-events-and-observability.md)) |
| [`daily_metrics`](0008-analytics-service.md#daily_metrics) | 3 | — | Document shape ends in a literal `…` ([archive/0003](archive/0003-events-and-observability.md)) |
| [`raw_events`](0008-analytics-service.md#raw_events) | 8 | — | "The full envelope" with no field list, though its TTL index cites `occurred_at`. Expanded from the envelope in §3.1.2.4 |
| [`generations`](0009-assistant-service.md#generations) | 3 | — | One row's Type cell is blank with the note "as above"; resolved against `messages` ([archive/0005](archive/0005-ai-assistant-and-streaming.md)) |
| [`bulk_generation_jobs`](0009-assistant-service.md#bulk_generation_jobs) | 6 | — | Prose list; only `status` typed ([archive/0005](archive/0005-ai-assistant-and-streaming.md)) |
| [`prompt_templates`](0009-assistant-service.md#prompt_templates) | — | 7 | **No columns named anywhere.** Purpose only — the whole table is proposed ([archive/0005](archive/0005-ai-assistant-and-streaming.md)) |

Plus four `‡` **tables** rather than columns — `identity_db.outbox`, `assistant_db.outbox`, `catalog_db.processed_events`, `inventory_db.processed_events` — required by S12/S13 and by the event catalogue, mandated generically by the archived events RFC but never enumerated per database. See §3.1.3.2.

**How to read the concentration.** Nine of the ten rows above are tables the archived source RFCs wrote as a prose sentence instead of a column table. That is the single best predictor of an under-specified table in this RFC set, and it is worth stating as a rule: *a table written as a sentence is a table nobody has designed yet.* `prompt_templates` is the limiting case — a sentence with no columns at all.

`stream_cursors` is the counter-example, and it adds **no rows to the table above**: it arrived fully typed, with its key, its constraint, its index, its retention and its cap already decided, so there was nothing to infer. It was the first table in the set to do so, which is what the rule above predicts when an RFC designs a table instead of mentioning one.

##### 3.1.3.5. Cross-boundary reference kinds

Every dashed line in §3.1.3.3 is one of five kinds. Naming them is what makes "no foreign keys" a design rather than an omission. The fifth arrived with origin-scoped notification delivery and is proposed rather than settled (§3.3.1, archive/0006 Q7).

| Kind | What it is | When used | Example | Consistency |
| --- | --- | --- | --- | --- |
| **Claim** | The id arrives in a verified JWT | Caller identity | `orders.customer_id`, `products.merchant_id` on write | Strong at write time — the token is signed |
| **Snapshot** | Values copied at write time and never updated | Facts whose historical value is the correct one | `order_items.product_name`, `unit_price` | Intentionally frozen. Updating it would be the bug |
| **Projection** | A read model kept current by events | Facts needed for filtering that change occasionally | `product_chunks.product_status`, `stock_items.merchant_id` | Eventual, seconds |
| **Live lookup** | An API call at read time | Facts that must be current and cannot be stale | price and stock in a search response; `order → catalog` at creation | Strong, at the cost of a dependency |
| **Opaque client token** `‡` | A client-minted correlator, validated for shape, always stored and queried scoped under an authenticated `user_id` | Correlating an action back to the device that started it | `notifications.origin_client_id`, `outbox.client_id`, `orders.origin_client_id`, `stream_cursors.client_id` | None, and advisory by design. Never an identity, never an authorisation input, never a join to an owned row |

The decision procedure, applied to any new cross-boundary reference:

```
  Does the historical value matter more than the current one?
        yes ──▶ SNAPSHOT
        no
        │
  Would a few seconds of staleness cause a wrong decision or a wrong display?
        no  ──▶ PROJECTION (event-fed)
        yes
        │
  Is the caller able to proceed without it?
        yes ──▶ PROJECTION, with the staleness documented
        no  ──▶ LIVE LOOKUP, and accept the availability coupling (R5)
```

The fifth kind sits outside that procedure, because it is not a reference to a fact another service owns — nobody owns it. It is a token the client made up, which the platform agrees to carry and compare for equality and nothing else. That is exactly why it needs its own row: calling it a snapshot would imply a source of truth it does not have, and calling it a claim would imply it was verified.

Applied: an order line's price is historical → snapshot. A chunk's `product_status` is a filter that may lag seconds → projection. A displayed price must never be stale → live lookup. Order creation cannot proceed without knowing the product is published and its real price → live lookup, the one such call on a write path in the system.

##### 3.1.3.6. Ownership map

The authoritative answer to "where does this fact live" (R3). One writer each; everything else is one of the reference kinds above. No other service writes these, and no other service reads them except through an API or an event.

| Fact | Sole writer | Read elsewhere as | Replicated to | Mechanism |
| --- | --- | --- | --- | --- |
| User identity, credentials, role | `identity` | Claim | JWT claims | JWT |
| Merchant profile, approval status | `identity` | Projection (in `catalog`) | `catalog` (id and status only) | `identity.merchant.status_changed` |
| Product definition, price, status | `catalog` | Snapshot (`order`), Projection (`assistant`), Live lookup (search display) | `order` (snapshot at purchase), vector store | Sync call at order creation; events for indexing |
| Category tree | `catalog` | Projection | `assistant` chunk metadata | Event |
| Stock on hand, reservations | `inventory` | Live lookup | — (read via API for display and hydration) | API |
| Stock movement ledger | `inventory` | — | — | — |
| Order, payment state, shipment state | `order` | Event payload only | `analytics`, `notification` | Events |
| Idempotency claim | `order` | — | — | — |
| Notification, delivery record, and per-device stream cursor | `notification` | — | — | — |
| Metric rollups | `analytics` | — | — | — |
| Chunks, embeddings, conversations | `assistant` | — | — | — |
| Workflow execution state | Temporal | Referenced by id from `order` / `catalog` | — | `workflow_id` column |

##### 3.1.3.7. Store selection

| Store | Used for | Chosen because |
| --- | --- | --- |
| PostgreSQL 16 | All transactional service data — six services' databases, one instance (A2) | Relational, ACID, mature; every core aggregate has real invariants worth enforcing |
| MongoDB | `analytics_db` — event-derived rollups and time series | Schema-flexible, append-heavy, per-metric documents with varying shape; satisfies Part A's "NoSQL DB" requirement with an actual workload rather than a token one |
| Redis | Rate limiting, idempotency fast path, hot read cache, notification pub/sub and stream tickets, token-budget fast path, Celery result backend | Introduced with three concrete jobs, each justified by an existing path (C13), never speculatively. Keyspaces: [RFC 0001](0001-platform-and-shared-infrastructure.md) |
| pgvector *(default)* / Qdrant *(alternative)* | Embeddings, in `assistant_db` | pgvector (D1, [RFC 0009](0009-assistant-service.md)); avoids adding a container for a catalogue of a few thousand products |
| MinIO | Media binaries | S3-compatible object storage, local ([RFC 0001](0001-platform-and-shared-infrastructure.md), [RFC 0004](0004-catalog-service.md)) |
| Temporal (its own persistence) | Workflow execution state | Owned by Temporal, referenced by `workflow_id` only |

**Shared table count across the whole system: zero.** That is the invariant this RFC exists to protect.

#### 3.1.4. Breaking changes

None externally — the platform has no released API and no production data. This RFC introduces no code and no running schema.

Contract changes that are staged by delivery phase are recorded in the owning service RFC's §3.1.4 so no client codes against an interim behaviour. The two that matter across services:

| Change | From | To | Owner |
| --- | --- | --- | --- |
| `POST /products/{id}/status` with `published` | `501 Not Implemented` with a problem detail, until the publishing workflow exists | `202 Accepted` + `{workflow_id, status: "publishing"}` | [RFC 0004](0004-catalog-service.md) |
| `POST /orders` | does not exist | `202 Accepted` | [RFC 0005](0005-order-service.md) |
| `X-Request-ID` value | a generated id | the OTel trace ID (C12) — header name and behaviour unchanged | [RFC 0002](0002-gateway-service.md), [RFC 0001](0001-platform-and-shared-infrastructure.md) |

Editorial changes a reader of the archived RFCs will notice:

- **The RFC set is reorganised from milestone-based to architecture- and service-based.** The archived milestone RFCs (0001–0005) and the decomposition RFC (0006) are superseded; see [archive/](archive/).
- **Column-level DDL moved from the single decomposition RFC to the owning service RFCs.** Each table is still defined exactly once; this RFC keeps the conventions, the two shared tables and the index.
- The prior prototype's decomposition — eleven mostly-empty services and a frontend — was deleted before the first implementation RFC, and the service inventory above supersedes it in full.

#### 3.1.5. Action Plan

This RFC produces no code. Its output is the RFC set below, the constraints each of them is checked against, and the delivery phasing.

**The RFC set and a reading order.**

| Order | RFC | Scope | Read it for |
| --- | --- | --- | --- |
| 1 | **0000 — System Architecture** (this document) | Cross-cutting architecture | The boundaries, the rules, the conventions, the catalogue, the index |
| 2 | [0001 — Platform & Shared Infrastructure](0001-platform-and-shared-infrastructure.md) | `common`, `outbox-relay`, `temporal-worker` host, `celery-worker`, `indexer-worker` runtime, Kafka + Schema Registry, RabbitMQ, Redis keyspaces, MinIO, Temporal server, OTel/Jaeger/Prometheus/Grafana, Compose, Makefile, Dockerfiles | How every convention here is implemented and hosted |
| 3 | [0002 — `gateway`](0002-gateway-service.md) | The stateless edge | Routing, early rejection, correlation, CORS, rate limits, streaming pass-through |
| 4 | [0003 — `identity`](0003-identity-service.md) | Users, credentials, tokens | Authentication; the claims every other service trusts |
| 5 | [0004 — `catalog`](0004-catalog-service.md) | Products, variants, categories, media, publishing | Flow 1 |
| 6 | [0005 — `order`](0005-order-service.md) | Orders, payment and shipment sub-state, idempotency | Flow 2 |
| 7 | [0006 — `inventory`](0006-inventory-service.md) | Stock, reservations, ledger | The contention path under Flow 2 |
| 8 | [0007 — `notification`](0007-notification-service.md) | Notifications, SSE push, delivery scoping | The consumer end of Flow 2 |
| 9 | [0008 — `analytics`](0008-analytics-service.md) | Event-derived rollups (MongoDB) | The pure-consumer pattern |
| 10 | [0009 — `assistant`](0009-assistant-service.md) | Retrieval and generation | Flow 3 |

**What each RFC must satisfy from this one.**

| RFC | Constraint |
| --- | --- |
| All | R1 enforced by grants; S1–S15 applied; every enum-valued column uses `DomainEnumText` and every `alembic/env.py` carries the `render_item` hook; every producer carries `outbox`, every consumer `processed_events` (§3.1.3.2); no topic outside the catalogue; no column that belongs to another service's fact |
| [0001](0001-platform-and-shared-infrastructure.md) | S4's `DomainEnum` and `DomainEnumText` exist in `common` before the first enum-valued column; `common` holds no vocabulary and no business rule (R8); the envelope, relay and consumer base class implement §3.1.2.4 exactly |
| [0003](0003-identity-service.md), [0004](0004-catalog-service.md) | `merchant_id` as a claim, not a FK |
| [0005](0005-order-service.md), [0006](0006-inventory-service.md) | `order` and `inventory` per §3.1.1.2; `payment`/`shipment` folded, not split; snapshots in `order_items`; S11 constraints on stock; S12 outbox in every producer, written in the state-change transaction even before a relay exists |
| [0007](0007-notification-service.md), [0008](0008-analytics-service.md) | S13 in every consumer; `analytics` a pure consumer (never a caller); projection references fed by events, not polling |
| [0009](0009-assistant-service.md) | `assistant` owns its vectors; chunks are a projection, hard-deletable and rebuildable; indexing consumes `catalog.product.published`, never queries `catalog_db`; generation is a router in the same service, not a sibling |
| Reviews | Check G4 directly — no shared table, no cross-service FK, no cross-database grant — plus the `†`/`‡` census in §3.1.3.4 |

**Delivery phasing.** The order in which components stand up. This is the only place delivery milestones appear; the design above does not depend on them.

| Milestone | Week | Components standing up | Databases, tables and vocabularies created | RFCs exercised |
| --- | --- | --- | --- | --- |
| 1 — Foundation & Core Services | 1 | `gateway`, `identity`, `catalog`, `common`, Postgres, Compose | `identity_db`, `catalog_db`: `users`, `merchant_profiles`, `refresh_sessions`, `categories`, `products`, `product_variants`, `product_media`, `product_audit_log`. `UserRole`, `MerchantStatus`, `ProductStatus` (less `publishing`) | 0001, 0002, 0003, 0004 |
| 2 — Core Business Logic | 2 | `order`, `inventory`, `temporal-worker`, Temporal, MinIO | `order_db`, `inventory_db`: `orders`, `order_items`, `order_status_history`, `idempotency_keys`, `stock_items`, `reservations`, `stock_movements`, `product_publish_failures`, `outbox` ×3 (`catalog`, `order`, `inventory`). Amended: `products` +`publish_workflow_id`; `product_media` +5 columns. `OrderStatus`, `PaymentStatus`, `ShipmentStatus`, `IdempotencyState`, `ReservationStatus`, `StockMovementReason`, `MediaProcessingStatus`; `ProductStatus` +`publishing` | 0001, 0004, 0005, 0006 |
| 3 — Events & Observability | 3 | `notification` (with SSE push), `analytics`, `outbox-relay`, `celery-worker`, Kafka + Schema Registry, RabbitMQ, Redis, MongoDB, OTel Collector, Jaeger, Prometheus, Grafana; gateway rate limiting and streaming pass-through | `notification_db`, `analytics_db`: `notifications`, `delivery_attempts`, `push_subscriptions`, `stream_cursors`; the six Mongo collections other than `assistant_conversions`; `processed_events` ×4 (`catalog` `‡`, `inventory` `‡`, `notification`, `analytics`); `outbox` ×1 (`identity` `‡`). Amended: `products` +`search_document`; `orders` +`notified_at` +`origin_client_id`; `outbox` +`client_id` ×5. `NotificationType`, `NotificationChannel`, `DeliveryStatus`, `NotificationDeliveryScope` | 0001, 0002, 0007, 0008 |
| 4 — Part A Review | — | Nothing new; review of the above | — | — |
| 5 — Retrieval Layer | 4 | `assistant` (retrieval half), `indexer-worker`, vector store (pgvector) | `assistant_db`: `product_chunks`, `embedding_cache`, `index_state`, `query_log`, `processed_events`; `conversations` and `messages` **created empty**. `ChunkType`, `IndexStatus`, `SearchMode` | 0001, 0009 |
| 6 — AI Assistant | 5 | `assistant` (generation half), LangGraph, SSE streaming | `generations`, `token_budgets`, `bulk_generation_jobs`, `prompt_templates`, `outbox` (`assistant` `‡`); `assistant_conversions` (Mongo). Amended: `conversations`, `messages` now written; `counters` and `daily_metrics` gain assistant fields. `MessageRole`, `Intent`, `FinishReason`, `ContentType`, `BulkJobStatus` | 0008, 0009 |
| 7 — Part B Review | — | Nothing new; review of the above | — | — |

No phase drops a table, and every migration is additive — which is what makes S14's mandatory `downgrade` achievable throughout.

**Container count by milestone** — worth stating, because the laptop is a real constraint:

| After | App containers | Infra containers | Total |
| --- | --- | --- | --- |
| M1 | 3 | 1 | 4 |
| M2 | 5 + 1 worker | 3 | 9 |
| M3 | 7 + 3 workers | 11 | 21 |
| M6 | 8 + 4 workers | 12 | 24 |

Milestone 3 is where this becomes uncomfortable. Mitigation is fixed now, not then: **Compose profiles** (`core`, `workflow`, `events`, `observability`, `ai`) so that a developer runs the subset they are working on, the observability profile off by default, and one Postgres instance for all services ([RFC 0001](0001-platform-and-shared-infrastructure.md)).

### 3.2. Rationale

**Why eight services rather than four, or fifteen.** Four (gateway, identity, commerce, ai) would collapse the two boundaries that carry the most design weight — orders against inventory, and transactional against analytical. Fifteen was the prototype's answer and produced services with nothing in them; a boundary with no state behind it costs a Dockerfile, a migration history, a health check, and CI time while teaching nothing. Eight is what the ownership test in §3.1.1.2 yields when applied honestly, and each of the rejections in that table is a place where the count could have inflated for appearance's sake.

**Why `payment` and `shipment` are not services.** This is the decision most likely to be challenged, so it is worth stating plainly. In a real system, a payment service exists because of a compliance boundary — PCI scope isolation — and because it integrates a provider with its own reconciliation, refund, and dispute lifecycle. We have neither: the provider is simulated, and there is no cardholder data anywhere in the system. What would remain is a table of payment intents keyed by order, which is order state. Splitting it would produce a distributed transaction across two databases to record one fact, which is strictly worse than a column. The design keeps the *interface* separate (A9) so that a real provider, or a real service, can be introduced without touching the workflow.

**Why Temporal, Kafka, and Celery all exist rather than one of them.** They are not redundant; they answer different questions, and using one for another's job is a recognisable anti-pattern.

| Tool | The question it answers | Used for | Would be wrong for |
| --- | --- | --- | --- |
| Temporal | "This multi-step process must complete or compensate, and I need to know where it is" | Product publishing, order lifecycle | Broadcasting a fact to unknown consumers — you would have to name every consumer in the workflow |
| Kafka | "This happened; whoever cares should react, and I should not need to know who" | Domain events, analytics feed, search indexing | Orchestrating a saga — no built-in state, no compensation, no visibility into where a process is stuck |
| Celery + RabbitMQ | "Do this one job soon, off the request path" | Sending an email, resizing an image, nightly rollups | Anything that needs a durable multi-step state machine, or an audit of what completed |

Part A's tech stack lists all three. Assigning each a distinct job — rather than picking one and using it for everything — is the substance of that requirement.

**Why analytics gets a different database.** Its access pattern is genuinely different: append-only writes from event consumers, aggregate reads over time windows, and per-metric documents whose shape varies (a revenue series and a stockout rate share no columns). It also must never be able to slow down a checkout, which is easiest to guarantee when it cannot contend for the same connection pool or lock manager. Part A requires a NoSQL store; this gives that requirement a real workload rather than a token table.

**Why data is snapshotted rather than joined.** An order line that reads the current product price is wrong — it would restate history every time a merchant changes a price. Storing name, SKU, and price as they were at purchase is what the domain actually means, and it happens to also be what removes the cross-service join. Where correct modelling and service independence agree, there is no trade being made.

**Why the event catalogue is fixed before any event exists.** Events are the hardest thing to change later, because a schema change has to be compatible with every consumer that has ever been deployed. Naming them in one place, with an envelope and a compatibility rule, means each producer implements a design rather than discovering one topic at a time.

**Why the outbox, at-least-once and Avro are one design.** A handler that commits to Postgres and then produces to Kafka has a window: crash in between, and the state changed but nobody was told; the outbox makes the event part of the same transaction, so the relay's failure mode is a duplicate rather than a loss. Exactly-once in Kafka does not extend to a Postgres write or an email, so the end-to-end property has to come from consumer idempotency anyway — recording `(consumer_group, event_id)` in the effect's transaction is simpler and gives the actual guarantee. And events outlive the code that wrote them: JSON gives no compatibility checking, so a renamed field is discovered by a consumer crash; `BACKWARD` compatibility in the registry fails the incompatible change at registration, in CI. Full treatment: [RFC 0001](0001-platform-and-shared-infrastructure.md).

**Why trace context must cross Kafka.** A trace that stops at the producer answers "the API was fast" and nothing about where the work actually went. Part A §6 asks for failure diagnosis in background workers and notifications — exactly the part that lives past the producer. Carrying `traceparent` with the event and restoring it in the consumer is a few lines that turn five disconnected traces into one.

**Why per-service RFCs, with one architecture RFC.** A single RFC covering the whole system would be several thousand lines and would not survive review. Organising RFCs by delivery milestone was tried and scattered each service across several documents — `order` was described in four, and `products` was defined in three places that drifted independently. Organising by service puts everything about one service — responsibility, domain model, state machines, workflows, configuration, observability, endpoints, events and every column it owns — in one document that one reader can review as a unit, and a change to that service is an edit to that document. What must be consistent *across* services cannot live in any one of them, so it lives here: the boundaries and the test behind them, the rules, the conventions, the catalogue and envelope, the shared tables, and the index that proves the set is complete. Each service RFC holds everything about one service; this RFC holds only what must be consistent across services.

**The decomposition arguments, compressed.** Each is stated in full as a row of the table it justifies in §3.1.1 or §3.1.3.1.

| Decision | Why |
| --- | --- |
| **Ownership, not capability, as the boundary** | Capability splits reliably produce two services writing one fact — "order management" and "fulfilment" both want order status; "catalog" and "pricing" both want price. That forces either a shared table or a distributed transaction. Asking "who writes this?" *before* drawing the boundary cannot produce that situation |
| **A written four-part test rather than intuition** | Intuition produced the eleven-service prototype. Each of those services was individually plausible; the set was not. A written test means a reviewer disagrees with a row and a column, not with a diagram |
| **No foreign key across a boundary** | Lost: the database can no longer prevent an order line referencing a deleted product. Gained: independent deploy, migrate, restore and scale. The lost property is also less valuable than it looks — an order line *should* survive its product being deleted, because it records what was bought, not what is for sale. S5 and the snapshot reference kind turn the constraint into correct domain behaviour |
| **Named reference kinds** | A prohibition alone produces inconsistency: one developer copies a value, another calls an API, a third polls a table. Naming the legitimate kinds makes every cross-boundary reference's consistency guarantee a decision rather than an accident |
| **`numeric` for money (S2)** | `float` cannot represent 0.10. Sum a hundred lines and a stored total eventually disagrees with the sum of its parts by a cent, in a system that also emits revenue analytics and processes refunds. No defence once shipped, and free to avoid |
| **UUID primary keys (S1)** | Ids cross boundaries, appear in event payloads and sit in URLs. A sequential integer leaks volume, collides across services that merge ids in analytics, and cannot be client-generated — which idempotency and correlation ids require. Index cost accepted; S1's exception covers the volume cases |
| **Per-service audit tables (S7)** | An audit row must be in the same transaction as the change it records; any other arrangement admits a state where the change happened and the trail did not. Cross-service transactions do not exist here, so the trail lives with the data |
| **Module boundaries mirroring service boundaries** | The layering seam is what makes one business rule reachable from four callers — an HTTP route, a Temporal activity, an event consumer and a CLI — with one implementation and one set of tests. It is the highest-leverage structural rule in the codebase and the first to erode under deadline pressure |
| **Boundaries and schema answered together** | The two questions have one answer. A boundary that does not coincide with a data-ownership boundary is not a boundary, so each table is defined in the RFC of the service that owns it, and this RFC's index is checked against both |

#### Why enum values are validated in the model, not the database (S4)

This is the one convention that reverses the platform's usual instinct — S11 puts invariants in the database wherever they can be expressed there — so it carries more weight than the others. It is also the argument the `order` and `catalog` RFCs cite.

**The decisive argument is that the old rule made another rule impossible to keep.** S14 mandates a working `downgrade` on every Alembic revision. PostgreSQL has no `ALTER TYPE … DROP VALUE`, in any version, so a revision whose `upgrade` adds an enum label has no inverse. Removing a label means creating a replacement type, rewriting every dependent column under `ACCESS EXCLUSIVE`, re-pointing every dependent function and composite type, dropping the old type and renaming — which is not an inverse at all, because it fails outright if any row already holds the value being dropped, and after a deploy that used the value, every such row does. The honest `downgrade` is therefore either a lie (`pass`) or a destructive operation that can lose data. **Native enums and S14 are mutually unsatisfiable for any vocabulary that will ever grow**, and vocabularies grow: `products.status` gains `publishing` when the publishing workflow arrives. Under the old rule that is a revision nobody can roll back.

One qualification, because the usual version of this argument overstates it: it is often claimed that `ALTER TYPE … ADD VALUE` cannot run inside a transaction block. That was true before PostgreSQL 12 and is **false on this platform's PostgreSQL 16**. What remains true is narrower — the new value cannot be *used* until the adding transaction commits, so adding a label and backfilling to it cannot be one revision. That is an inconvenience. The irreversibility is the defect.

`text`-plus-`CHECK` rescues nothing, because `ALTER TABLE … DROP CONSTRAINT / ADD CONSTRAINT` is a migration too. It has one virtue the native enum lacks — it is trivially reversible — which is why it is the escape hatch in §5 if this rule's premise ever fails.

**What is gained is a stronger guarantee, not a weaker one.** A Postgres enum on `orders.status` constrains a write to a valid label; it accepts all 81 ordered pairs over the nine order states. It always permitted `delivered → pending`, `cancelled → confirmed`, and `pending → confirmed` skipping payment entirely — every write that actually corrupts an order. An explicit transition table accepts 14 of those 81. The enum checked the label and never the edge, and the edge is where the money is.

**What is given up** is enforcement against writers that do not go through the service layer — a hand-run `UPDATE` in `psql`. That is a real loss, recorded in §3.3 rather than argued away. It is bounded by R1: one service and one role per database, so the only unconstrained writer is a human with that role and a terminal, which is why the runbook routes manual status edits through an admin utility that calls the service layer. The trade is sound only while R1 and R3 hold — **if a non-Python writer to any service database is ever introduced, S4 must be revisited**, and the replacement is `CHECK (status IN (…))`.

### 3.3. Tradeoffs

| Trade being made | The cost | Why it is acceptable |
| --- | --- | --- |
| Eight services on one laptop, rather than a modular monolith | Real memory and CPU pressure once the event backbone is up; slower local iteration; debugging spans containers; eight schemas, eight migration histories, eight deployments; no cross-service joins | Compose profiles; one Postgres instance; the distributed-systems learning objective is the point of the exercise, and boundaries are cheapest to build at the start (§5) |
| Designing the whole topology up front | Some of it will be wrong, and this RFC will need amendment | Amendments are cheap and visible. Discovering boundaries one piece of work at a time is not cheaper, it is just less visible |
| Ownership-based split | Some boundaries cut across a user-visible feature — the merchant order view spans `order` and `catalog` | Features change; data ownership does not |
| `payment` and `shipment` folded into `order` | Looks less "microservice" in a review; a real payment integration later needs extraction | The interface boundary (A9) makes extraction mechanical. Building an empty service to look right is the worse failure |
| No cross-service referential integrity | Orphan references are possible; the database cannot prevent them | S5, snapshots and events maintain it at the application level. An orphan is usually the correct outcome |
| Snapshotting product data into orders | Storage duplication; a corrected product name does not propagate to historical orders | That is the desired behaviour for an order record. The alternative restates history |
| Projections lagging seconds | A suspended merchant's products stay searchable briefly | Bounded, and it fails safe. The alternative is a synchronous call on a read path |
| Events fixed in a catalogue before implementation | Some events will be unused; some needed ones will be missing | Additive changes are easy; the catalogue's job is to prevent inconsistent naming and envelopes, which are not |
| Transactional outbox everywhere | Extra table, extra relay component, added publish latency | The alternative is losing events on any crash between the commit and the publish. Non-negotiable for order and payment events |
| Three orchestration tools | Three sets of operational concepts to learn and run | Explicitly required by Part A, and each has a distinct job (§3.2) |
| Simulated payment and shipping | No real integration experience | Neither Part A nor Part B asks for one, and a sandbox account is an external dependency the assignment does not need |
| UUID keys | Larger indexes, worse locality than `bigserial` | Necessary for boundary-crossing ids; S1's exception covers the volume cases |
| Rigid conventions | Occasional local awkwardness | Eight schemas by one developer in five weeks read as one system only if the conventions hold. Deviations are listed (§3.1.3.1), not ad hoc |
| `jsonb` for open-ended fields | No database-level validation; typo'd keys accepted | Variant dimensions are merchant-defined; a fixed schema would be wrong immediately. Validated at the Pydantic layer, never used for filtered fields (S8) |
| Per-service audit tables | Cross-service audit queries need manual correlation | Transactional atomicity with the change is non-negotiable |
| **Enum validity in the model, not the database (S4)** | A direct `psql` write is no longer constrained — `UPDATE orders SET status = 'shippd'` now succeeds. `\d orders` no longer lists permitted values | S14 becomes satisfiable at all, and state machines gain a transition table strictly stricter than the type it replaces. Bounded by R1 (one role per database) and reduced by a `COMMENT ON COLUMN` pointing at `app/models/enums.py`, a per-service round-trip test, and a runbook routing manual edits through an admin utility. Residual risk is a human with the service role at 2am, accepted knowingly |
| The gateway as a stateless exception | Slight inconsistency in the ownership test | Bounded explicitly — no logic, no database. The alternative duplicates edge concerns eight times |
| **Schema split across service RFCs, with the index here** | A reader wanting every column opens eight documents; the index here can drift from the service RFCs and from the code | One definition per column is preserved, and each lives next to the behaviour that uses it. The catalogue-completeness test (§7) compares the index against each service's Alembic head; the `Cols` column makes a dropped column visible as arithmetic |

#### 3.3.1. Challenges and Open Questions

**From archive/0000 (Q1–Q9).** Each now points to the RFC that owns the answer.

| # | Question | Current position | Owner / status |
| --- | --- | --- | --- |
| Q1 | Is `assistant` one service or two (retrieval and generation)? | One service, two routers. They share the conversation store and the embedding client; splitting would create a synchronous hop on every query for no ownership gain | **Closed** by D12 — [RFC 0009](0009-assistant-service.md#331-challenges-and-open-questions) |
| Q2 | pgvector or a dedicated vector database? | pgvector — no new container, and the corpus is small. Revisit if recall or latency disappoints | **Closed** by D1 — [RFC 0009](0009-assistant-service.md#331-challenges-and-open-questions) |
| Q3 | Does `order` call `catalog` synchronously, or keep a local product projection fed by events? | Synchronous call at creation (A13). A projection is the scalable answer but adds a consistency problem to a path that must be correct. Re-examined once events exist; still synchronous | [RFC 0005](0005-order-service.md#331-challenges-and-open-questions) — revisit at Part A review |
| Q4 | Where does inventory come from initially — merchant-set, or an event on publish? | `catalog.product.published` creates a zero-quantity stock row; the merchant sets quantity through `inventory` | [RFC 0006](0006-inventory-service.md#331-challenges-and-open-questions) — decided |
| Q5 | Should the gateway stay hand-written, or move to Traefik/Kong? | Hand-written while it shares `common`. Reconsider when rate limiting and SSE proxying arrive | [RFC 0002](0002-gateway-service.md#331-challenges-and-open-questions) |
| Q6 | How is the assistant's LLM cost bounded? | Per-user and global daily token budgets enforced in `assistant`, with a cached-response path | **Closed** by E10 — [RFC 0009](0009-assistant-service.md#331-challenges-and-open-questions) |
| Q7 | Do we need a cart, given that Part A never mentions one? | No. Orders are placed with a line-item body | Decided — [RFC 0005](0005-order-service.md#331-challenges-and-open-questions) |
| Q8 | Does `analytics` compute rollups on ingest, on read, or both? | On ingest for counters, on read for time-window queries, with a scheduled reconciliation job (C9) | [RFC 0008](0008-analytics-service.md#331-challenges-and-open-questions) — decided |
| Q9 | Should the observability stack be in the default Compose profile? | No — `observability` profile, off by default, on for demos and reviews (C14) | [RFC 0001](0001-platform-and-shared-infrastructure.md#331-challenges-and-open-questions) — decided |

**From archive/0006 (its own Q1–Q7, a separate numbering).** Cited as "archive/0006 Q*n*" to keep them distinct from the table above.

| # | Question | Position | Owner / status |
| --- | --- | --- | --- |
| Q1 | Is folding `payment` into `order` defensible at review? | Yes, on §3.1.1.2 — no compliance boundary, no owned state, no independent change. The interface is separate, so extraction is mechanical if the premise changes | [RFC 0005](0005-order-service.md#331-challenges-and-open-questions) — Part A review |
| Q2 | Does `order` keep the synchronous `catalog` call, or move to a projection? | Keeps it (R5). Price and publication status at purchase must be authoritative; a projection puts a staleness window on exactly the wrong fact | [RFC 0005](0005-order-service.md#331-challenges-and-open-questions) — revisit at Part A review (same question as Q3 above) |
| Q3 | **Are the four `‡` tables (§3.1.3.2) genuinely missing, or is the blanket rule enough?** | They are added, because "all producers"/"all consumers" never survives contact with an implementer reading a per-database table list. The opposite reading is defensible and costs nothing to adopt — delete four rows | This RFC — **Part A review** |
| Q4 | **`prompt_templates` has no specified columns, and `messages` has no `prompt_version`, so the attributability the table exists to provide is not wired up.** Which is right? | The table is proposed with every column marked `‡`; the `messages` column is **not** added, because adding it silently would fix the symptom and hide the design gap | [RFC 0009](0009-assistant-service.md#331-challenges-and-open-questions) |
| Q5 | Are eight services too many for one developer in five weeks? | It is the honest count the test produces, and the delivery phasing introduces them two at a time. The alternative — fewer services than the data model implies — means shared tables | This RFC — Part A review |
| Q6 | Should the `†` inferred types be ratified, or re-decided per service? | Ratified at the Part A review as a block, since each follows mechanically from S1–S15. Any row a reviewer disputes becomes that service RFC's decision | This RFC — Part A review |
| Q7 | **Is `origin_client_id` a legitimate fifth reference kind (§3.1.3.5), or a snapshot of an untrusted hint?** | A fifth kind. Calling it a snapshot implies a source of truth it does not have, and calling it a claim implies verification that never happened. It is a token the client invented, compared only for equality, and always scoped under a verified `user_id` | This RFC, with [RFC 0007](0007-notification-service.md) — **Part A review** |

**Raised by this consolidation.**

| # | Question | Position | Owner / status |
| --- | --- | --- | --- |
| ARC-Q1 | **Where does `traceparent` travel?** C11 requires W3C trace context to cross Kafka "in the event envelope", and the archived event flow shows it written into the outbox row — but neither the envelope in §3.1.2.4 nor the `outbox` DDL in §3.1.3.2 has a field for it. `correlation_id` carries the trace *id* (C12) but not the parent span id or flags | Proposed: the relay carries it as a Kafka message header populated from an added nullable `outbox.traceparent` column, and the envelope stays unchanged. **Not added silently** — it is a DDL change to a shared table and belongs in review | This RFC, implementation in [RFC 0001](0001-platform-and-shared-infrastructure.md) — Part A review |
| ARC-Q2 | **Is the assistant's live hydration a second synchronous cross-service call under R5 / A13?** A13 names exactly one synchronous path (`order → catalog`). D9 hydrates live price and availability per search, calling `GET /api/v1/inventory/{variant_id}` (the archived Flow 3 diagram showed `catalog`) | Proposed: it is a *read-path* live lookup (§3.1.3.5), not a write-path dependency: the assistant can answer from its projection if the lookup fails. A13 is read as governing write paths; R5 holds because the caller can proceed. The degradation behaviour is stated in [RFC 0009 §3.1.1.5](0009-assistant-service.md#3115-dependencies-and-degradation) | This RFC with [RFC 0009](0009-assistant-service.md) — Part B review |

---

## 4. Dependencies/blockers/known issues

**Dependencies.** None of its own. This RFC constrains [RFC 0001](0001-platform-and-shared-infrastructure.md)–[RFC 0009](0009-assistant-service.md), and is constrained by [Part A](../part-a.md)'s named stack (PostgreSQL, a NoSQL store, Redis, Kafka + Schema Registry, Celery/RabbitMQ, Temporal, OpenTelemetry) and the five-week schedule.

**Infrastructure.** Specified, configured and versioned in [RFC 0001](0001-platform-and-shared-infrastructure.md); when each arrives is in the Delivery phasing (§3.1.5).

| Component | Version | Purpose |
| --- | --- | --- |
| PostgreSQL | 16 | Transactional store for six services |
| Temporal + UI | 1.25.x | Workflow orchestration |
| MinIO | latest | S3-compatible media storage |
| Kafka (KRaft) + Schema Registry | 7.7.x | Event backbone |
| RabbitMQ | 3.13 | Celery broker |
| Redis | 7.x | Cache, rate limit, pub/sub, Celery results |
| MongoDB | 7.x | Analytics store |
| OTel Collector, Jaeger, Prometheus, Grafana | current | Observability |
| pgvector extension *(or Qdrant)* | 0.7.x | Embeddings |

**External dependencies.** One only: an LLM provider API key (OpenAI, Groq, or Anthropic) for `assistant`. Every other dependency is a public image or package. This is the single item requiring an account, and it should be obtained before the retrieval work begins rather than during it.

**Blockers.** None.

**Known issues carried across the project.**

- Local resource pressure once the event backbone and observability stack are up (see §3.1.5); mitigated by Compose profiles, not eliminated.
- No TLS anywhere; plain HTTP inside Compose.
- No email or SMS delivery — `notification` records rather than sends on those channels, with an interface allowing a real provider; `in_app` is delivered over SSE.
- Payments and carriers are simulated throughout (A9).
- No load testing; capacity statements are structural rather than measured.
- Eight services is more than a five-week solo project would choose on delivery grounds; the learning objective is what justifies it (archive/0006 Q5).
- No cross-service referential integrity; orphan references are mitigated, not prevented.
- One Postgres instance is a shared local blast radius — an operational choice, reversible by connection string.
- Snapshots mean a corrected product name never reaches historical orders. Intended, and occasionally surprising.
- 39 rows carry an inferred type and 11 are columns no archived source names (§3.1.3.4 census). Every one is a proposal until the Part A review ratifies it.
- Schema conventions are enforced by review and by the tests in §7, not by a linter. A linter for S1–S15 would be a genuine improvement and is out of scope.
- The trace-context carrier (ARC-Q1) is unspecified until reviewed.

---

## 5. Alternate solutions

The two archived RFCs each listed alternatives; they are merged here, with overlaps combined. The *Archive* column keeps each traceable.

| # | Archive | Alternative | Why rejected |
| --- | --- | --- | --- |
| 1 | 0000 Alt 1, 0006 Alt 1 | **Modular monolith**, with enforced internal boundaries, split later | The strongest alternative: it would ship faster, run on less, debug more easily, and model the domain just as well. Rejected because the stated objective is distributed-systems experience; because a monolith makes the event backbone pointless — if every module can call every other in-process, Kafka becomes ceremony, and the requirement that produced it goes unlearned; and because the split is deferred forever once `order` can simply join `catalog`. The cost of boundaries is lowest at the start and rises every week after |
| 2 | 0000 Alt 2, 0006 Alt 2 | **Fifteen fine-grained services**, one per noun (payment, shipment, cart, review, search, media, pricing, merchant, audit…) | This was the prototype's shape. It fails ownership tests 1, 2 and 4 for most candidates (§3.1.1.2), multiplies operational cost with no isolation gain, and makes it impossible for a reviewer to tell a reasoned boundary from a guessed one |
| 3 | 0006 Alt 3 | **Split by capability** (order-management, fulfilment, merchandising…) | Sounds closer to the business and produces shared writes: fulfilment and order-management both write order status. Every overlap becomes a shared table or a distributed transaction |
| 4 | 0006 Alt 4 | **Shared database, schema per service** | Makes the boundary advisory, and an advisory boundary does not survive the first tight deadline. It removes the one property that makes R1 real — a cross-service query failing at connection time |
| 5 | 0000 Alt 3, 0006 Alt 5 | **Event sourcing as the primary persistence model** rather than state tables plus an outbox | A genuinely good fit for orders and inventory, and the natural home for the audit requirement. But it is a large conceptual and implementation cost on a five-week schedule, it makes every simple read query harder, and Part A's requirements are satisfied by state tables with an audit log. The transactional outbox gives most of the integration benefit at a fraction of the cost |
| 6 | 0006 Alt 6 | **A shared canonical data model** with a shared ORM library | Couples every service's deploy to every schema change and reintroduces the shared-model problem separate databases exist to remove. Services share an *event* contract and an *API* contract, never a persistence model (R8) |
| 7 | 0000 Alt 4 | **Choreography instead of orchestration** — no Temporal; the order saga runs as a chain of Kafka consumers each reacting to the previous step | The classic distributed-saga trap: no single place shows where an order is stuck, compensation logic is scattered across consumers, and timeouts have to be reimplemented per step. Part A explicitly requires recovery from partial failures and resumability, which is Temporal's core competence and choreography's weakest point. Part A also lists Temporal in the stack |
| 8 | 0000 Alt 5 | **Kafka for everything**, replacing both Temporal and Celery | See the tool table in §3.2. Kafka has no workflow state, no compensation primitive, and no per-task retry with backoff. Building those on top is reimplementing Temporal badly |
| 9 | 0000 Alt 6 | **Managed services** (Confluent Cloud, Temporal Cloud, a hosted vector DB) to reduce local load | External accounts, credentials, and network dependency for an assignment whose stated target environment is Docker Compose on a laptop, and whose review is a walkthrough on that laptop |
| 10 | 0000 Alt 7 | **Skip the gateway; expose services directly** | Correlation IDs, CORS, rate limiting, and aggregated documentation need one place to live, and the alternative is duplicating them into every service or into an nginx config nobody reviews |
| 11 | 0006 Alt 7 | **Keep native Postgres enums** (the original S4) | Not rejected for the usual reason (that growing a vocabulary costs a migration), which would not on its own overturn S11. Rejected because `ALTER TYPE … ADD VALUE` is irreversible and cannot satisfy S14's mandatory `downgrade` — the two rules cannot both be kept. Its type safety was also shallower than it appeared: it validated the label and never the transition (§3.2) |
| 12 | 0006 Alt 8 | **`text` with `CHECK (status IN (…))`** | *Rejected for now, retained as the escape hatch.* Still needs a migration per value, so it fails the goal outright. But dropping and re-adding a `CHECK` is trivially reversible, so it satisfies S14 — making it the correct fallback **if a non-Python writer to a service database is ever introduced** |
| 13 | 0006 Alt 9 | **A lookup table with a foreign key** per vocabulary | The closest call. It delivers both properties — adding a value is an `INSERT`, and the database still enforces validity. Costs a table and a join for every vocabulary (the archived text said seventeen; §3.1.3.1 lists twenty-two), and S15 forbids a shared one, so each must be duplicated per database and kept in step by hand. That is a standing consistency problem introduced to defend against a writer R1 says does not exist. It is the right answer for a vocabulary that is genuinely *data* — merchant-editable, labelled, ordered, translated — and should be reached for the first time one appears. None of the current vocabularies is one |
| 14 | 0006 Alt 10 | **Leave the schema distributed across RFCs organised by milestone** | The status quo that the archived consolidation replaced. Each milestone RFC read well alone, but `products` was defined in three places, `outbox` in one service's section and inherited by four others by a sentence, and four required tables were missing entirely because no document owned the whole picture (§3.1.3.2). The service-based set keeps the single definition per table while placing it with its owner |
| 15 | *(this reorganisation)* | **Keep one document holding every column** (archive/0006 as it was) | It solved the drift problem at the cost of ~1,300 lines that no service's reviewer read in context, and a column change was an edit far from the behaviour that used it. The service RFCs now hold columns next to their state machines and endpoints; this RFC keeps the conventions, shared DDL and index, and the completeness test (§7) covers the drift risk the single document was protecting against |

---

## 6. Production readiness

"Production" here means the assignment's target: Docker Compose on a developer machine, demonstrated to a mentor. The properties below are structural — designed to hold in principle, demonstrated at that scale.

### 6.1. Scalability and Fault tolerance

- **Stateless services.** All eight hold no in-process state; every one scales with `--scale`. State is in Postgres, Mongo, Redis, or Kafka. `notification` holds connections but no replica-affine state (C20, [RFC 0007](0007-notification-service.md)).
- **Independent scaling.** Each service scales on its own load profile — `catalog` on read volume, `inventory` on write contention, `assistant` on provider concurrency. A shared schema would force them to scale together.
- **Local token verification.** `identity` being down does not stop any other service serving authenticated traffic. It is not a runtime dependency of the request path.
- **Workflow durability.** Temporal persists workflow state, so a worker crash mid-order resumes rather than losing the order. This is the direct answer to Part A's "recovery from partial failures" and "resumable or cleanly reversible".
- **Backpressure.** Kafka consumers lag rather than drop; the gateway rate-limits; Celery queues buffer. Part A's requirement is that the system degrade gracefully rather than collapse, and each layer has a defined behaviour when it falls behind.
- **Idempotency at both ends.** `Idempotency-Key` on order creation prevents duplicate orders from client retries; `event_id` deduplication in consumers prevents double-processing from at-least-once delivery. Part A names both.
- **Inventory contention.** Reservations use row-level locking with a short transaction and a bounded retry, which is the flash-sale path and the one place where a hot row is expected ([RFC 0006](0006-inventory-service.md)).
- **Contained failure / blast radius.** The ownership map determines blast radius directly: `analytics` and `notification` are pure consumers with no synchronous call into the order path, so their failure is invisible to checkout — the main reason they are consumers rather than callees. `catalog` is a live-lookup dependency of order creation (R5, archive/0006 Q2) — the one write-path coupling the design accepts and states.
- **Independent schema evolution.** Per-service migration histories (S14) mean one service's migration never blocks another's deploy or rollback.
- **Rebuildable projections.** Every projection — chunks, analytics rollups, catalog's merchant state — is reconstructible from events or a backfill. No derived store is ever the only copy of anything.
- **Constraints as the last line of defence.** S11's database constraints hold under concurrency where application checks race: `on_hand >= reserved` and the idempotency uniqueness are correctness guarantees, not validations. S4's enum columns are the deliberate exception, and the distinction is the point — a value's spelling has no race to lose.
- **Health and readiness** on every service, with Compose dependency ordering, so nothing accepts traffic before its store is reachable.
- **Known ceilings.** Single Postgres instance, no replicas — a shared local blast radius, and splitting it is a connection-string change, deliberately; single-broker Kafka locally; no partitioning strategy beyond the keys in §3.1.2.3. None of these are schema decisions that would block scaling later.

### 6.2. Security considerations

- **Authentication** — owned by `identity` alone: RS256 JWTs, private key only in `identity`, 15-minute access tokens, rotating refresh tokens with reuse detection. Every other component verifies locally against the published public key. No shared secret whose leak would forge identity. Ownership split in full in §3.1.1.4.
- **Authorisation** — owned by the service that owns the resource, against claims from the verified token, never against a request body or query parameter. The gateway's route-level check is an early rejection, not the boundary. Three roles: `customer`, `merchant`, `admin`. Internal routes accept only service tokens with the right scope; user routes reject service tokens.
- **Tenant isolation** — a merchant's scope is derived from the verified token's `merchant_id` on every catalog, order, and inventory query. If broken once, this silently defeats tenant isolation; it is the single most important recurring test across every service RFC.
- **The boundary is a security boundary.** R1's separate roles and grants mean a compromise of one service cannot read another's data — `catalog` cannot read password hashes, `assistant` cannot read orders. The strongest security property the decomposition provides, and it comes from the same rule as the architectural one.
- **Data minimisation across boundaries.** Snapshots carry only what the consumer needs. `order_items` holds a product name and price, never merchant credentials; event payloads carry ids, never hashes or card data, and no full PII beyond what a consumer requires.
- **Device correlation is advisory.** `X-Client-Id` is client-minted and spoofable; it is always scoped under the authenticated `user_id` and never an authorisation input, so the worst a forger achieves is affecting their own notification toasts.
- **Audit locality (S7)** means a trail cannot be missing for a change that happened. **Soft delete (S5)** preserves referential meaning for snapshots and projections that no foreign key protects.
- **Secrets** — environment only, `.env` gitignored, keys mounted from a gitignored directory. The LLM API key is the only real credential in the system and never leaves `assistant`.
- **Transport** — plain HTTP inside Compose; TLS is an environment concern outside scope.
- **Injection** — ORM with bound parameters throughout; no string-built SQL.
- **Tracing hygiene** — traces may contain URLs and user IDs; request and response bodies are never recorded as span attributes, and headers are allow-listed ([RFC 0001](0001-platform-and-shared-infrastructure.md)).
- **AI-specific** — prompt injection through product text and user messages, retrieval scoped to published products only, no PII in prompts, output filtering, and per-user rate and token budgets. Detailed in [RFC 0009](0009-assistant-service.md).
- **Rate limiting** — at the gateway, with tighter limits on `/auth/login` and on assistant endpoints ([RFC 0002](0002-gateway-service.md)).

### 6.3. Deployment strategy

- **One `docker-compose.yml`** at the repository root with profiles `core`, `workflow`, `events`, `observability`, `ai`. `make up` brings up the default set; `make up-all` brings up everything ([RFC 0001](0001-platform-and-shared-infrastructure.md)).
- **One Makefile** as the single entry point: `up`, `down`, `logs`, `migrate`, `seed`, `test`, `lint`, `fmt`, `reset`.
- **Multi-stage Dockerfiles**, non-root user, one per service, sharing a base layer for `common`.
- **One logical database and one role per service**, created by the Postgres init script with grants on its own database only. This is the mechanism behind R1 and G4.
- **Per-service Alembic histories** (S14), run as an explicit step, never automatically on startup, with a working `downgrade` on every migration. S4 is what makes that last clause achievable.
- **Additive-first migrations.** Expand, migrate, contract — even with no production data, because the habit is what makes the boundaries hold.
- **Independent deployability is the test**: any single service can be rebuilt, migrated and restarted without touching another. Where that is not true, the coupling is a defect. Rollback is per service — separate Alembic histories mean rolling back one service does not require rolling back another.
- **The shared instance is configuration.** Moving a service to its own Postgres is a connection string, because nothing in any schema depends on co-location.
- **Configuration** is twelve-factor and validated at startup by `pydantic-settings`; a missing variable fails the container immediately.

---

## 7. Test Plan

This RFC introduces no code, so its test plan is the testing *strategy* every other RFC instantiates, and the set of tests that prove the architecture holds. A correctness property of this RFC is tested by what the codebase *cannot* do. Harness and CI wiring: [RFC 0001](0001-platform-and-shared-infrastructure.md).

**Layers.**

| Layer | Scope | Where it runs |
| --- | --- | --- |
| Unit | Pure logic — state machines, token handling, chunking, prompt assembly | In-process, no I/O |
| Integration | One service against a real Postgres (or Mongo), `httpx.ASGITransport` | Per service |
| Workflow | Temporal workflows under the test environment, including activity failure injection | `catalog`, `order` |
| Contract | Every produced event validated against its registered schema; every consumer tested against the registered schema; v1↔v2 compatibility in both directions | Every producer and consumer |
| End-to-end | Through the gateway, cold start, no fixtures | Cumulative, at every delivery phase |

**Invariants tested cumulatively — re-run at every delivery phase rather than assumed to still hold.** These are the properties this RFC exists to protect:

1. No service can reach another service's database — asserted by configuration inspection and by a test that the grant does not exist.
2. Every non-public endpoint enforces its role matrix, for all three roles (and internal routes reject user tokens; user routes reject service tokens).
3. A merchant cannot read or modify another merchant's data, on any surface.
4. Every response carries `X-Request-ID`, and a single request is traceable across every service it touches.
5. Once `order` exists: retrying a create with the same `Idempotency-Key` produces one business object.
6. Once events flow: replaying an event produces no duplicate effect.
7. Once events flow: every event published validates against its registered schema.

**Boundary tests.**

1. **No cross-database grant exists.** A test connects as each service's role and asserts every other service's database is unreachable. This is G4 and the single most important test in the project — it makes R1 enforced rather than believed.
2. **No cross-service foreign key exists.** Query `information_schema` in each database; assert every FK's target is a table in the same database.
3. **No service's configuration contains another service's connection string.** Static check over settings and environment.
4. **One writer per fact.** For each row in the ownership map (§3.1.3.6), assert only the owning service's code writes that table — a static check over ORM model imports.

**Convention tests** — static checks over the schema, so S1–S15 are enforced by CI rather than by memory:

| Check | Asserts |
| --- | --- |
| Primary keys | `uuid` everywhere except the S1 exception list |
| Money columns | `numeric(12,2)`, never `float`/`double`; each has an adjacent currency column |
| Timestamps | `timestamptz`, never `timestamp`; `created_at` on every table; `updated_at` on every mutable one |
| Nullability | Every nullable column appears in a documented allow-list (S10) |
| Naming | `snake_case`, plural tables, `<entity>_id` references |
| Outbox and dedupe | Every producer in §3.1.3.2's matrix has an identical `outbox`; every consumer has `processed_events`. **This test is what would have caught the four `‡` tables** |
| Migrations | Every Alembic revision has a non-trivial `downgrade` |
| Enums (S4) | No column's type is `sa.Enum`; no `CHECK` constraint names a status value; `pg_type` holds no enum type in any service database |
| Enum round trip (S4) | Per service: discover every `DomainEnum` subclass in `app/models/enums.py`, and for every member of every `DomainEnumText` column, write it to real Postgres and read it back. Adding a value extends this test with no edit |
| Enum write strictness (S4) | An unknown value is rejected on write and leaves no row behind. The error arrives as `sqlalchemy.exc.StatementError` with `UnknownEnumValueError` on `.orig`, because `process_bind_param` runs inside the flush |
| Enum read tolerance (S4) | A value written by a newer deploy is readable by an older one — the rollback case. Write an unknown value through raw SQL, read it through the ORM |
| `common` vocabulary (R8, S4) | A grep of `common` for any domain status literal returns nothing — the library holds the mechanism, services hold the values |
| **Catalogue completeness** | Every table in a service's Alembic head appears in §3.1.3.4's index and in that service RFC's §3.1.3, and vice versa. The index's `Cols` count matches the model's column count. This is what keeps the RFC set from drifting from the code it specifies |
| Event catalogue completeness | Every topic produced or subscribed to in code appears in §3.1.2.3, with the declared producer and consumers |

**Model tests.**

- Snapshot immutability: changing a product's name and price leaves every existing `order_item` unchanged.
- Projection convergence: after a source change and event propagation the projection matches; after a deliberate divergence, a rebuild restores it.
- Soft delete: archiving a product leaves order history intact and readable.
- Constraint enforcement: each S11 invariant is tested by attempting to violate it **concurrently**, not just sequentially.
- Reference kinds: for each dashed edge in §3.1.3.3, assert the declared consistency behaviour — a snapshot does not change, a projection converges, a live lookup reflects the current value immediately.
- State machine totality (S4): the transition table's keys are exactly the members of its `DomainEnum`, every target is a known state, every state is reachable from the initial one, and the terminal set is the expected one.
- Illegal transitions (S4): every pair *not* in the transition table is parametrised and asserted to raise, with the entity unmutated. For `orders` that is 67 named assertions — the writes a native Postgres enum always accepted.

**Cross-cutting behaviour tests.**

- A checkout produces exactly one trace containing spans from gateway, order, inventory, the Temporal workflow, the relay, and the notification consumer — asserted by querying the Jaeger API for the trace ID and checking the service set, not by looking at the UI.
- Every log line emitted during one request carries the same trace ID (C12).
- Relay crash between produce and `published_at` produces a duplicate that consumers absorb — no loss, no double effect.

**The end-to-end scenario grows with the project** and is each review's spine. Each stage adds to the one before:

1. register → login → create category → create product → list → status transitions.
2. \+ set stock → publish (workflow completes) → place order → reserve → pay → confirm; and the failure path with compensation.
3. \+ assert events published and consumed, notification recorded and pushed, analytics counters moved, one trace spans gateway → order → inventory → consumer.
4. \+ product indexed on publish → semantic search returns it.
5. \+ ask the assistant about it and receive a streamed, grounded answer citing that product.

**Review checks** — at the Part A and Part B reviews, by hand, because no test covers them:

- Does every service still pass all four ownership tests?
- Has any service acquired a second writer for a fact it does not own?
- Does `common` contain a business rule (R8)? For S4 this is mechanical: a grep of the package for any status literal must return nothing, and that grep is itself a unit test.
- Has any router acquired direct ORM access, or any service-layer function an HTTP concern?
- Has any new cross-boundary reference been added without declaring its kind?
- **Has every `†` and `‡` cell in the §3.1.3.4 census been ratified or re-decided?**

---

## 8. Impact Analysis

**On the schedule.** Five implementation weeks for eight services is tight, and this document's main practical function is to remove design time from the later weeks. The risk it addresses is spending a week deciding whether analytics is a service.

**On each RFC.**

| RFC | What this RFC gives it | What it constrains |
| --- | --- | --- |
| [0001](0001-platform-and-shared-infrastructure.md) Platform | The envelope, catalogue, outbox and dedupe DDL, S4 contract and API conventions to implement once | Must implement them exactly; `common` may not acquire a rule or a vocabulary (R8) |
| [0002](0002-gateway-service.md) `gateway` | Its bounded exception to the ownership test; the surface map | No business logic, no database; not the security boundary |
| [0003](0003-identity-service.md) `identity` | Sole ownership of authentication and signing; the service-token path | Must not know about products; events it produces are fixed in the catalogue |
| [0004](0004-catalog-service.md) `catalog` | Flow 1's shape; product events in the catalogue | Must not add stock columns (R3) or read `identity_db` |
| [0005](0005-order-service.md), [0006](0006-inventory-service.md) `order`, `inventory` | Their boundaries, the payment/shipment folding decision, the Temporal-vs-Kafka split | Must use the outbox pattern from their first state change, so the relay is a configuration change rather than a rewrite |
| [0007](0007-notification-service.md), [0008](0008-analytics-service.md) `notification`, `analytics` | The full event catalogue and envelope, ready to consume | Pure consumers; may not invent topic names outside the catalogue without amending this RFC |
| [0009](0009-assistant-service.md) `assistant` | Scoped as one service; indexing driven by events rather than polling; the interaction event already named | Must consume `catalog.product.published`, not query `catalog_db` |
| Reviews | A single document a reviewer can read to see the whole design, its rejected alternatives, the method, the reference-kind taxonomy, the conventions, and an explicit list of what the consolidation proposed rather than transcribed (§3.1.3.4 census) | — |

**On the codebase.** The conventions are why eight schemas written over five weeks read as one system. The layering rule (§3.1.1.7) is why a business rule has one implementation reachable by four callers.

**On the RFC set.** A column has exactly one definition, in the owning service RFC; this RFC indexes them. A type change is one edit.

**If a premise changed.**

| If | Then |
| --- | --- |
| A real payment provider is introduced | `payment` becomes a service. The interface boundary makes it an extraction, not a rewrite |
| Multi-warehouse inventory is required | `stock_items` gains `location_id` in its unique key. No boundary moves |
| The corpus grows by two orders of magnitude | `assistant`'s vector store moves behind the `VectorStore` protocol to a dedicated engine. No boundary moves |
| A non-Python writer to a service database appears | S4 is revisited; the replacement is `CHECK (status IN (…))` (§5, alternative 12) |
| Reviews or a cart are required | New services, each tested against the same four criteria |
| The project became a team of four | The boundaries already support it — four owners, eight services, no shared schema |

**Risks at project level.**

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| The event-backbone phase overruns — Kafka, Celery, three observability tools, and two new services at once is the heaviest phase by far (§3.1.5) | **High** | High | Outbox tables and structured logging land earlier; `notification` kept focused; the observability stack is configuration, not code; drop `analytics` read APIs to a minimum if time is short |
| Laptop cannot run the full stack | Medium | High | Compose profiles from the start; one Postgres; observability off by default |
| Boundary erosion under deadline pressure (a cross-service query "just this once") | Medium | **High** | Separate DB roles make it fail at connection time, not at review time; the boundary test is in CI |
| A second writer appearing for an owned fact | Medium | High | The ownership map is explicit; the static check in §7 catches it |
| Event schema churn breaking consumers | Medium | Medium | Schema Registry with `BACKWARD` compatibility; additive-only changes |
| LLM provider cost or rate limits | Medium | Medium | Small embedding model, cached embeddings, token budgets, a local fake provider for tests |
| **The RFC set drifting from the code it specifies** | **High** | **High** | The catalogue-completeness test in §7 compares §3.1.3.4 and each service RFC against each service's Alembic head in CI |
| This RFC drifting out of date as work teaches us things | **High** | Low | Amendment is expected. §11 records amendments with dates; a service RFC that contradicts this one must say so explicitly |
| Conventions drifting across eight schemas | High | Medium | The convention tests are automated; drift is a CI failure, not a review observation |
| An inferred `†` type turning out wrong in implementation | Medium | Low | Each is marked and counted (§3.1.3.4); the cost is one migration in the service that hits it |
| The eight-service count being judged excessive | Medium | Low | The method and every rejection are recorded, so the disagreement is specific and answerable (archive/0006 Q1, Q5) |
| `common` accumulating domain logic | Medium | Medium | R8, checked at every review and by the vocabulary grep |
| The layering seam eroding, making rules unreachable from activities and consumers | Medium | High | Reviewed at each review; highest cost of erosion, lowest visibility |

**On reviewers.** The Part A review should be able to check this document against the code and find: eight services or a documented reason for a different number, no shared tables, every event in the catalogue, and every deferral traceable to the RFC that owns it.

---

## 9. Useful Links/References

**Project documents**

- [Part A — platform requirements](../part-a.md)
- [Part B — GenAI layer](../part-b.md)
- [Milestones](../milestones.md) · [Execution Guidelines](../execution-guidelines.md)

**The RFC set**

- [RFC 0001 — Platform & Shared Infrastructure](0001-platform-and-shared-infrastructure.md)
- [RFC 0002 — `gateway`](0002-gateway-service.md) · [RFC 0003 — `identity`](0003-identity-service.md) · [RFC 0004 — `catalog`](0004-catalog-service.md) · [RFC 0005 — `order`](0005-order-service.md)
- [RFC 0006 — `inventory`](0006-inventory-service.md) · [RFC 0007 — `notification`](0007-notification-service.md) · [RFC 0008 — `analytics`](0008-analytics-service.md) · [RFC 0009 — `assistant`](0009-assistant-service.md)

**Archived (superseded) RFCs**

- [archive/0000 — System Architecture](archive/0000-system-architecture.md) · [archive/0006 — Service Decomposition & Data Model](archive/0006-service-decomposition-and-data-model.md)
- [archive/0001 — Foundation & Core Services](archive/0001-foundation-and-core-services.md) · [archive/0002 — Core Business Logic & Workflows](archive/0002-core-business-logic-and-workflows.md) · [archive/0003 — Events & Observability](archive/0003-events-and-observability.md) · [archive/0004 — Retrieval Layer](archive/0004-retrieval-layer.md) · [archive/0005 — AI Assistant & Streaming](archive/0005-ai-assistant-and-streaming.md)

**Technical references**

- Transactional outbox pattern — https://microservices.io/patterns/data/transactional-outbox.html
- Saga pattern — https://microservices.io/patterns/data/saga.html
- Database per service — https://microservices.io/patterns/data/database-per-service.html
- Shared database anti-pattern — https://microservices.io/patterns/data/shared-database.html
- Temporal documentation — https://docs.temporal.io/
- Confluent Schema Registry compatibility types — https://docs.confluent.io/platform/current/schema-registry/fundamentals/avro.html
- OpenTelemetry specification — https://opentelemetry.io/docs/specs/otel/
- W3C Trace Context — https://www.w3.org/TR/trace-context/
- RFC 9457, Problem Details for HTTP APIs — https://www.rfc-editor.org/rfc/rfc9457
- IETF draft, The Idempotency-Key HTTP Header Field — https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/
- Stripe idempotent requests (the reference implementation of the header contract) — https://docs.stripe.com/api/idempotent_requests
- PostgreSQL numeric types — https://www.postgresql.org/docs/16/datatype-numeric.html
- PostgreSQL `uuid` and `gen_random_uuid()` — https://www.postgresql.org/docs/16/functions-uuid.html
- pgvector HNSW indexing — https://github.com/pgvector/pgvector#hnsw
- Aggregate design rules (Vernon) — https://www.dddcommunity.org/library/vernon_2011/
- Evans, *Domain-Driven Design* — bounded contexts, aggregates, context mapping
- Vernon, *Implementing Domain-Driven Design* — aggregate design rules
- Newman, *Building Microservices*, 2nd ed. — decomposition and the database-per-service pattern
- Kleppmann, *Designing Data-Intensive Applications* — chapters 11 and 12, derived data and stream processing

---

## 10. Related tickets/issues

| ID | Title | Maps to |
| --- | --- | --- |
| — | RFC 0000 — System Architecture | This document |
| — | Platform & shared infrastructure | [RFC 0001](0001-platform-and-shared-infrastructure.md) |
| — | Service RFCs | [0002](0002-gateway-service.md), [0003](0003-identity-service.md), [0004](0004-catalog-service.md), [0005](0005-order-service.md), [0006](0006-inventory-service.md), [0007](0007-notification-service.md), [0008](0008-analytics-service.md), [0009](0009-assistant-service.md) |
| — | Part A Review | Reviews this RFC and 0001–0008: G4, the §7 review questions, the §3.1.3.4 census, archive/0006 Q3/Q5/Q6/Q7, ARC-Q1 |
| — | Part B Review | Reviews 0009, and this RFC's rules as they apply to `assistant` (ARC-Q2) |
| — | Delivery phasing | §3.1.5 |

---

## 11. Notes

- **On the eight-service count.** It is a design decision, not a requirement. Neither Part A nor Part B states a number. The count is defended by the ownership test in §3.1.1.2 and by the explicit rejections in the same section; a reviewer who disagrees should disagree with a specific row.
- **On the rejections being the substance.** The accepted services are the easy half. The rejected candidates in §3.1.1.2 are where the decomposition was actually decided, and `payment` is the one most likely to be challenged.
- **On conventions as a force multiplier.** Fifteen rules that take an hour to agree are what let eight schemas written across five weeks look like one system. The cost of not agreeing them shows up mid-project, as three timestamp conventions and two ways of representing money.
- **On the honest position.** Eight services is more than a five-week solo project would choose on delivery grounds. That is a real tension, stated rather than hidden (archive/0006 Q5).
- **On what this document is not.** It is a specification, not an implementation record. If a table in the index or a service RFC disagrees with a service's Alembic head, the head is right and the RFC is stale — and the catalogue-completeness test in §7 exists to make that state short-lived.
- **On domain wording.** The source milestone list ([milestones.md](../milestones.md)) is phrased for an e-learning platform ("User & Course management", "student/instructor", "instructor content generation (summaries, quizzes)"). [Part A](../part-a.md) specifies retail commerce, so every RFC reads *course* → *product*, *student* → *customer*, *instructor* → *merchant*, *quizzes* → *promotional copy and FAQs*, with `admin` as the third role Part A §1 requires. The source wording can be checked against [milestones.md](../milestones.md) directly. Milestone structure and expected outcomes are unchanged.
- **On decision numbering.** Archived decision IDs (A*, B*, C*, D*, E*, R*, S*, Q*) are kept exactly, so they stay traceable to [archive/](archive/). Two archived series collide with this RFC's: archive/0001's A1–A8 (mapped in §3.1.1.5) and archive/0006's Q1–Q7 (kept in their own table in §3.3.1 and cited as "archive/0006 Q*n*"). New decisions and questions carry a prefix — `ARC-` here, and `PLT-`, `GW-`, `IDN-`, `CAT-`, `ORD-`, `INV-`, `NTF-`, `ANL-`, `AST-` in RFCs 0001–0009.
- **On amendment.** This RFC is expected to change as work executes. Amendments are appended below with a date and a reason rather than edited silently, so that the review can see what was learned.
- **Amendment log.**

  | Date | Section | Change | Reason |
  | --- | --- | --- | --- |
  | Sep 27, 2026 | §3.1.3 (archive/0006) | Consolidated the complete column-level schema from the milestone RFCs into one document | One definition per column. `products` had been defined in three places |
  | Sep 27, 2026 | §3.1.3.2 | Added `identity_db.outbox`, `assistant_db.outbox`, `catalog_db.processed_events`, `inventory_db.processed_events` | Required by S12/S13 and the event catalogue; mandated generically but never enumerated per database (archive/0006 Q3) |
  | Sep 27, 2026 | §3.1.3.4 | Specified `prompt_templates` for the first time; 39 rows given inferred types | The source RFCs wrote ten tables as prose sentences rather than column tables (census, §3.1.3.4) |
  | Sep 27, 2026 | §2.2 (archive/0006) | Removed "column-level DDL" from the non-goals | It had become the point of that document |
  | Sep 29, 2026 | All | RFC set reorganised from milestone-based to architecture + service-based; 0006 merged here; per-table columns moved to service RFCs | Each service RFC holds everything about one service; this RFC holds only what must be consistent across services |
  | Sep 29, 2026 | §3.1.2.1, §3.1.2.2 | Internal routes are mounted under `/internal/v1/*` and never routed by the gateway; service-token scopes are named `<service>:internal` | Reconciliation across the service RFCs: `inventory`'s internal routes had sat under `/api/v1`, leaving `stock-items` reachable at the edge, and scope names differed between 0003, 0004, 0005 and 0006 |
  | Sep 29, 2026 | §3.1.1.4 | Recorded the service-token path (client-credentials JWT with `scope`) for callers with no user in context, alongside user-token forwarding for `order → catalog` | archive/0000 reserved "a call with no user in context" for an amendment here; archive/0002 introduced it for Temporal activities without amending this section |
  | Sep 29, 2026 | §3.1.2.1, §3.1.2.3 | Surface map widened to `/api/v1/search/*` and `/api/v1/webhooks/shipment`; `catalog` added as a consumer of `catalog.product.updated`; topic count stated as fourteen rows / sixteen topics | Brought into line with the archived retrieval and events RFCs, which had extended both without amending the catalogue |
  | Sep 29, 2026 | §3.3.1 | Added ARC-Q1 (trace-context carrier) and ARC-Q2 (assistant hydration under R5/A13) | Inconsistencies surfaced by merging the archived RFCs; recorded rather than resolved silently |

- **Definition of done for this RFC.** Goals G1–G12 satisfied; RFCs 0001–0009 exist and none contradicts it without saying so; the boundary and convention tests in §7 implemented and green in CI; the §3.1.3.4 census ratified at the Part A review; reviewed and moved to **DONE**.

---

## 12. Comments

*Reviewer comments go here. Please raise objections against a specific row: a service inventory row or ownership-test candidate in §3.1.1, a boundary rule R1–R8, a decision A1–A14, a topic in the catalogue, a schema convention S1–S15, a `†` or `‡` cell in the §3.1.3.4 census, an open question (Q1–Q9, archive/0006 Q1–Q7, ARC-Q1–Q2), or a rejected alternative in §5 — so the discussion attaches to something specific.*

| Reviewer | Section | Comment | Resolution |
| --- | --- | --- | --- |
| | | | |
