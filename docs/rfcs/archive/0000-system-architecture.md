# RFC 0000 — System Architecture

> **SUPERSEDED — archived Sep 29, 2026.** The RFC set was reorganised from milestone-based to architecture- and service-based. This document is kept for history only and is no longer maintained. Its content now lives in: [0000-architecture](../0000-architecture.md). See [docs/README.md](../../README.md).

| | |
| --- | --- |
| **Owner** | @muhammad.adnan |
| **Collaborators** | SarwanAhmed |
| **Status** | **DRAFT** · IN-REVIEW · DONE |
| **Created** | Sep 22, 2026 |
| **Related Epic or story** | All milestones ([milestones.md](../../milestones.md)), Parts A and B ([part-a.md](../../part-a.md), [part-b.md](../../part-b.md)) |
| **Companion to** | [RFC 0006 — Service Decomposition & Data Model](0006-service-decomposition-and-data-model.md) |

> **Read this alongside [RFC 0006](0006-service-decomposition-and-data-model.md).**
> This RFC answers *how the system runs* — topology, flows, events, cross-cutting
> decisions. RFC 0006 answers *where the boundaries fall and what the data looks
> like*: the decomposition method with every rejected candidate, **the complete
> schema — every table and every column in the platform** — and the conventions
> (S1–S15) every service follows. Where the two overlap — the service inventory
> in §3.1.1 and the ownership map in §3.1.3 — this document is the summary and
> RFC 0006 is the specification.

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

**SmartRetail system architecture — the complete service topology, data ownership map, event catalogue, and cross-cutting decisions for Parts A and B.**

This RFC is the umbrella design for the whole platform. It answers "how many services are there, what does each one own, and how do they talk to each other" once, for all six implementation milestones, so that the per-milestone RFCs (0001–0005) can confine themselves to the detail of what is actually being built that week.

Nothing here is implemented directly. Every component named below arrives in the milestone this RFC assigns it to, under its own RFC.

---

## 2. Overview

[Part A](../../part-a.md) specifies an event-driven commerce backend: catalog, orders, inventory, fulfilment, notifications, and analytics, built on FastAPI, PostgreSQL, Redis, Celery/RabbitMQ, Kafka, and Temporal, with OpenTelemetry-based observability. [Part B](../../part-b.md) adds a GenAI layer on top: semantic product retrieval and a conversational shopping assistant.

Those two documents describe the system by requirement. Neither says how many services there are or where a boundary falls, which means those are our decisions — and they are the decisions the milestone reviews will examine most closely. Making them piecemeal, one milestone at a time, produces boundaries that are artefacts of the order the work happened in rather than of what the data actually looks like.

This RFC therefore fixes the target shape up front: **eight services**, each justified by a distinct piece of state it owns, plus three non-HTTP worker components and one shared library. It also fixes the cross-cutting conventions — authentication, error format, correlation IDs, event envelope, idempotency — that every service has to agree on, because retrofitting a convention across eight services is exactly the kind of work that gets skipped.

### 2.1. Goals

| # | Goal | How it is verified |
| --- | --- | --- |
| G1 | A complete service inventory, each entry justified by the state it owns | §3.1.1; every service in the table has a database or store that nothing else writes |
| G2 | A data ownership map with no shared tables and no cross-service foreign keys | §3.1.3; grep for cross-database connection strings finds nothing at any milestone |
| G3 | A named event catalogue with a fixed envelope and versioning rule | §3.1.2; every event produced in Milestone 3 onwards appears in the catalogue |
| G4 | End-to-end flows for the three critical paths — publish, checkout, assistant query | §3.1.1; each flow is traceable across services by a single correlation ID once Milestone 3 lands |
| G5 | Cross-cutting conventions agreed once: auth, errors, pagination, config, idempotency, tracing | §3.1.2; implemented in `libs/common`, reused rather than reimplemented per service |
| G6 | A milestone→component map, so no milestone is surprised by what it has to stand up | §3.1.5 |
| G7 | Every deferral recorded with the milestone that owns it, so scope boundaries are decisions and not omissions | Non-goals table and each RFC's §2.2 |

### 2.2. Non-goals

| Excluded | Why |
| --- | --- |
| Implementation detail — endpoint bodies, column types, test cases | Belongs to RFCs 0001–0005. This document stops at the boundary and the contract |
| Kubernetes, cloud deployment, managed services, CI/CD pipelines | The target environment for the whole assignment is Docker Compose on a developer machine ([Execution Guidelines](../../execution-guidelines.md)) |
| Multi-region, multi-tenancy beyond merchant scoping, disaster recovery | No requirement asks for them, and they would distort every boundary below |
| A frontend | The API and its OpenAPI documentation are the deliverable |
| Real payment or carrier integrations | Both are simulated. See decision A9 |
| Performance targets and load testing | No traffic figure is specified anywhere in Parts A or B. Capacity claims in §6.1 are structural, not measured |

---

## 3. Solution Overview

### 3.1. Specification

#### 3.1.1. Architectural changes

**The complete service inventory.**

| # | Service | Owns (state nothing else writes) | Store | Arrives in | RFC |
| --- | --- | --- | --- | --- | --- |
| 1 | `gateway` | Nothing — stateless edge | — | M1 | 0001 |
| 2 | `identity` | Users, credentials, roles, merchant profiles, refresh sessions | `identity_db` | M1 | 0001 |
| 3 | `catalog` | Products, variants, categories, media, catalog audit trail | `catalog_db` | M1 | 0001 |
| 4 | `order` | Orders, order lines, order state, payment and shipment sub-state, idempotency keys | `order_db` | M2 | 0002 |
| 5 | `inventory` | Stock levels, reservations, stock movement ledger | `inventory_db` | M2 | 0002 |
| 6 | `notification` | Notification records and delivery attempts | `notification_db` | M3 | 0003 |
| 7 | `analytics` | Event-derived rollups and metric series | `analytics_db` (MongoDB) | M3 | 0003 |
| 8 | `assistant` | Conversations, messages, generation records | `assistant_db` + vector store | M5–M6 | 0004, 0005 |

**Non-HTTP components.** These are deployable containers but not services in the sense above — they hold no API surface and own no schema of their own.

| Component | Role | Arrives in |
| --- | --- | --- |
| `temporal-worker` | Hosts `ProductPublishingWorkflow` and `OrderWorkflow`; activities call service-layer functions | M2 |
| `celery-worker` | Background tasks off RabbitMQ — notification dispatch, media processing, scheduled rollups | M3 |
| `indexer-worker` | Consumes `product.published`, chunks and embeds product text, writes the vector store | M5 |
| `outbox-relay` | Reads each service's transactional outbox and publishes to Kafka | M3 |
| `libs/common` | Shared non-domain code: auth, config, logging, errors, pagination, DB session, event envelope, tracing setup | M1, extended per milestone |

**Target topology at the end of Milestone 6.**

```
                                 ┌──────────────────────────────┐
     client ─────────────────────▶│  gateway  (:8000)            │
     HTTP / JSON / SSE            │  JWT verify · routing        │
                                  │  correlation ID · CORS       │
                                  │  rate limit (M3)             │
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
       │   Kafka + Schema Registry   — the event backbone (M3)           │
       └───┬───────────────────┬──────────────────────┬─────────────┬────┘
           │                   │                      │             │
   ┌───────▼──────┐   ┌────────▼───────┐    ┌─────────▼──────┐  ┌───▼────────┐
   │ notification │   │ indexer-worker │    │  analytics     │  │ inventory  │
   │   (:8005)    │   │     (M5)       │    │  consumers     │  │ consumers  │
   └───────┬──────┘   └────────────────┘    └────────────────┘  └────────────┘
           │
   ┌───────▼──────┐   ┌────────────────┐   ┌──────────────────────────────────┐
   │ RabbitMQ +   │   │   Temporal     │   │ OTel Collector → Jaeger          │
   │ celery-worker│   │   + worker     │   │               → Prometheus/Grafana│
   └──────────────┘   └────────────────┘   └──────────────────────────────────┘
```

**Why these eight and not others — the ownership test.** A service exists here only if there is a piece of state that it alone writes and that has a different consistency, volume, or lifecycle profile from its neighbours. Applying that test:

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

**Who handles authentication and authorisation.** These are two different jobs and they live in two different places, so they are stated separately here rather than left to be inferred from A4 and A5.

*Authentication is owned by `identity`, alone.* It is the only component that holds credentials, the only one that signs a token, and the only one that can revoke a session. No other service reads `identity_db`, and no other service has the private key.

| Concern | Owner | Detail |
| --- | --- | --- |
| Registration, password hashing, credential storage | `identity` | Argon2id; `identity_db` only |
| Login, token issuance, refresh, logout | `identity` | RS256; 15-minute access token, rotating refresh token with reuse detection |
| Signing key custody and rotation | `identity` | Private key mounted into `identity` only; public key (JWKS) served at `/api/v1/auth/jwks` and cached by every verifier |
| Role assignment, merchant approval | `identity` | Roles and merchant status are claims, set here and nowhere else |
| Token **verification** | every service, plus `gateway` | Local signature and claims check against the cached public key. No network call to `identity` on the request path (A4, A5) |

*Authorisation is owned by the service that owns the resource.* There is no central policy service and no authorisation call across a service boundary — a service that owns a row is the only component that knows what it means to be allowed to touch it.

| Layer | What it decides | What it must not do |
| --- | --- | --- |
| `gateway` | Is there a valid, unexpired token? Coarse route-level role check (e.g. `/api/v1/analytics/*` is `admin`-only) | Be the only check. It is an optimisation that rejects early, never the security boundary |
| Owning service | Does *this* subject, with *these* claims, get to perform *this* action on *this* row? Role check plus ownership check plus merchant scoping | Trust a role, user id, or merchant id from the request body or a header — claims come from the verified token only |
| `libs/common` | Supplies the verification, the claim model, and the dependency that services declare their requirements with | Contain domain rules. It answers "who is this", not "may they" |

Concretely: `identity` says a caller is merchant `M` with role `merchant`; `catalog` decides whether merchant `M` may edit product `P`, by checking `P.merchant_id`. Neither decision is made anywhere else. The same split holds for internal callers — when `order` calls `catalog` from M2, it presents the end user's token rather than a service credential, so `catalog` applies exactly the same rule it applies at the edge.

Service-to-service calls therefore need no separate trust model in Milestones 1–3. If a future path needs a call with no user in context, it gets a dedicated client-credentials token issued by `identity` with its own claim, and that is an amendment to this section rather than a local decision.

**Cross-cutting architectural decisions.** These bind every milestone. Per-milestone RFCs may add decisions but may not silently contradict these.

- **A1 — Service per owned aggregate, database per service.** Boundaries are enforced by deployment topology, not developer discipline. Established in [RFC 0001](0001-foundation-and-core-services.md).
- **A2 — One PostgreSQL instance, one logical database and role per service.** Real isolation at schema level without paying for eight containers on a laptop that also has to host Kafka, Temporal, and Grafana.
- **A3 — No foreign keys across service boundaries.** `order.order_items.product_id` is a plain indexed UUID. Referential integrity across services is an eventual property maintained by events, not a constraint.
- **A4 — RS256 JWTs; `identity` is the only signer.** Every other component holds the public key and verifies locally, with no network call on the request path.
- **A5 — Every service re-verifies the token.** The gateway rejects early; it is not trusted. Services must be safe to call directly, because from M2 there are internal callers.
- **A6 — Async all the way down.** FastAPI `async def`, SQLAlchemy 2.0 async, `asyncpg`. No sync I/O inside the event loop.
- **A7 — RFC 9457 problem details, one shape everywhere**, produced by one handler in `libs/common`.
- **A8 — `X-Request-ID` in, W3C `traceparent` out.** A correlation ID is generated at the gateway if absent, propagated over HTTP headers and in the event envelope, and attached to every log line. From M3 it is the OpenTelemetry trace ID.
- **A9 — Payments and carriers are simulated, behind an interface.** A `PaymentProvider` and `ShippingProvider` protocol with a deterministic fake implementation that can be told to fail, time out, or succeed. The point of the milestone is the workflow's behaviour under those failures, not the integration.
- **A10 — Transactional outbox for every state-change event.** A service writes its state change and its outbox row in one transaction; `outbox-relay` publishes. Dual-writing to Postgres and Kafka in the same handler is the standard way to lose events, and it is not done anywhere.
- **A11 — Consumers are idempotent and keyed.** Every consumer records processed `event_id`s and is safe to replay, because Kafka delivery is at-least-once.
- **A12 — Temporal owns multi-step business processes; Kafka owns notifications of fact; Celery owns fire-and-forget tasks.** Three tools, three distinct jobs — see §3.2.
- **A13 — Synchronous cross-service calls only where the caller cannot proceed without the answer.** In practice that is exactly one path: `order` validating and snapshotting product data at creation. Everything else is an event.
- **A14 — Data is snapshotted at the boundary, not joined across it.** An order line stores the product name, SKU, and price as they were at purchase. This is correct domain modelling, not a workaround.

**The three critical flows.**

*Flow 1 — Product publishing (M2, extended M3/M5).*

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
   │                 │                  │ 2 process media ───┼──▶ celery (M3)
   │                 │                  │ 3 build search doc │
   │                 │                  │ 4 sync downstream  │
   │                 │◀─────────────────┤ 5 mark published   │
   │                 │   + outbox row   │   (one txn)        │
   │                 ├──── outbox-relay ┼───▶ Kafka: product.published (M3)
   │                 │                  │        ├──▶ indexer-worker  → embeddings (M5)
   │                 │                  │        ├──▶ analytics       → products_published
   │                 │                  │        └──▶ inventory       → create stock row
   │ GET /products/{id}  status: published                   │
```

Partial failure never leaves a half-published product: steps 1–4 are Temporal activities with retry policies, step 5 is the only state write, and a workflow that fails permanently runs compensations and leaves the product in `draft` with a recorded failure reason.

*Flow 2 — Checkout (M2, extended M3).*

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

Any step failing runs its compensation in reverse — release reservation, refund, mark order `failed` — which is why this is a workflow and not a chain of event handlers.

*Flow 3 — Assistant query (M6).*

```
customer        gateway       assistant        vector store      catalog       LLM
   │               │              │                 │               │           │
   │ POST /assistant/chat (SSE)   │                 │               │           │
   ├──────────────▶├─────────────▶│                 │               │           │
   │               │              │ embed query ────┼───────────────┼──────────▶│
   │               │              │ top-k search ──▶│               │           │
   │               │              │◀── chunks ──────┤               │           │
   │               │              │ hydrate live price/stock ──────▶│           │
   │               │              │ assemble prompt + history       │           │
   │               │              ├─────────────────┼───────────────┼──────────▶│
   │◀═══ token stream (SSE) ══════┤◀════════════════┼═══════════════┼═══════════┤
   │               │              │ persist message, emit usage event           │
```

#### 3.1.2. API changes

**Surface map.** Every route is versioned under `/api/v1` and reached through the gateway at `:8000`.

| Prefix | Service | Introduced |
| --- | --- | --- |
| `/api/v1/auth/*`, `/api/v1/users/*`, `/api/v1/merchants/*` | `identity` | M1 |
| `/api/v1/products/*`, `/api/v1/categories/*`, `/api/v1/variants/*`, `/api/v1/media/*` | `catalog` | M1 |
| `/api/v1/orders/*` | `order` | M2 |
| `/api/v1/inventory/*` | `inventory` | M2 |
| `/api/v1/notifications/*` | `notification` | M3 |
| `/api/v1/analytics/*` | `analytics` | M3 |
| `/api/v1/search/semantic` | `assistant` | M5 |
| `/api/v1/assistant/*` | `assistant` | M6 |
| `/health`, `/health/ready`, `/metrics`, `/docs` | all | M1, `/metrics` M3 |

**Conventions fixed here, once, for all services.**

- *Errors* — RFC 9457 problem details. `{"type", "title", "status", "detail", "instance", "request_id"}` plus `violations[]` on validation failures.
- *Pagination* — `{"items": [...], "total": N, "limit": L, "offset": O}`, hard `limit` ceiling of 100.
- *Correlation* — `X-Request-ID` accepted or generated at the gateway, echoed on every response, present on every log line, carried into every event and workflow.
- *Device correlation* — an optional `X-Client-Id` header on mutating requests, minted by the client per browser profile or app install. The gateway passes it through and validates its shape only; it is carried into the event envelope and used to scope notification delivery to the originating device ([RFC 0003](0003-events-and-observability.md) C21). It is never an identity and never an authorisation input.
- *Idempotency* — an `Idempotency-Key` header on every non-idempotent POST that creates business state. Introduced for orders in M2; the mechanism lives in `libs/common` so later services reuse it.
- *Async acceptance* — an endpoint that starts a workflow returns `202` with a resource URL to poll, never a blocking wait on completion.
- *Versioning* — additive changes only within `v1`. A breaking change means `v2` mounted alongside.

**The event catalogue.** Fixed here so producers and consumers are designed against the same list. Topics are `smartretail.<domain>.<event>`, one schema per topic in the Schema Registry, Avro-encoded, `BACKWARD` compatibility enforced. Implemented in M3 ([RFC 0003](0003-events-and-observability.md)).

| Topic | Producer | Key | Consumers |
| --- | --- | --- | --- |
| `identity.user.registered` | `identity` | `user_id` | `analytics`, `notification` |
| `identity.merchant.status_changed` | `identity` | `user_id` | `catalog`, `analytics` |
| `catalog.product.published` | `catalog` | `product_id` | `inventory`, `analytics`, `indexer-worker` |
| `catalog.product.updated` | `catalog` | `product_id` | `indexer-worker`, `analytics` |
| `catalog.product.unpublished` | `catalog` | `product_id` | `indexer-worker`, `analytics` |
| `order.created` | `order` | `order_id` | `analytics` |
| `order.confirmed` | `order` | `order_id` | `notification`, `analytics` |
| `order.failed` | `order` | `order_id` | `notification`, `analytics` |
| `order.cancelled` | `order` | `order_id` | `notification`, `analytics`, `inventory` |
| `payment.succeeded` / `payment.failed` | `order` | `order_id` | `notification`, `analytics` |
| `shipment.status_changed` | `order` | `order_id` | `notification`, `analytics` |
| `inventory.reserved` / `inventory.released` | `inventory` | `variant_id` | `analytics` |
| `inventory.stockout` | `inventory` | `variant_id` | `analytics`, `notification` |
| `assistant.interaction` | `assistant` | `conversation_id` | `analytics` |

**Event envelope.** Every message on every topic, regardless of domain:

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

#### 3.1.3. Data/schema changes

**Ownership map**, summarised. The authoritative version is [RFC 0006 §3.1.3.7](0006-service-decomposition-and-data-model.md#3137-the-ownership-map) (R3); this table is a reading aid and must not diverge from it. No other service writes these, and no other service reads them except through an API or an event.

| Fact | Owner | Replicated to | How |
| --- | --- | --- | --- |
| User identity, credentials, role | `identity` | JWT claims | Token |
| Merchant profile, approval status | `identity` | `catalog` (id only) | Event (M3) |
| Product definition, price, status | `catalog` | `order` (snapshot at purchase), vector store | Sync call at order creation; event for indexing |
| Stock on hand, reservations | `inventory` | `catalog` reads via API for display | API |
| Order, payment state, shipment state | `order` | `analytics`, `notification` | Events |
| Notification delivery record | `notification` | — | — |
| Metric rollups | `analytics` | — | — |
| Conversations, embeddings | `assistant` | — | — |

**Store selection.**

| Store | Used for | Chosen because |
| --- | --- | --- |
| PostgreSQL 16 | All transactional service data | Relational, ACID, mature; every core aggregate has real invariants worth enforcing |
| MongoDB | `analytics_db` — event-derived rollups and time series | Schema-flexible, append-heavy, per-metric documents with varying shape; satisfies Part A's "NoSQL DB" requirement with an actual workload rather than a token one |
| Redis | Rate limiting, idempotency fast path, hot read cache, Celery result backend | Introduced M3, when there is a measured need |
| pgvector *(default)* / Qdrant *(alternative)* | Embeddings | Decided in [RFC 0004](0004-retrieval-layer.md); default is pgvector, to avoid adding a container for a catalogue of a few thousand products |

**Shared table count across the whole system: zero.** That is the invariant this RFC exists to protect.

#### 3.1.4. Breaking changes

None externally — the platform has no released API and no production data.

Two planned contract changes are scheduled and recorded in advance so no client codes against an interim behaviour:

| Change | From | To | Milestone |
| --- | --- | --- | --- |
| `POST /products/{id}/status` with `published` | `501 Not Implemented` | `202 Accepted` + workflow id | M2 |
| `POST /orders` | does not exist | `202 Accepted` | M2 |

The prior prototype, which scaffolded eleven mostly-empty services and a frontend, was deleted before [RFC 0001](0001-foundation-and-core-services.md). The service inventory above supersedes it in full.

#### 3.1.5. Action Plan

This RFC produces no code. Its output is the sequence below and the five RFCs that implement it.

| Milestone | Week | Components standing up | RFC | Status |
| --- | --- | --- | --- | --- |
| 1 — Foundation & Core Services | 1 | `gateway`, `identity`, `catalog`, `libs/common`, Postgres, Compose | [0001](0001-foundation-and-core-services.md) | DRAFT |
| 2 — Core Business Logic | 2 | `order`, `inventory`, `temporal-worker`, Temporal, MinIO | [0002](0002-core-business-logic-and-workflows.md) | DRAFT |
| 3 — Events & Observability | 3 | `notification`, `analytics`, `outbox-relay`, `celery-worker`, Kafka + Schema Registry, RabbitMQ, Redis, MongoDB, OTel Collector, Jaeger, Prometheus, Grafana | [0003](0003-events-and-observability.md) | DRAFT |
| 4 — Part A Review | — | Nothing new; review of the above | — | — |
| 5 — Retrieval Layer | 4 | `assistant` (retrieval half), `indexer-worker`, vector store | [0004](0004-retrieval-layer.md) | DRAFT |
| 6 — AI Assistant | 5 | `assistant` (generation half), LangGraph, SSE streaming | [0005](0005-ai-assistant-and-streaming.md) | DRAFT |
| 7 — Part B Review | — | Nothing new; review of the above | — | — |

**Container count by milestone** — worth stating, because the laptop is a real constraint:

| After | App containers | Infra containers | Total |
| --- | --- | --- | --- |
| M1 | 3 | 1 | 4 |
| M2 | 5 + 1 worker | 3 | 9 |
| M3 | 7 + 3 workers | 11 | 21 |
| M6 | 8 + 4 workers | 12 | 24 |

Milestone 3 is where this becomes uncomfortable. Mitigation is fixed now, not then: Compose profiles (`core`, `events`, `observability`, `ai`) so that a developer runs the subset they are working on, and one Postgres instance for all services.

### 3.2. Rationale

**Why eight services rather than four, or fifteen.** Four (gateway, identity, commerce, ai) would collapse the two boundaries that carry the most design weight — orders against inventory, and transactional against analytical. Fifteen was the prototype's answer and produced services with nothing in them; a boundary with no state behind it costs a Dockerfile, a migration history, a health check, and CI time while teaching nothing. Eight is what the ownership test in §3.1.1 yields when applied honestly, and each of the rejections in that table is a place where the count could have inflated for appearance's sake.

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

**Why the event catalogue is fixed before any event exists.** Events are the hardest thing to change later, because a schema change has to be compatible with every consumer that has ever been deployed. Naming them now, with an envelope and a compatibility rule, means Milestone 3 implements a design rather than discovering one topic at a time.

**Why per-milestone RFCs rather than one document.** A single RFC covering M1–M6 would be several thousand lines and would not survive review. Worse, its M5 and M6 sections would be written before anything had been learned from M2 and M3, and would be revised anyway. This RFC holds the decisions that must be consistent across all milestones; each milestone RFC holds the decisions that are best made with that milestone's work in view.

### 3.3. Tradeoffs

| Trade being made | The cost | Why it is acceptable |
| --- | --- | --- |
| Eight services on one laptop | Real memory and CPU pressure from M3; slower local iteration; debugging spans containers | Compose profiles; one Postgres instance; the distributed-systems learning objective is the point of the exercise |
| Designing the whole topology up front | Some of it will be wrong, and RFC 0000 will need amendment | Amendments are cheap and visible. Discovering boundaries milestone by milestone is not cheaper, it is just less visible |
| `payment` and `shipment` folded into `order` | Looks less "microservice" in a review; a real payment integration later needs extraction | The interface boundary (A9) makes extraction mechanical. Building an empty service to look right is the worse failure |
| Snapshotting product data into orders | Storage duplication; a corrected product name does not propagate to historical orders | That is the desired behaviour for an order record |
| Events fixed in a catalogue before implementation | Some events will be unused; some needed ones will be missing | Additive changes are easy; the catalogue's job is to prevent inconsistent naming and envelopes, which are not |
| Transactional outbox everywhere | Extra table, extra relay component, added publish latency | The alternative is losing events on any crash between the commit and the publish. Non-negotiable for order and payment events |
| Three orchestration tools | Three sets of operational concepts to learn and run | Explicitly required by Part A, and each has a distinct job (§3.2) |
| Simulated payment and shipping | No real integration experience | Neither Part A nor Part B asks for one, and a sandbox account is an external dependency the assignment does not need |

#### 3.3.1. Challenges and Open Questions

| # | Question | Current position | Needs a decision by |
| --- | --- | --- | --- |
| Q1 | Is `assistant` one service or two (retrieval and generation)? | One service, two routers. They share the conversation store and the embedding client; splitting would create a synchronous hop on every query for no ownership gain | M5 |
| Q2 | pgvector or a dedicated vector database? | pgvector by default — no new container, and the corpus is small. Revisit if recall or latency disappoints | M5, in [RFC 0004](0004-retrieval-layer.md) |
| Q3 | Does `order` call `catalog` synchronously, or keep a local product projection fed by events? | Synchronous call at creation (A13). A projection is the scalable answer but adds a consistency problem to a path that must be correct | M2; revisit M3 once events exist |
| Q4 | Where does inventory come from initially — merchant-set, or an event on publish? | `catalog.product.published` creates a zero-quantity stock row; the merchant sets quantity through `inventory` | M2 |
| Q5 | Should the gateway stay hand-written, or move to Traefik/Kong? | Hand-written while it shares `libs/common`. Reconsider when rate limiting and SSE proxying arrive | M3 |
| Q6 | How is the assistant's LLM cost bounded? | Per-user and global daily token budgets enforced in `assistant`, with a cached-response path | M6, in [RFC 0005](0005-ai-assistant-and-streaming.md) |
| Q7 | Do we need a cart, given that Part A never mentions one? | No. Orders are placed with a line-item body | Decided |
| Q8 | Does `analytics` compute rollups on ingest, on read, or both? | On ingest for counters, on read for time-window queries, with a scheduled reconciliation job | M3 |
| Q9 | Should the observability stack be in the default Compose profile? | No — `observability` profile, off by default, on for the Milestone 3 demo and review | M3 |

---

## 4. Dependencies/blockers/known issues

**Infrastructure by milestone.**

| Component | Version | Milestone | Purpose |
| --- | --- | --- | --- |
| PostgreSQL | 16 | M1 | Transactional store for six services |
| Temporal + UI | 1.25.x | M2 | Workflow orchestration |
| MinIO | latest | M2 | S3-compatible media storage |
| Kafka (KRaft) + Schema Registry | 7.7.x | M3 | Event backbone |
| RabbitMQ | 3.13 | M3 | Celery broker |
| Redis | 7.x | M3 | Cache, rate limit, Celery results |
| MongoDB | 7.x | M3 | Analytics store |
| OTel Collector, Jaeger, Prometheus, Grafana | current | M3 | Observability |
| pgvector extension *(or Qdrant)* | 0.7.x | M5 | Embeddings |

**External dependencies.** One only: an LLM provider API key (OpenAI, Groq, or Anthropic) from Milestone 5. Every other dependency is a public image or package. This is the single item requiring an account, and it should be obtained before Week 4 rather than during it.

**Blockers.** None for Milestones 1–3.

**Known issues carried across the project.**

- Local resource pressure from Milestone 3 (see §3.1.5); mitigated by Compose profiles, not eliminated.
- No TLS anywhere; plain HTTP inside Compose.
- No email or SMS delivery — the notification service logs and records rather than sends, with an interface allowing a real provider.
- Payments and carriers are simulated throughout (A9).
- No load testing; capacity statements are structural rather than measured.

---

## 5. Alternate solutions

**Alternative 1 — Modular monolith, split later.** One FastAPI app, one database, boundaries by package convention.

*Rejected* for the reasons in [RFC 0001](0001-foundation-and-core-services.md) §5, and one more that only appears at this level: a monolith makes the event backbone pointless. If every module can call every other in-process, Kafka becomes ceremony, and the requirement that produced it goes unlearned.

**Alternative 2 — Fifteen fine-grained services**, one per noun (payment, shipment, cart, review, search, media, pricing…).

*Rejected.* This was the prototype's shape. It fails the ownership test in §3.1.1, multiplies operational cost with no isolation gain, and makes it impossible for a reviewer to tell which boundaries were reasoned about.

**Alternative 3 — Event sourcing as the primary persistence model** rather than state tables plus an outbox.

*Rejected.* A genuinely good fit for orders and inventory, and the natural home for the audit requirement. But it is a large conceptual and implementation cost applied to a five-week schedule, it makes every simple read query harder, and Part A's requirements are satisfied by state tables with an audit log. The transactional outbox gives most of the integration benefit at a fraction of the cost.

**Alternative 4 — Choreography instead of orchestration** — no Temporal; the order saga runs as a chain of Kafka consumers each reacting to the previous step.

*Rejected.* This is the classic distributed-saga trap: no single place shows where an order is stuck, compensation logic is scattered across consumers, and timeouts have to be reimplemented per step. Part A explicitly requires recovery from partial failures and resumability, which is Temporal's core competence and choreography's weakest point. Part A also lists Temporal in the stack.

**Alternative 5 — Kafka for everything**, replacing both Temporal and Celery.

*Rejected.* See the tool table in §3.2. Kafka has no workflow state, no compensation primitive, and no per-task retry with backoff. Building those on top is reimplementing Temporal badly.

**Alternative 6 — Managed services** (Confluent Cloud, Temporal Cloud, a hosted vector DB) to reduce local load.

*Rejected.* External accounts, credentials, and network dependency for an assignment whose stated target environment is Docker Compose on a laptop, and whose review is a walkthrough on that laptop.

**Alternative 7 — Skip the gateway; expose services directly.**

*Rejected.* Correlation IDs, CORS, rate limiting, and aggregated documentation need one place to live, and the alternative is duplicating them into every service or into an nginx config nobody reviews.

---

## 6. Production readiness

"Production" here means the assignment's target: Docker Compose on a developer machine, demonstrated to a mentor. The properties below are structural — designed to hold in principle, demonstrated at that scale.

### 6.1. Scalability and Fault tolerance

- **Stateless services.** All eight hold no in-process state; every one scales with `--scale`. State is in Postgres, Mongo, Redis, or Kafka.
- **Local token verification.** `identity` being down does not stop any other service serving authenticated traffic. It is not a runtime dependency of the request path.
- **Workflow durability.** Temporal persists workflow state, so a worker crash mid-order resumes rather than losing the order. This is the direct answer to Part A's "recovery from partial failures" and "resumable or cleanly reversible".
- **Backpressure.** Kafka consumers lag rather than drop; the gateway rate-limits from M3; Celery queues buffer. Part A's requirement is that the system degrade gracefully rather than collapse, and each layer has a defined behaviour when it falls behind.
- **Idempotency at both ends.** `Idempotency-Key` on order creation prevents duplicate orders from client retries; `event_id` deduplication in consumers prevents double-processing from at-least-once delivery. Part A names both.
- **Inventory contention.** Reservations use row-level locking with a short transaction and a bounded retry, which is the flash-sale path and the one place where a hot row is expected.
- **Blast radius.** A failure in `analytics` or `notification` cannot affect checkout — they are downstream consumers with no synchronous call into the order path. That is the main reason they are consumers rather than callees.
- **Health and readiness** on every service, with Compose dependency ordering, so nothing accepts traffic before its store is reachable.
- **Known ceilings.** Single Postgres instance, no replicas; single-broker Kafka locally; no partitioning strategy beyond the keys in §3.1.2. None of these are schema decisions that would block scaling later.

### 6.2. Security considerations

- **Authentication** — owned by `identity` alone: RS256 JWTs, private key only in `identity`, 15-minute access tokens, rotating refresh tokens with reuse detection. Every other component verifies locally against the published public key. Ownership split in full in §3.1.1.
- **Authorisation** — owned by the service that owns the resource, against claims from the verified token, never against a request body. The gateway's route-level check is an early rejection, not the boundary. Three roles: `customer`, `merchant`, `admin`.
- **Tenant isolation** — a merchant's scope is derived from the token on every catalog, order, and inventory query. This is the single most important recurring test across every milestone RFC.
- **Secrets** — environment only, `.env` gitignored, keys mounted from a gitignored directory. The LLM API key (M5) is the only real credential in the system and never leaves `assistant`.
- **Transport** — plain HTTP inside Compose; TLS is an environment concern outside scope.
- **Injection** — ORM with bound parameters throughout; no string-built SQL.
- **Event data** — events carry IDs and the minimum payload needed; no credentials, no password hashes, no full PII beyond what a consumer requires.
- **AI-specific** (M6) — prompt injection through product text and user messages, retrieval scoped to published products only, no PII in prompts, output filtering, and per-user rate and token budgets. Detailed in [RFC 0005](0005-ai-assistant-and-streaming.md).
- **Rate limiting** — from M3, at the gateway, with tighter limits on `/auth/login` and on assistant endpoints.

### 6.3. Deployment strategy

- **One `docker-compose.yml`** at the repository root with profiles: `core` (M1), `workflow` (M2), `events` (M3), `observability` (M3), `ai` (M5). `make up` brings up what the current milestone needs; `make up-all` brings up everything.
- **One Makefile** as the single entry point: `up`, `down`, `logs`, `migrate`, `seed`, `test`, `lint`, `fmt`, `reset`.
- **Multi-stage Dockerfiles**, non-root user, one per service, sharing a base layer for `libs/common`.
- **Migrations** are an explicit step per service, never automatic on startup.
- **Configuration** is twelve-factor and validated at startup by `pydantic-settings`; a missing variable fails the container immediately.
- **Rollback** is per service — separate Alembic histories mean rolling back one service does not require rolling back another.

---

## 7. Test Plan

This RFC introduces no code, so its test plan is the testing *strategy* every milestone RFC then instantiates.

**Layers.**

| Layer | Scope | Where it runs |
| --- | --- | --- |
| Unit | Pure logic — state machines, token handling, chunking, prompt assembly | In-process, no I/O |
| Integration | One service against a real Postgres, `httpx.ASGITransport` | Per service, per milestone |
| Workflow | Temporal workflows under the test environment, including activity failure injection | M2 onwards |
| Contract | Every produced event validated against its registered schema; every consumer tested against the registered schema | M3 onwards |
| End-to-end | Through the gateway, cold start, no fixtures | Every milestone, cumulative |

**Invariants tested at every milestone, cumulatively.** These are the properties this RFC exists to protect, so each milestone re-runs them rather than assuming they still hold:

1. No service can reach another service's database — asserted by configuration inspection and by a test that the grant does not exist.
2. Every non-public endpoint enforces its role matrix, for all three roles.
3. A merchant cannot read or modify another merchant's data, on any surface.
4. Every response carries `X-Request-ID`, and a single request is traceable across every service it touches.
5. From M2: retrying a create with the same `Idempotency-Key` produces one business object.
6. From M3: replaying an event produces no duplicate effect.
7. From M3: every event published validates against its registered schema.

**The end-to-end scenario grows with the project** and is the milestone review's spine:

- M1: register → login → create category → create product → list → status transitions.
- M2: + set stock → publish (workflow completes) → place order → reserve → pay → confirm; and the failure path with compensation.
- M3: + assert events published and consumed, notification recorded, analytics counters moved, one trace spans gateway → order → inventory → consumer.
- M5: + product indexed on publish → semantic search returns it.
- M6: + ask the assistant about it and receive a streamed, grounded answer citing that product.

---

## 8. Impact Analysis

**On the schedule.** Five implementation weeks for eight services is tight, and this document's main practical function is to remove design time from Weeks 2–5. The risk it addresses is spending Week 3 deciding whether analytics is a service.

**On each milestone.**

| Milestone | What this RFC gives it | What it constrains |
| --- | --- | --- |
| M1 | Confirms three services is the right start, and that the four it *doesn't* build are deferred rather than forgotten | Must not add columns that later belong to `order` or `inventory` |
| M2 | `order` and `inventory` boundaries, the payment/shipment folding decision, the Temporal-vs-Kafka split | Must use the outbox pattern even before Kafka exists, so M3 is a configuration change |
| M3 | The full event catalogue and envelope, ready to implement | Must not invent topic names outside the catalogue without amending this RFC |
| M4 | A single document a reviewer can read to see the whole design and its rejected alternatives | — |
| M5 | `assistant` scoped as one service; indexing driven by events rather than polling | Must consume `catalog.product.published`, not query `catalog_db` |
| M6 | Streaming contract, cost controls, and the interaction event already named | — |

**Risks at project level.**

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Milestone 3 overruns — Kafka, Celery, three observability tools, and two new services in one week is the heaviest week by far | **High** | High | Outbox tables and structured logging land in M1–M2; `notification` is deliberately thin; the observability stack is configuration, not code; drop `analytics` read APIs to a minimum if time is short |
| Laptop cannot run the full stack | Medium | High | Compose profiles from M1; one Postgres; observability off by default |
| Boundary erosion under deadline pressure (a cross-service query "just this once") | Medium | High | Separate DB roles make it fail at connection time, not at review time |
| Event schema churn breaking consumers | Medium | Medium | Schema Registry with `BACKWARD` compatibility; additive-only changes |
| LLM provider cost or rate limits in M5–M6 | Medium | Medium | Small embedding model, cached embeddings, token budgets, a local fake provider for tests |
| This RFC drifts out of date as milestones teach us things | **High** | Low | Amendment is expected. §11 records amendments with dates; a milestone RFC that contradicts this one must say so explicitly |

**On reviewers.** The Part A review (M4) should be able to check this document against the code and find: eight services or a documented reason for a different number, no shared tables, every event in the catalogue, and every deferral traceable to the milestone that owns it.

---

## 9. Useful Links/References

**Project documents**

- [Part A — platform requirements](../../part-a.md)
- [Part B — GenAI layer](../../part-b.md)
- [Milestones](../../milestones.md) · [Execution Guidelines](../../execution-guidelines.md) · [Learning Path](../../learning-path.md)

**Implementation RFCs**

- [RFC 0001 — Foundation & Core Services](0001-foundation-and-core-services.md)
- [RFC 0002 — Core Business Logic & Workflows](0002-core-business-logic-and-workflows.md)
- [RFC 0003 — Events & Observability](0003-events-and-observability.md)
- [RFC 0004 — Retrieval Layer](0004-retrieval-layer.md)
- [RFC 0005 — AI Assistant & Streaming](0005-ai-assistant-and-streaming.md)

**Technical references**

- Transactional outbox pattern — https://microservices.io/patterns/data/transactional-outbox.html
- Saga pattern — https://microservices.io/patterns/data/saga.html
- Temporal documentation — https://docs.temporal.io/
- Confluent Schema Registry compatibility types — https://docs.confluent.io/platform/current/schema-registry/fundamentals/avro.html
- OpenTelemetry specification — https://opentelemetry.io/docs/specs/otel/
- W3C Trace Context — https://www.w3.org/TR/trace-context/
- RFC 9457, Problem Details for HTTP APIs — https://www.rfc-editor.org/rfc/rfc9457
- Domain-Driven Design, bounded contexts — Evans, *Domain-Driven Design*
- *Designing Data-Intensive Applications* — Kleppmann, chapters 11 and 12 on stream processing

---

## 10. Related tickets/issues

| ID | Title | Maps to |
| --- | --- | --- |
| — | RFC 0000 — System Architecture | This document |
| — | Milestone 1 — Foundation & Core Services | [RFC 0001](0001-foundation-and-core-services.md) |
| — | Milestone 2 — Core Business Logic | [RFC 0002](0002-core-business-logic-and-workflows.md) |
| — | Milestone 3 — Events & Observability | [RFC 0003](0003-events-and-observability.md) |
| — | Milestone 4 — Part A Review | Reviews this RFC and 0001–0003 |
| — | Milestone 5 — Retrieval Layer | [RFC 0004](0004-retrieval-layer.md) |
| — | Milestone 6 — AI Assistant | [RFC 0005](0005-ai-assistant-and-streaming.md) |
| — | Milestone 7 — Part B Review | Reviews 0004–0005 |

---

## 11. Notes

- **On the eight-service count.** It is a design decision, not a requirement. Neither Part A nor Part B states a number. The count is defended by the ownership test in §3.1.1 and by the explicit rejections in the same table; a reviewer who disagrees should disagree with a specific row.
- **On domain wording.** The source milestone list is phrased for an e-learning platform ("User & Course management", "student/instructor", "instructor content generation (summaries, quizzes)"). [Part A](../../part-a.md) specifies retail commerce, so every RFC reads *course* → *product*, *student* → *customer*, *instructor* → *merchant*, *quizzes* → *promotional copy and FAQs*, with `admin` as the third role Part A §1 requires. The verbatim source text is reproduced in each milestone RFC so the mapping can be checked. Milestone structure and expected outcomes are unchanged.
- **On amendment.** This RFC is expected to change as milestones execute. Amendments are appended below with a date and a reason rather than edited silently, so that the review can see what was learned.
- **Amendment log.**

  | Date | Section | Change | Reason |
  | --- | --- | --- | --- |
  | — | — | — | — |

- **Definition of done for this RFC.** Goals G1–G7 satisfied; RFCs 0001–0005 exist and none contradicts it without saying so; reviewed and moved to **DONE** before Milestone 2 begins.

---

## 12. Comments

*Reviewer comments go here. Please raise objections against the numbered decisions (A1–A14), the open questions (Q1–Q9), the service inventory rows in §3.1.1, or the rejected alternatives in §5, so the discussion attaches to something specific.*

| Reviewer | Section | Comment | Resolution |
| --- | --- | --- | --- |
| | | | |
