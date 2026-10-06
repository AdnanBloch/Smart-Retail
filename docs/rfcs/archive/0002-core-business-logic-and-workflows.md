# RFC 0002 — Core Business Logic & Workflows

> **SUPERSEDED — archived Sep 29, 2026.** The RFC set was reorganised from milestone-based to architecture- and service-based. This document is kept for history only and is no longer maintained. Its content now lives in: [0004-catalog-service](../0004-catalog-service.md), [0005-order-service](../0005-order-service.md), [0006-inventory-service](../0006-inventory-service.md), [0001-platform-and-shared-infrastructure](../0001-platform-and-shared-infrastructure.md). See [docs/README.md](../../README.md).

| | |
| --- | --- |
| **Owner** | @muhammad.adnan |
| **Collaborators** | SarwanAhmed |
| **Status** | **DRAFT** · IN-REVIEW · DONE |
| **Created** | Sep 22, 2026 |
| **Related Epic or story** | Milestone 2 — Core Business Logic ([milestones.md](../../milestones.md)), Week 2 ([execution-guidelines.md](../../execution-guidelines.md)) |
| **Builds on** | [RFC 0000 — System Architecture](0000-system-architecture.md) · [RFC 0001 — Foundation & Core Services](0001-foundation-and-core-services.md) |

---

### Milestone source text (verbatim)

> **Week 2 — Business Logic + Workflows**
>
> Focusing on core business logic and implementation of end-end assignment.
>
> **Expected Outcome**
> - Implementation of Core Business Logic
> - Workflow orchestration integrated

The source text is deliberately broad. The concrete scope below is drawn from
Week 2 of the [Execution Guidelines](../../execution-guidelines.md) and from
[Part A](../../part-a.md) §2 (Product Publishing Workflow) and §3 (Order Workflow).
Domain wording is mapped per [RFC 0000](0000-system-architecture.md) §11.

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

**Core business logic — the `order` and `inventory` services, and Temporal orchestration of the product publishing and order lifecycles.**

This RFC takes the platform from "CRUD that works" to "a commerce system". It adds the two services that own transactional commerce state, introduces Temporal as the orchestrator for the two multi-step processes [Part A](../../part-a.md) specifies, and implements the four properties that make an order system trustworthy: idempotency, reservation, compensation, and resumability.

It covers Milestone 2 only. Events reach Kafka in Milestone 3 — but the transactional outbox that feeds Kafka is written here, so that Milestone 3 is a relay and a topic configuration rather than a change to every write path.

---

## 2. Overview

After [RFC 0001](0001-foundation-and-core-services.md), SmartRetail can describe products but cannot sell them. `POST /products/{id}/status` with `published` returns `501`, pointing at this document.

[Part A](../../part-a.md) specifies two processes in enough detail to implement directly:

- **§2 Product Publishing** — validate, process media, build a search record, synchronise downstream, mark ready. With the constraint: *partial failures must not corrupt product state or leave the workflow incomplete.*
- **§3 Order Workflow** — record the order, reserve inventory, trigger payment, create a shipment, notify, update analytics. With the requirements: *high traffic order bursts, idempotent retries, backpressure handling, recovery from partial failures, prevention of duplicate orders.*

Both are multi-step, both cross service boundaries, and both must either complete or undo cleanly. That is the definition of a saga, and it is why Temporal appears in Part A's stack. This RFC adds `order` (:8003), `inventory` (:8004), a `temporal-worker`, Temporal itself, and MinIO for media.

The design principle throughout: **the workflow owns the sequence, the services own the state.** A Temporal activity is a thin call into the same service-layer function an HTTP route would call — the seam [RFC 0001](0001-foundation-and-core-services.md) §3.1.1 established for exactly this purpose.

### 2.1. Goals

| # | Goal | How it is verified |
| --- | --- | --- |
| G1 | Product publishing runs as a durable workflow; partial failure never leaves a half-published product | Activity-failure injection tests: every failure point leaves the product in `draft` with a recorded reason, never `published` |
| G2 | Orders are created, persisted, and progressed through a defined state machine | State-machine tests cover every legal and illegal transition |
| G3 | Inventory is reserved before payment and committed only on success; failure releases it | Test: a failed payment leaves `available_quantity` exactly as it began |
| G4 | Duplicate orders are impossible — a retried request produces one order | Concurrent-duplicate test: 10 parallel requests with one `Idempotency-Key` yield one order and ten identical `200`s |
| G5 | An order interrupted mid-flight resumes; one that cannot complete is cleanly reversed | Worker killed mid-workflow; on restart the order reaches a terminal state with no orphaned reservation |
| G6 | Oversell is impossible under concurrency | 50 concurrent orders against 10 units yield exactly 10 confirmations and 40 explicit stockouts |
| G7 | Media upload and processing work, driven by the publishing workflow | Upload returns a URL; the workflow's media step produces derived sizes |
| G8 | Every state change writes an outbox row in the same transaction | Test: a rolled-back state change leaves no outbox row, and vice versa |
| G9 | The `501` on publish becomes `202`, as pre-announced | Contract test |

### 2.2. Non-goals

| Excluded | Why / when |
| --- | --- |
| Kafka, Schema Registry, real event publication | Milestone 3. Outbox rows are written here and accumulate; nothing consumes them yet |
| Celery and RabbitMQ | Milestone 3. Media processing in M2 runs as a Temporal activity; it moves to Celery in M3 where it belongs |
| `notification` and `analytics` services | Milestone 3. The order workflow has a `notify` activity that writes a log line and an outbox row — the seam, not the service |
| OpenTelemetry, Jaeger, Prometheus | Milestone 3. Temporal's own UI provides workflow visibility in the meantime, which is most of what is needed this week |
| A real payment provider | Never — simulated behind an interface per [RFC 0000](0000-system-architecture.md) A9 |
| A real carrier integration | Same |
| Returns and refunds as a customer-facing flow | Deferred. The `refund` compensation activity exists because the saga needs it; a customer-initiated return API does not |
| A shopping cart | Not specified anywhere in Part A or B. Orders carry line items directly |
| Promotions, discounts, tax, multi-currency arithmetic | Later. Orders compute from `base_price` and a per-order `currency` that must match across lines |
| Redis caching | Milestone 3 |
| Partial shipment, split fulfilment, backorder | Out of scope. One order, one shipment |

---

## 3. Solution Overview

### 3.1. Specification

#### 3.1.1. Architectural changes

**What joins the topology.**

```
              ┌──────────────────────────────────────────────┐
  client ────▶│  gateway  (:8000)                            │
              └──┬──────────┬──────────┬───────────┬─────────┘
                 ▼          ▼          ▼           ▼
          ┌──────────┐ ┌─────────┐ ┌────────┐ ┌───────────┐
          │ identity │ │ catalog │ │ order  │ │ inventory │
          │  :8001   │ │  :8002  │ │ :8003  │ │  :8004    │   ← new
          └────┬─────┘ └────┬────┘ └───┬────┘ └─────┬─────┘
               │            │          │            │
               │            │   snapshot product    │
               │            │◀─────────┤ (sync, A13)│
               │            │          │            │
          ┌────▼────┐  ┌────▼────┐ ┌───▼────┐  ┌────▼────┐
          │identity │  │catalog  │ │order   │  │inventory│
          │  _db    │  │  _db    │ │  _db   │  │   _db   │
          └─────────┘  └─────────┘ └────────┘  └─────────┘
                            ▲          ▲            ▲
                            │  activities call service layer
                       ┌────┴──────────┴────────────┴────┐
                       │      temporal-worker            │   ← new
                       │  ProductPublishingWorkflow      │
                       │  OrderWorkflow                  │
                       └────────────┬────────────────────┘
                                    │ gRPC
                       ┌────────────▼────────────┐   ┌────────┐
                       │  Temporal server + UI   │   │ MinIO  │  ← new
                       │  (:7233 / :8080)        │   │ :9000  │
                       └─────────────────────────┘   └────────┘
```

**New architectural decisions.** These extend, and do not contradict, [RFC 0000](0000-system-architecture.md) A1–A14.

- **B1 — Activities are thin adapters over the service layer.** An activity does argument marshalling, calls one service function, and returns a serialisable result. No business rule lives in workflow code, so every rule stays unit-testable without a Temporal environment.
- **B2 — Workflows hold no business data, only identifiers and step results.** Temporal's history is not a database. A workflow carries `order_id`, not the order.
- **B3 — Every activity is idempotent and keyed by a deterministic idempotency token** derived from `(workflow_id, activity_name, attempt-invariant input)`. Temporal retries at-least-once; an activity that charges twice on a retry is the classic failure and is designed out rather than hoped against.
- **B4 — Reservation before payment, commit after.** Inventory moves `available → reserved` before the charge and `reserved → committed` after it succeeds. A failed charge releases. The inverse order — charge then reserve — takes money for stock that may not exist.
- **B5 — Reservations expire.** Every reservation carries `expires_at` (15 minutes). A sweeper releases expired ones, so a workflow lost in a way Temporal cannot recover does not strand stock forever. This is a safety net beneath the compensation, not a replacement for it.
- **B6 — Inventory quantity is never computed; it is stored and ledgered.** `stock_items` holds current values; `stock_movements` is an append-only ledger of every change with its cause. Any disagreement between them is detectable, which is the entire point of Part A's "inventory disagrees across channels" complaint.
- **B7 — Idempotency keys are claimed by a unique insert, not by a read-then-write.** `INSERT ... ON CONFLICT DO NOTHING` on `(user_id, idempotency_key)` is the concurrency-safe claim. A read-check-write has a race that only appears under exactly the load Part A describes.
- **B8 — Orders snapshot product data at creation.** `order_items` stores `product_name`, `sku`, `unit_price`, and `variant_options` as they were. Correct domain modelling that also removes the cross-service join ([RFC 0000](0000-system-architecture.md) A14).
- **B9 — Order creation returns `202`, not `201`.** The order exists and is addressable, but its lifecycle is asynchronous. A client polls `GET /orders/{id}` or, from M3, receives a notification. Blocking the HTTP request until payment settles is the latency problem Part A opens with.
- **B10 — Payment and shipping are protocol interfaces with a deterministic fake.** `FakePaymentProvider` fails on a configured card token, times out on another, and succeeds otherwise, so every branch of the saga is reachable in a test.
- **B11 — The outbox is written in the state-change transaction, in every service.** Nothing reads it yet. This is the single most important forward-looking decision here: it makes Milestone 3 a relay rather than a rewrite of every handler.
- **B12 — Media upload is direct-to-service, then processed by an activity.** The merchant uploads to `catalog`, which stores the original in MinIO and records a `pending` media row. The publishing workflow processes derivatives. Presigned direct-to-MinIO upload is the scalable answer and is noted as Q6.

**`ProductPublishingWorkflow`** — the direct implementation of [Part A](../../part-a.md) §2.

```
  POST /products/{id}/status {"status": "published"}
        │
        │ catalog: guard (owner? draft or inactive? not already running?)
        │          mark status = publishing, record workflow_id
        ▼
  ┌─────────────────────────────────────────────────────────────┐
  │ ProductPublishingWorkflow(product_id, actor_id, request_id)  │
  │                                                              │
  │  1. validate_product      → name, description, price, ≥1     │
  │                             variant, ≥1 media, category set  │
  │       fail → NonRetryable → compensate                       │
  │  2. process_media         → derive sizes, store in MinIO,    │
  │                             mark media ready   (retry 3×)    │
  │  3. build_search_document → flatten product+variants to the  │
  │                             projection M5 will embed         │
  │  4. sync_downstream       → initialise inventory rows for    │
  │                             every variant (idempotent)       │
  │  5. mark_published        → status=published, published_at,  │
  │                             audit row, OUTBOX row            │
  │                             ── single transaction ──         │
  │                                                              │
  │  compensate: status → draft, publish_failures row with the   │
  │              failing step and reason, media left as-is       │
  └─────────────────────────────────────────────────────────────┘
```

Step 5 is the only step that makes the product publicly visible, and it is a single transaction. There is no interleaving in which a customer can see a product whose media or inventory is not ready — which is precisely the constraint Part A states.

**`OrderWorkflow`** — the direct implementation of [Part A](../../part-a.md) §3.

```
  POST /orders  (Idempotency-Key: <uuid>)
        │
        │ order service, synchronously, before the workflow starts:
        │   a. claim idempotency key (unique insert; conflict → return existing)
        │   b. GET catalog /products/{id} for each line; reject unpublished
        │   c. snapshot name/sku/price; compute totals; currency consistency
        │   d. INSERT order (status=pending) + order_items + outbox(order.created)
        │   e. start OrderWorkflow, workflow_id = f"order-{order_id}"
        │   f. return 202 {order_id, status: pending, _links.self}
        ▼
  ┌─────────────────────────────────────────────────────────────┐
  │ OrderWorkflow(order_id)                                      │
  │                                                              │
  │  1. reserve_inventory(order_id, lines)                       │
  │       insufficient → NonRetryable                            │
  │       ✗ → set_order_status(failed, INSUFFICIENT_STOCK)       │
  │                                                              │
  │  2. set_order_status(awaiting_payment)                       │
  │  3. charge_payment(order_id, amount, currency)               │
  │       retry 3× on transient; NonRetryable on decline         │
  │       ✗ → COMPENSATE: release_inventory                      │
  │            → set_order_status(failed, PAYMENT_DECLINED)      │
  │                                                              │
  │  4. commit_inventory(order_id)      reserved → committed     │
  │       ✗ → COMPENSATE: refund_payment, release_inventory      │
  │                                                              │
  │  5. create_shipment(order_id, address)                       │
  │       ✗ → COMPENSATE: refund_payment, release_inventory      │
  │            → set_order_status(failed, FULFILMENT_FAILED)     │
  │                                                              │
  │  6. set_order_status(confirmed) + outbox(order.confirmed)    │
  │  7. notify(order_id, "confirmed")   ← outbox row only in M2  │
  │                                                              │
  │  cancellation signal accepted at steps 1–3; after step 4 a   │
  │  cancel becomes a refund request, not a cancellation         │
  └─────────────────────────────────────────────────────────────┘
```

**Order state machine.**

```
                    ┌─────────┐
                    │ pending │ ◀── created
                    └────┬────┘
           reserve ok    │    reserve fails
          ┌──────────────┴───────────────┐
          ▼                              ▼
  ┌─────────────────┐              ┌──────────┐
  │ awaiting_payment│              │  failed  │ (terminal)
  └────────┬────────┘              └──────────┘
   paid    │    declined ─────────────▶ failed
           ▼
    ┌─────────────┐   shipment fails ──▶ refunding ──▶ failed
    │  confirmed  │
    └──────┬──────┘
           │ shipment events (simulated in M2, webhook-driven in M3)
           ▼
    ┌──────────┐     ┌───────────┐     ┌───────────┐
    │ shipped  │ ──▶ │ delivered │     │ cancelled │ (from pending /
    └──────────┘     └───────────┘     └───────────┘  awaiting_payment)
```


**The transition table is the diagram, transcribed.** Under [RFC 0006](0006-service-decomposition-and-data-model.md) S4 these three columns are `text`, so the database no longer rejects a bad status. What replaces it is stricter than what it replaces, and it lives in `services/order/app/domain/state_machine.py` — one file, next to the enum it constrains:

```python
ORDER_TRANSITIONS: Mapping[OrderStatus, frozenset[OrderStatus]] = {
    S.PENDING:          frozenset({S.AWAITING_PAYMENT, S.FAILED, S.CANCELLED}),
    S.AWAITING_PAYMENT: frozenset({S.CONFIRMED, S.FAILED, S.CANCELLED}),
    S.CONFIRMED:        frozenset({S.SHIPPED, S.REFUNDING}),
    S.SHIPPED:          frozenset({S.DELIVERED, S.REFUNDING}),
    S.DELIVERED:        frozenset({S.REFUNDING}),
    S.REFUNDING:        frozenset({S.REFUNDED, S.FAILED}),
    S.CANCELLED:        frozenset(),   # terminal
    S.FAILED:           frozenset(),   # terminal
    S.REFUNDED:         frozenset(),   # terminal
}
```

`PAYMENT_TRANSITIONS` and `SHIPMENT_TRANSITIONS` are declared the same way. Every status change goes through one guarded method, which raises rather than assigning:

```python
def transition(self, target: OrderStatus, *, actor: str, reason: str | None = None):
    if target not in ORDER_TRANSITIONS[self.status]:
        raise IllegalTransition("order", self.id, "status", self.status, target)
    self.status = target
```

`IllegalTransition` maps to `409` through the problem-detail handler. Nothing outside `transition()` assigns to `status`, `payment_status` or `shipment_status` — that is a review check, and it is what makes the guard total.

**Why this is strictly stronger than the type it replaces.** Let *V* be the value set and *T ⊆ V × V* the legal transition relation. A Postgres `ENUM` on `orders.status` constrained an `UPDATE` to *new ∈ V* — it checked the label and never the edge. It therefore accepted all 81 ordered pairs over the nine order states. `ORDER_TRANSITIONS` accepts 14. The 67 it now rejects are not hypothetical curiosities; they are the writes that actually corrupt an order: un-delivering (`delivered → pending`), reviving a terminal state (`cancelled → confirmed`), and skipping payment entirely (`pending → confirmed`). Every one of those was always valid against a native `order_status` type.

The honest boundary of the claim: the enum's guarantee applied to *every* writer including a hand-run `psql` statement, while the transition table applies only to writers that go through `OrderService`. This is strictly stronger **within the application**, not within the database — the trade is recorded in [RFC 0006 §3.3](0006-service-decomposition-and-data-model.md#33-tradeoffs), and it is bounded by R1 giving `order_db` exactly one service and one role.

**Both guards run in the same transaction, and neither subsumes the other.** The S6 `version` check answers *"has anyone else changed this row since I read it?"* — a question about concurrency. `transition()` answers *"is this move legal from where the row actually is?"* — a question about the state machine. `version` alone permits a perfectly serialised `delivered → pending`; `transition()` alone permits two workers both reading `confirmed` and both legally moving to `shipped`, writing two shipments. `OrderService.advance()` performs the version check, the transition, the S7 audit row and the B11 outbox row in one transaction, and both failures surface as `409`.

Because the column is `text`, a rolled-back build can read a status a newer build wrote; the read policy is `fallback` rather than `passthrough` precisely here, because a state machine must never branch on a bare string. An unrecognised status degrades to `pending`, every transition out of `pending` that does not apply is rejected, and the row freezes loudly instead of corrupting quietly.

**Inventory model.**

```
   on_hand   = physically present
   reserved  = held for in-flight orders
   available = on_hand - reserved       ← what a customer can buy

   reserve : available ≥ qty  →  reserved += qty         (row lock)
   release : reserved  -= qty                            (compensation)
   commit  : reserved  -= qty ; on_hand -= qty           (payment settled)
   restock : on_hand   += qty                            (merchant / return)

   every one of the above appends a stock_movements row with reason + ref
```

The reserve path is the contended one. It runs as `SELECT ... FOR UPDATE` on a single `stock_items` row inside a short transaction, with a `CHECK (on_hand >= reserved >= 0)` constraint as the last line of defence. Under a flash sale the lock serialises writers on one row, which is correct — oversell is worse than queueing.

#### 3.1.2. API changes

**order — customer surface**

| Method | Path | Auth | Role | Purpose |
| --- | --- | --- | --- | --- |
| `POST` | `/api/v1/orders` | bearer | `customer` | Create. **Requires `Idempotency-Key`.** Body: `{items: [{product_id, variant_id, quantity}], shipping_address, payment_method}`. Returns `202` with `{order_id, status, _links}` |
| `GET` | `/api/v1/orders` | bearer | `customer` | The caller's own orders. Filters: `status`, `from`, `to`. Paginated |
| `GET` | `/api/v1/orders/{id}` | bearer | owner, `admin` | Full order with items, payment state, shipment state, and a `timeline[]` of transitions |
| `POST` | `/api/v1/orders/{id}/cancel` | bearer | owner, `admin` | Signals the workflow. `409` if past the cancellable point, with a problem detail explaining why |

**order — merchant and admin surface**

| Method | Path | Auth | Role | Purpose |
| --- | --- | --- | --- | --- |
| `GET` | `/api/v1/orders/merchant` | bearer | `merchant` | Orders containing the caller's products, with only that merchant's lines |
| `POST` | `/api/v1/orders/{id}/shipment/status` | bearer | `admin` | Simulates a carrier callback: `shipped`, `in_transit`, `delivered`. Replaced by a webhook in M3 |
| `GET` | `/api/v1/orders/{id}/workflow` | bearer | `admin` | Workflow id, run id, current state, failure reason — for diagnosis without opening the Temporal UI |

**inventory**

| Method | Path | Auth | Role | Purpose |
| --- | --- | --- | --- | --- |
| `GET` | `/api/v1/inventory/{variant_id}` | public | — | `{on_hand, reserved, available, in_stock}`. Public because a product page needs it |
| `GET` | `/api/v1/inventory` | bearer | `merchant`, `admin` | The caller's stock across variants. Filters: `low_stock`, `out_of_stock` |
| `PUT` | `/api/v1/inventory/{variant_id}` | bearer | owner `merchant`, `admin` | Set absolute `on_hand`. Writes a ledger row with reason `manual_adjustment` |
| `POST` | `/api/v1/inventory/{variant_id}/adjust` | bearer | owner `merchant`, `admin` | Relative `{delta, reason}`. Preferred over `PUT` because it is safe under concurrency |
| `GET` | `/api/v1/inventory/{variant_id}/movements` | bearer | owner `merchant`, `admin` | The ledger — the audit trail for stock |
| `POST` | `/api/v1/inventory/reservations` | internal | — | Reserve. Called only by the workflow activity; rejected unless the caller presents the service token |
| `DELETE` | `/api/v1/inventory/reservations/{id}` | internal | — | Release |
| `POST` | `/api/v1/inventory/reservations/{id}/commit` | internal | — | Commit |

**catalog — changed and added**

| Method | Path | Change |
| --- | --- | --- |
| `POST` | `/api/v1/products/{id}/status` | `published` now returns **`202`** with `{workflow_id, status: "publishing"}` instead of `501`. The pre-announced change from [RFC 0001](0001-foundation-and-core-services.md) §3.1.4 |
| `GET` | `/api/v1/products/{id}/publish-status` | *New.* `{state, step, started_at, failure_reason?}` — so a merchant can see why a publish failed without a Temporal login |
| `POST` | `/api/v1/products/{id}/media/upload` | *New.* Multipart binary upload to MinIO, replacing the URL-only attachment of M1. The URL form remains for seeding |

**Internal authentication.** Activities call `inventory` and `catalog` as a service, not as a user. A short-lived service JWT is minted by `identity` from a client-credentials grant, carrying `sub: "service:temporal-worker"` and a `scope` claim. Internal routes require that scope and reject user tokens; user-facing routes reject service tokens. Sharing a role for both would mean a stolen customer token could call `/inventory/reservations`.

#### 3.1.3. Data/schema changes

**Every column of every table in the platform is specified in
[RFC 0006 §3.1.3](0006-service-decomposition-and-data-model.md#313-dataschema-changes).**
This section names only what this milestone creates or amends; it does not
restate columns, so a type has exactly one definition in the RFC set.

**Databases created:** `order_db`, `inventory_db`.

| Table | Database | Action | Columns |
| --- | --- | --- | --- |
| [`orders`](0006-service-decomposition-and-data-model.md#orders) | `order_db` | Created — order, totals, payment and shipment sub-state, Temporal ids | [0006](0006-service-decomposition-and-data-model.md#orders) |
| [`order_items`](0006-service-decomposition-and-data-model.md#order_items) | `order_db` | Created — line snapshots (B8) | [0006](0006-service-decomposition-and-data-model.md#order_items) |
| [`order_status_history`](0006-service-decomposition-and-data-model.md#order_status_history) | `order_db` | Created — append-only timeline | [0006](0006-service-decomposition-and-data-model.md#order_status_history) |
| [`idempotency_keys`](0006-service-decomposition-and-data-model.md#idempotency_keys) | `order_db` | Created — one order per key (B7) | [0006](0006-service-decomposition-and-data-model.md#idempotency_keys) |
| [`stock_items`](0006-service-decomposition-and-data-model.md#stock_items) | `inventory_db` | Created — `on_hand`, `reserved`, generated `available`; the lock target | [0006](0006-service-decomposition-and-data-model.md#stock_items) |
| [`reservations`](0006-service-decomposition-and-data-model.md#reservations) | `inventory_db` | Created — held stock per order, with expiry (B5) | [0006](0006-service-decomposition-and-data-model.md#reservations) |
| [`stock_movements`](0006-service-decomposition-and-data-model.md#stock_movements) | `inventory_db` | Created — append-only signed ledger (B6) | [0006](0006-service-decomposition-and-data-model.md#stock_movements) |
| [`product_publish_failures`](0006-service-decomposition-and-data-model.md#product_publish_failures) | `catalog_db` | Created — a failed publish is a record, not a log line | [0006](0006-service-decomposition-and-data-model.md#product_publish_failures) |
| [`outbox`](0006-service-decomposition-and-data-model.md#outbox) | `catalog_db`, `order_db`, `inventory_db` | Created — S12, written in the state-change transaction (B11) | [0006](0006-service-decomposition-and-data-model.md#outbox) |

**Amended.** `catalog.products` gains `publish_workflow_id` (`text`, nullable) —
the running or last workflow. `catalog.product_media` gains `storage_key`,
`content_type`, `size_bytes`, `processing_status` and `derived`, for the media
pipeline (B12).

**Vocabulary changes (S4), not migrations.** `products.status` gains `publishing`
between `draft` and `published`: one line in `catalog/app/domain/enums.py` and one
edge in its transition table. No Alembic revision, no `ALTER TYPE`, no deploy
ordering constraint, because the `text` column already accepts the string —
`alembic check` must report no pending diff. New vocabularies: `OrderStatus`,
`PaymentStatus`, `ShipmentStatus`, `IdempotencyState`, `ReservationStatus`,
`StockMovementReason`, `MediaProcessingStatus`.

All additive. No existing column changes type, and no data migration is required.
Under the original S4 the `products.status` change above would have been an
`ALTER TYPE … ADD VALUE` revision with no writable `downgrade` — the concrete case
that motivated replacing it ([RFC 0006](0006-service-decomposition-and-data-model.md#why-enum-values-are-validated-in-the-model-not-the-database-s4)).

#### 3.1.4. Breaking changes

| Change | Impact | Handling |
| --- | --- | --- |
| `POST /products/{id}/status` with `published`: `501` → `202` | The pre-announced change from [RFC 0001](0001-foundation-and-core-services.md) §3.1.4 | Announced a milestone in advance; nothing was allowed to code against the `501` |
| `products.status` gains `publishing` | A client switching exhaustively on status sees a new value. This is an API concern, not a schema one — the column's type does not change (S4) | No external clients. The OpenAPI enum is generated from `ProductStatus`, so it cannot drift from what the service accepts |
| Media attachment moves from URL-only to upload-plus-URL | The M1 URL form still works | Additive; the seed script keeps using URLs |
| The seed script no longer writes `published` directly | It now drives the real workflow | Better: the M1 shortcut noted as a known issue in RFC 0001 is retired here, and the seed exercises the publishing path on every run |

#### 3.1.5. Action Plan

| # | Task | Output | Depends on |
| --- | --- | --- | --- |
| T1 | Temporal + MinIO in Compose under a `workflow` profile; `temporal-worker` skeleton; connectivity smoke test | Temporal UI reachable, worker registers | RFC 0001 done |
| T2 | `libs/common`: outbox model and helper, idempotency helper, service-token mint and verify, Temporal client factory | Unit-tested, importable | T1 |
| T3 | `inventory` service skeleton, Alembic, all four tables | Migration to head | T2 |
| T4 | Inventory core: reserve / release / commit with row locking and ledger writes | Concurrency test — 50 parallel reserves on 10 units yield 10 successes | T3 |
| T5 | Inventory merchant APIs: read, set, adjust, movements, low-stock filter | Tests green, ownership enforced | T4 |
| T6 | Reservation expiry sweeper | Test: an expired reservation returns stock and writes a ledger row | T4 |
| T7 | `catalog` changes: `publishing` status, workflow id, failures table, media upload to MinIO | Migration + upload test | T2 |
| T8 | `ProductPublishingWorkflow` + its five activities | Workflow tests including per-step failure injection | T7, T4 |
| T9 | Publish endpoint returns `202`; publish-status endpoint | Contract tests | T8 |
| T10 | `order` service skeleton, Alembic, all five tables | Migration to head | T2 |
| T11 | Idempotency middleware — claim, replay, conflict, in-progress | Concurrent-duplicate test (G4) | T10 |
| T12 | Order creation: catalog snapshot, validation, totals, persistence, workflow start | Tests green | T11, T9 |
| T13 | Payment and shipping provider interfaces + deterministic fakes | Every branch reachable by configuration | T2 |
| T14 | `OrderWorkflow` + activities + compensations | Failure-injection tests at every step (G3, G5) | T12, T13, T4 |
| T15 | Order read APIs: customer list, detail with timeline, merchant view | Tests green, isolation enforced | T12 |
| T16 | Cancellation signal and the post-commit `409` | Tests green | T14 |
| T17 | Shipment status simulation endpoint and the resulting transitions | Tests green | T15 |
| T18 | Outbox writes wired into every state change in all four services | Test: rollback leaves no outbox row (G8) | T14, T8 |
| T19 | Seed extended — stock for every variant, products published through the real workflow, sample orders in several states | `make seed` idempotent and green | T18 |
| T20 | End-to-end: publish → order → confirm, and the two failure paths | `make test` green cold | T19 |
| T21 | README and RFC updates; architecture diagram refreshed | Docs current | T20 |

### 3.2. Rationale

**Why Temporal rather than a chain of Kafka consumers.** Part A requires that an interrupted order be "resumable or cleanly reversible" and that partial publishing failures never corrupt product state. With choreography, the answer to "where is order X stuck?" is "read six consumers' logs and infer", compensation is scattered across the consumers that happen to run after a failure, and step timeouts have to be hand-rolled everywhere. Temporal answers that question with a query, keeps compensation adjacent to the step it undoes, and persists workflow state so a worker crash resumes rather than loses. This is also why Part A lists Temporal at all — using Kafka for it would leave that requirement unmet.

**Why reserve before charging.** If payment runs first and the reservation then fails, money has been taken for stock that does not exist, and the fix is a refund the customer did not ask for. Reserving first means the worst case is a released reservation, which costs nothing and is invisible. The ordering is chosen so that the cheap failure comes first.

**Why the ledger.** Part A's opening complaint is that stock disagrees across systems. A current-value column alone makes disagreement undetectable — the number is simply wrong, with no way to find out when or why. An append-only ledger makes `sum(deltas)` reconcilable against the stored value, so a drift is a detectable, diagnosable event rather than a mystery. It also gives merchants the stock history they will ask for immediately.

**Why idempotency keys are claimed by unique insert.** Read-then-write has a window: two concurrent requests both find no key, both proceed, both create an order. That window is narrow under light load and wide under exactly the burst traffic Part A describes, which is the worst possible property — it works in testing and fails in the demo. A unique constraint has no window; the database arbitrates.

**Why `202` and not `201`.** Payment authorisation against a real provider takes hundreds of milliseconds to seconds, and shipment creation is another network call. Holding the HTTP connection through both is the checkout latency Part A opens with, and it makes the client's timeout the order's timeout. Returning the order id immediately means a client retry is a poll, not a second order.

**Why product data is snapshotted rather than referenced.** A merchant repricing a product must not retroactively change what a customer paid. The snapshot is what the order *means*; that it also removes a cross-service join is a bonus rather than the motivation.

**Why the outbox is written now, with nothing reading it.** The dual-write problem — commit to Postgres, then publish to Kafka, and crash in between — is the standard way event-driven systems lose events, and it is invisible until it happens. Writing outbox rows from the first state change means Milestone 3 adds a relay process and a topic configuration, rather than revisiting every handler in four services during the busiest week of the project.

**Why `publishing` is a real status rather than an in-memory flag.** A workflow can take seconds. During that window the product is neither draft nor published, and a second publish request must be rejected rather than starting a competing workflow. A status value makes that a persisted, transactional fact that a second request can see and be rejected against; a flag makes it a race. Under S4 the rejection comes from the transition table — `publishing` has no edge back to itself — rather than from a database type, which is the stricter of the two: a Postgres enum would have accepted a second write of `publishing` without complaint.

**Why activities are thin.** If business logic lives in activity code, it can only be tested inside a Temporal test environment, and it cannot be reached by the HTTP route that needs the same rule. Keeping activities as adapters over the service layer means one implementation, two callers, and fast unit tests — the seam RFC 0001 built the layering for.

### 3.3. Tradeoffs

| Trade being made | The cost | Why it is acceptable |
| --- | --- | --- |
| Temporal in the stack | A server, a UI, a worker, and a new mental model in week 2 | Required by Part A, and it is the correct tool for the two processes specified. The alternative is hand-rolling durability badly |
| Asynchronous order creation (`202`) | Clients must poll; the demo is less immediate than a blocking `201` | The workflow completes in well under a second with fakes, so the demo is unaffected in practice, and the contract is honest about what the system does |
| Synchronous `order → catalog` call at creation | `catalog` being down blocks order creation; adds latency to the create path | The alternative — a local product projection — introduces a staleness window on price and availability, which is worse. Revisited in M3 (Q3) |
| Row-level locking on reserve | Writers serialise on a hot variant during a flash sale | Correct behaviour. Oversell is a business failure; queueing is a latency cost. The lock is held for microseconds |
| Reservation expiry sweeper | A background loop and a second path that changes stock | The safety net for a workflow lost beyond Temporal's recovery. Without it, one bad day strands stock permanently |
| Payment and shipping simulated | No real integration experience | Not asked for by either part. The interface boundary makes a real provider a later swap, and the fake makes every failure branch testable — which a real sandbox would not |
| Outbox rows accumulating unread for a week | A growing table with no consumer | Bounded by a week of local test data; a sweeper arrives with the relay in M3 |
| `order_items` duplicating product data | Storage, and a corrected product name never reaches historical orders | That is the intended semantics of an order record |
| Two new services in one week | Week 2 is heavy — two schemas, two workflows, a saga, and idempotency | `inventory` is deliberately small (four tables, one hard function). The action plan orders T3–T6 first so the order workflow has something to reserve against |
| A single `stock_items` row per variant | No multi-warehouse or per-branch stock, which Part A's "multi-branch retail" framing hints at | Explicitly out of scope; the ledger's `reference_type` and a future `location_id` make it additive rather than a rewrite |

#### 3.3.1. Challenges and Open Questions

| # | Question | Current position | Needs a decision by |
| --- | --- | --- | --- |
| Q1 | Should `order` call `catalog` synchronously or keep an event-fed product projection? | Synchronous ([RFC 0000](0000-system-architecture.md) A13). Correctness of price and availability at purchase outweighs the coupling | Revisit M3, once `catalog.product.updated` exists |
| Q2 | Reservation TTL — 15 minutes, or shorter? | 15 minutes. Long enough to cover a slow payment retry, short enough that stranded stock returns within a demo | M3, if flash-sale testing shows contention |
| Q3 | One workflow per order, or one long-running workflow per customer? | One per order. `workflow_id = order-{order_id}` gives free deduplication at the Temporal level as a second line of defence behind the idempotency key | Decided |
| Q4 | Does cancellation after payment auto-refund, or require an admin? | Auto-refund in M2, because the fake provider makes it trivial and the saga needs the activity anyway. A real provider would need an approval step | Revisit if returns are ever in scope |
| Q5 | Should stock be per-variant only, or per-variant-per-location? | Per-variant. Multi-location is a schema addition (`location_id` in the unique key), not a redesign | Out of scope; recorded so it is not accidental |
| Q6 | Direct-to-MinIO presigned upload, or upload through `catalog`? | Through `catalog` in M2 — simpler, and it lets validation happen before anything is stored. Presigned is the scalable answer | M3, if upload size becomes a problem |
| Q7 | Where does media processing run — Temporal activity or Celery task? | Temporal activity in M2, because Celery does not exist yet. Moves to Celery in M3, with the activity becoming an enqueue-and-await | M3 |
| Q8 | How does a merchant learn a publish failed, before notifications exist? | `GET /products/{id}/publish-status` and the failures table. A notification arrives in M3 | M3 |
| Q9 | Should `order` enforce that every line belongs to one merchant? | No — a multi-merchant order is allowed and the merchant view filters lines. Splitting into per-merchant sub-orders is a real marketplace concern and is out of scope | Recorded |
| Q10 | Is optimistic concurrency (`version`) enough on orders, given the workflow is the main writer? | Yes. The workflow is effectively a single writer per order; `version` protects against a concurrent admin action | Decided |

---

## 4. Dependencies/blockers/known issues

**New dependencies.**

| Dependency | Version | Used for |
| --- | --- | --- |
| Temporal server + UI | 1.25.x | Workflow orchestration |
| `temporalio` (Python SDK) | 1.8.x | Workflows, activities, worker |
| MinIO + `boto3` | latest / 1.35.x | Media object storage |
| Pillow | 11.x | Image derivative generation |
| `python-multipart` | 0.0.x | Multipart upload in FastAPI |

Temporal needs its own Postgres database (`temporal`, `temporal_visibility`) in the same instance, created by the existing init script.

**Blockers.** None. Every dependency is a public image or package, and the simulated providers remove the only external-account requirement.

**Known issues carried into this milestone.**

- Outbox rows accumulate with no consumer until M3.
- No notification reaches a customer; `notify` writes an outbox row and a log line only.
- Shipment progression is driven by an admin endpoint rather than a carrier.
- Media derivatives are generated synchronously inside an activity, which makes the publishing workflow slower than it will be once Celery takes the work (Q7).
- No analytics: order and revenue figures exist only as rows to be queried by hand.
- Temporal's UI is the only workflow observability; there is no tracing correlation between an HTTP request and its workflow until M3.
- No rate limiting on order creation. The idempotency key prevents duplicates but not volume.

---

## 5. Alternate solutions

**Alternative 1 — Choreographed saga over Kafka; no Temporal.** `order` publishes `order.created`; `inventory` reacts and publishes `inventory.reserved`; a payment consumer reacts, and so on.

*Rejected.* Compensation logic ends up distributed across consumers that must each know what to undo; there is no single place that knows where an order is; timeouts and retries are reimplemented per consumer; and debugging a stuck order means reading several consumer logs. It also inverts the dependency — the services would need to know the process, which is what the orchestrator is for. Part A's resumability requirement is precisely Temporal's strength.

**Alternative 2 — Two-phase commit / XA across services.**

*Rejected.* Blocking, poor failure behaviour, and unsupported across the mix of stores involved. Sagas with compensation are the accepted answer for exactly this problem.

**Alternative 3 — Synchronous orchestration inside the order service** — a Python function calling inventory, payment, and shipment in sequence, with `try/except` compensation.

*Rejected,* though it is genuinely the fastest thing to write. It fails on process death: if the service is killed between charging and committing inventory, nothing resumes and nothing compensates, and the only evidence is a log line that may not have flushed. It also holds the HTTP connection for the full sequence, and every retry policy and timeout has to be hand-written. This is the alternative Temporal exists to replace, and choosing it would leave Part A's core reliability requirement unmet.

**Alternative 4 — Celery chains with `on_error` callbacks.**

*Rejected.* Celery has no workflow state, no visibility into where a chain stopped, and error callbacks that themselves fail are lost. It is the right tool for one-shot background jobs and is used for exactly that in M3.

**Alternative 5 — Optimistic inventory: sell first, reconcile later.** No reservation; decrement on confirmation and handle oversell by cancelling.

*Rejected.* It converts a technical constraint into a customer-facing failure — the customer who receives a cancellation after paying. Acceptable for some businesses at some margins, and explicitly not what Part A asks for, which names inventory consistency as the problem to solve.

**Alternative 6 — Inventory as a column on `product_variants` in `catalog`.** Simpler: no new service, no cross-service reservation.

*Rejected.* It makes `catalog` a second writer on a fact that `order` also needs to change, which is the exact multi-writer inconsistency Part A opens by describing. [RFC 0001](0001-foundation-and-core-services.md) already declined to add the column for this reason.

**Alternative 7 — Redis for reservations** rather than Postgres rows.

*Rejected for now.* Faster under contention, and genuinely how high-scale systems do this. But reservations must survive a restart and must be reconcilable against the ledger, and Redis does not exist in the stack until M3. Postgres row locks are sufficient at the demonstrated scale, and the reservation interface is narrow enough that a Redis-backed implementation is a later swap.

---

## 6. Production readiness

### 6.1. Scalability and Fault tolerance

- **Workflow durability.** Temporal persists every workflow event. A worker killed mid-order resumes from the last completed activity on restart; no order is lost and none is silently abandoned. This is G5 and is tested by actually killing the worker.
- **Activity retries.** Exponential backoff, capped attempts, and an explicit `NonRetryableError` for business failures — a declined card is retried zero times, a network blip up to three.
- **Timeouts at every level.** `start_to_close` per activity, `schedule_to_close` per attempt chain, and a workflow-level execution timeout, so nothing hangs indefinitely.
- **Oversell prevention under concurrency.** Row lock plus a `CHECK` constraint. Tested at 50 concurrent orders against 10 units (G6).
- **Duplicate prevention at two layers.** The idempotency key at the API, and `workflow_id = order-{order_id}` at Temporal, which rejects a duplicate start.
- **Backpressure.** The Temporal task queue buffers when workers are saturated; workers scale horizontally with `--scale`; order creation stays fast because it only persists and starts a workflow.
- **Blast radius.** `inventory` being down fails reservations, which fails orders cleanly with a recorded reason — it does not hang the API or corrupt state. `catalog` being down blocks order creation, which is the accepted cost of Q1.
- **Stranded-stock recovery.** The expiry sweeper (B5), independent of Temporal, so a failure mode Temporal cannot see still self-heals.
- **Known ceilings.** Single-node Temporal; one Postgres; a hot variant row serialises writers. None is a schema decision that blocks scaling.

### 6.2. Security considerations

- **Internal endpoints** (`/inventory/reservations*`) require a service-scoped token and reject user tokens outright. A customer token cannot reserve or commit stock.
- **Order isolation** — a customer reads only their own orders; a merchant sees only their own lines within an order, never another merchant's lines or the customer's full basket; `admin` sees all. This is the highest-value test in the milestone.
- **Stock isolation** — a merchant can adjust stock only for variants of products they own, checked against the snapshot `merchant_id` in `stock_items`, not against a request field.
- **No card data anywhere.** The request carries a `payment_method` token; the fake provider returns a reference. There is no field in any schema that could hold a card number.
- **Idempotency keys are scoped per user,** so one caller cannot probe or collide with another's keys.
- **Amounts are computed server-side** from the catalog snapshot. A client-supplied price is ignored, not trusted — the obvious attack on any checkout.
- **Media upload** — content-type allow-list, size ceiling, extension and magic-byte validation, randomised storage keys, and no execution path over uploaded content.
- **Workflow inputs carry identifiers, not secrets.** Temporal history is readable in its UI; nothing sensitive is put in it.
- **Accepted gaps** — no rate limiting on order creation (M3); no fraud checks; MinIO runs with default credentials from `.env` locally.

### 6.3. Deployment strategy

- **Compose profile `workflow`** adds `temporal`, `temporal-ui`, `minio`, `temporal-worker`, `order`, `inventory`. `make up` starts `core` + `workflow` from this milestone on.
- **Temporal schema** is set up by an init container before the server starts; its two databases are created by the existing Postgres init script.
- **Worker deployment** is separate from the services it calls, so workflow code can be redeployed without restarting the API. Temporal's versioning handles in-flight workflows; with no production traffic, the simple path — drain and restart — is used.
- **Migrations** remain per service and explicit.
- **Rollback.** Alembic downgrade per service. Workflow code rollback requires care with in-flight executions; locally, `make reset` is the answer.
- **MinIO bucket** is created idempotently at startup by an init script, with a versioning-off, private-by-default policy.

---

## 7. Test Plan

**Unit** — no I/O.

- Order state machine: every legal transition, every illegal one rejected — all 67 illegal pairs parametrised individually, asserting `IllegalTransition` and that the entity is left unmutated. Plus structural tests that `ORDER_TRANSITIONS`'s keys are exactly `OrderStatus`'s members, every target is a known state, every state is reachable from `pending`, and the terminal set is exactly `{cancelled, failed, refunded}` — these catch a typo'd edit to the table, which under S4 is the only thing standing between the service and a bad transition.
- Total calculation: multi-line, multi-quantity, rounding at two decimal places, mixed-currency rejection.
- Inventory arithmetic: reserve, release, commit, expire; the `available` invariant never negative.
- Idempotency: key canonicalisation, request hashing, same-key-different-body detection.
- Payment and shipping fakes: each configured branch returns what it claims.
- Outbox helper: envelope construction, correlation propagation.

**Integration** — real Postgres, per service.

*inventory*
- Reserve with sufficient, insufficient, and exactly-equal stock.
- **50 concurrent reservations against 10 units → exactly 10 succeed, 40 receive an explicit insufficient-stock error, `on_hand` unchanged, `reserved` = 10** (G6).
- Release restores `available`; commit reduces `on_hand`; both write ledger rows.
- Double-reserve for one `(order_id, variant_id)` is rejected by the unique constraint (B3).
- Expiry sweeper releases and ledgers.
- Ledger reconciliation: `sum(delta_on_hand) == on_hand` after a random sequence of operations.
- Ownership: a merchant adjusting another merchant's variant → `403`.

*order*
- Creation: valid order → `202`, `pending`, items snapshotted with the catalog's values at that moment.
- **Ten parallel requests with one `Idempotency-Key` → one order, ten identical responses** (G4).
- Same key, different body → `422`. In-progress duplicate → `409`.
- Unpublished, archived, or unknown product → `422` with a problem detail naming the line.
- Mixed currencies → `422`. Zero or negative quantity → `422`.
- Isolation: another customer's order → `404` (not `403`, which would confirm it exists); merchant view shows only own lines; admin sees all.
- Cancellation before payment succeeds; after commit → `409` with an explanatory detail.

*catalog*
- Publish returns `202` and sets `publishing`; a second publish while running → `409`.
- Publish-status reflects each stage and a failure reason.
- Media upload: accepted types stored, rejected types refused, oversize refused.

**Workflow tests** — Temporal test environment, time-skipping.

- `ProductPublishingWorkflow`: happy path reaches `published` with `published_at` set.
- Each of the five steps forced to fail in turn → product ends `draft`, a `product_publish_failures` row names the step, and **no `published` state is ever observed** (G1).
- Transient failure retried then succeeding → still reaches `published` exactly once; `mark_published` is idempotent.
- `OrderWorkflow`: happy path reaches `confirmed`; stock committed; outbox row written.
- Payment declined → inventory released, order `failed` with `PAYMENT_DECLINED`, `available` back to its starting value (G3).
- Shipment creation fails → refund issued, inventory released, order `failed`.
- Insufficient stock → order `failed` immediately, no payment attempted.
- Activity retried twice then succeeding → exactly one charge, verified against the fake's call log (B3).
- Cancellation signal at each stage: honoured before commit, `409` after.

**End-to-end** — through the gateway, cold start.

1. Seed an admin, a merchant, a customer, and a category.
2. Merchant creates a product with a variant, uploads media, sets stock.
3. Merchant publishes → `202` → poll until `published`.
4. Customer places an order → `202` → poll until `confirmed`.
5. Assert: stock reduced by the ordered quantity; the ledger reconciles; the order timeline shows every transition with actors; outbox rows exist in `order_db` and `catalog_db`.
6. Failure path: order a product whose payment token is the declining one → order `failed`, stock unchanged from step 5.
7. **Resumability:** kill `temporal-worker` mid-order, restart it, assert the order reaches a terminal state with no orphaned reservation (G5).

**Manual verification for the milestone review** — `make reset && make up && make migrate && make seed && make test` from a clean clone; the Temporal UI shows completed and compensated executions; a deliberately failed publish is visible in `publish-status`.

**Explicitly not tested** — load and performance beyond the concurrency tests above; anything belonging to M3 onwards.

---

## 8. Impact Analysis

**On Milestone 1's code.** Additive. `catalog` gains columns, a status value, and an upload route; nothing it already does changes behaviour. The status value costs no migration at all under S4 — it is an edit to `catalog/app/domain/enums.py`. `identity` gains a client-credentials grant for service tokens. `gateway` gains two route prefixes. `libs/common` gains the outbox, idempotency, and Temporal client helpers — all non-domain, consistent with its stated rule. The S4 enum machinery it already carries from Milestone 1 needs no extension; `order` and `inventory` supply their own `app/domain/enums.py`.

**On later milestones.**

| Milestone | What this RFC makes easy | What it makes harder |
| --- | --- | --- |
| 3 — Events & observability | Outbox rows already exist in four services, so Kafka integration is a relay plus topic configuration, not a change to any handler. The event catalogue's payloads are already the shapes being written | Twenty-one containers. Temporal and MinIO arrive before Kafka and the observability stack, and the resource ceiling is reached in M3 |
| 4 — Part A review | The two workflows are the centrepiece; failure injection tests demonstrate the reliability claims rather than asserting them | — |
| 5 — Retrieval | `build_search_document` already produces the flattened product projection the indexer will embed | The projection is built inside the workflow; M5 must also handle products published before it existed, via a backfill |
| 6 — AI assistant | Order history and live stock are available through APIs the assistant can call for grounded answers | — |

**On effort.** The heaviest week of Part A alongside Milestone 3. Two services, two workflows, a saga with compensations, and idempotency. The mitigation is in the action plan's ordering: `inventory` (T3–T6) is small and lands first so that the order workflow has something real to reserve against, and the two workflows (T8, T14) are independent of each other.

**Risks.**

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Temporal's learning curve consumes the week | **High** | High | T1 is a connectivity spike on day one; `ProductPublishingWorkflow` is deliberately the simpler workflow and is built first as the learning vehicle |
| Compensation logic is subtly wrong and only fails under a race | Medium | High | Failure injection at every step is a required test, not an optional one; the ledger makes any inventory drift detectable |
| Idempotency implemented as read-then-write | Medium | High | Explicitly designed as a unique-insert claim (B7), and the concurrent-duplicate test is the acceptance criterion |
| Scope creep into returns, promotions, multi-warehouse | Medium | Medium | Listed as non-goals with the reason attached |
| Activity code accreting business logic, making it untestable | Medium | Medium | B1 is a review check |
| MinIO and media processing swallowing time better spent on the saga | Medium | Medium | Media is one activity with a simple implementation; derivatives move to Celery in M3 anyway |

**On reviewers.** Milestone 2's review should be able to check: a publish that fails leaves `draft` and says why; a payment that fails leaves stock exactly as it was; a retried order creates one order; a killed worker resumes; and no business rule lives in workflow code.

---

## 9. Useful Links/References

**Project documents**

- [RFC 0000 — System Architecture](0000-system-architecture.md) · [RFC 0001 — Foundation & Core Services](0001-foundation-and-core-services.md)
- [Part A](../../part-a.md) · [Milestones](../../milestones.md) · [Execution Guidelines](../../execution-guidelines.md)

**Technical references**

- Temporal Python SDK — https://docs.temporal.io/develop/python
- Temporal testing and time-skipping — https://docs.temporal.io/develop/python/testing-suite
- Saga pattern — https://microservices.io/patterns/data/saga.html
- Transactional outbox — https://microservices.io/patterns/data/transactional-outbox.html
- Stripe idempotent requests (the reference implementation of the header contract) — https://docs.stripe.com/api/idempotent_requests
- IETF draft, The Idempotency-Key HTTP Header Field — https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/
- PostgreSQL explicit locking — https://www.postgresql.org/docs/16/explicit-locking.html
- MinIO Python SDK — https://min.io/docs/minio/linux/developers/python/minio-py.html

---

## 10. Related tickets/issues

| ID | Title | Maps to |
| --- | --- | --- |
| *(to be created)* | M2-T1 … M2-T21 | The tasks in §3.1.5, one ticket each |
| — | Milestone 2 — Core Business Logic | This RFC |
| — | RFC 0001 §3.1.4 | The pre-announced `501` → `202` change, delivered here |
| — | RFC 0003 — Events & Observability | Consumes the outbox rows this RFC writes |

---

## 11. Notes

- **On the milestone's breadth.** The source text for Week 2 is one sentence — "focusing on core business logic and implementation of end-end assignment". Read literally it is unbounded. The scope above is the intersection of [Part A](../../part-a.md) §2 and §3 with what one week can carry, and every exclusion is listed in §2.2 with the milestone that owns it.
- **On the outbox.** Writing outbox rows that nothing reads for a week looks like waste and is the opposite. It is the single decision here that most reduces Milestone 3's risk, and Milestone 3 is the week most likely to overrun.
- **On simulated providers.** The fakes are not placeholders to be replaced later; within this project they are the design ([RFC 0000](0000-system-architecture.md) A9). They are more useful than a real sandbox for the purpose at hand, because they can be told to fail on demand — which is what the saga's tests need.
- **Definition of done for Milestone 2.** Goals G1–G9 verified by their stated checks; `make up && make migrate && make seed && make test` green from a clean clone; the failure-injection and concurrency suites green; README updated with the two workflow diagrams; this RFC moved to **DONE** with review outcomes folded in.

---

## 12. Comments

*Reviewer comments go here. Please raise objections against the numbered decisions (B1–B12), the open questions (Q1–Q10), or the rejected alternatives in §5, so the discussion attaches to something specific.*

| Reviewer | Section | Comment | Resolution |
| --- | --- | --- | --- |
| | | | |
