# Business logic and decisions — traceability

This page lists every business rule SmartRetail implements. For each rule it gives:

- **Rule**: what the system does, with the real states, numbers and error codes.
- **Why**: the reason we chose it.
- **Source**: the requirement it comes from — a section of
  [Part A](part-a.md), [Part B](part-b.md) or the
  [Execution Guidelines](execution-guidelines.md) — or **Silent**, meaning the
  requirements say nothing and the rule is our own decision.
- **Alternatives**: what we rejected, and why. Alternatives marked *(not in RFC)*
  are realistic options that no RFC records; they are listed so a reviewer can
  ask about them.
- **ID**: the decision ID in the owning RFC, so the full argument can be found.

Status markers:

- ✅ implemented
- 🟡 implemented with a deviation from the RFC
- ⏳ designed in the RFC, not yet built

The status was checked against the code on 2026-10-20.

[business-flows.md](business-flows.md) walks the same flows as diagrams. This
page adds the *why* and the *where from*.

## Contents

1. [What the requirements give us, and what they leave open](#1-what-the-requirements-give-us-and-what-they-leave-open)
2. [Architecture-level decisions](#2-architecture-level-decisions)
3. [Identity: users, roles, merchants, tokens](#3-identity-users-roles-merchants-tokens)
4. [Catalog and the publishing workflow](#4-catalog-and-the-publishing-workflow)
5. [Inventory](#5-inventory)
6. [Order and checkout](#6-order-and-checkout)
7. [Checkout failure matrix (payment failure and the rest)](#7-checkout-failure-matrix)
8. [Cancellation and refunds](#8-cancellation-and-refunds)
9. [Events, messaging and background work](#9-events-messaging-and-background-work)
10. [Notifications](#10-notifications)
11. [Analytics — how each metric is defined](#11-analytics--how-each-metric-is-defined)
12. [AI assistant (Part B)](#12-ai-assistant-part-b)
13. [Tech stack — mandated versus chosen](#13-tech-stack--mandated-versus-chosen)
14. [Every decision taken where the requirements are silent](#14-every-decision-taken-where-the-requirements-are-silent)
15. [Designed but not built, and known deviations](#15-designed-but-not-built-and-known-deviations)

---

## 1. What the requirements give us, and what they leave open

The three source documents describe **capabilities**. They do not describe
**mechanisms**.

| The requirements say | They do not say |
| --- | --- |
| Roles `customer`, `merchant`, `admin` (Part A §1) | Who may become an admin; whether a merchant needs approval |
| Product states "for example `draft`, `published`, `inactive`" (Part A §1) | The full state machine, or what happens when a publish fails |
| Publishing: validate → media → search records → sync → "ready only after processing completes" (Part A §2) | The validation rules, retry counts, or where a failed publish goes |
| Order: record → reserve → pay → ship → notify → analytics (Part A §3) | What happens when payment fails, whether stock or payment comes first, when a customer may cancel |
| "Idempotent retries", "Prevention of duplicate orders" (Part A §3) | How a retry is recognised, or what the response is |
| "Backpressure handling", "Recovery from partial failures" (Part A §3) | Timeouts, compensation order, reservation expiry |
| Nine analytics metrics, by name only (Part A §5) | How any of them is computed (for example, is a refunded order revenue?) |
| Tech stack: "NoSQL DB", "Vector DB", "LLM Provider (OpenAI / Groq / Anthropic)" | Which product to use for each |
| Assistant: Q&A, compare, vague queries, streaming (Part B §2) | Chunking, retrieval method, cost limits, guardrails |

Nothing in Parts A or B mentions a gateway, a cart, a payment provider, a
service count, token lifetimes, or traffic figures. Each of those is a
decision recorded below.

---

## 2. Architecture-level decisions

Owner: [RFC 0000](rfcs/0000-architecture.md), decision register §3.2.1.

| # | Rule | Why | Source | Alternatives rejected | ID |
| --- | --- | --- | --- | --- | --- |
| 2.1 | **Eight services** (gateway, identity, catalog, order, inventory, notification, analytics, assistant). A candidate becomes a service only if it passes a four-part test: owned state, an invariant, an independent reason to change, and a distinct operational profile | Boundaries follow data ownership, so they survive requirement changes. Eighteen candidates were tested, and the rejections are recorded | **Silent** — the requirements name no services. Indirectly: Part A §6 "Clear separation of responsibilities"; Guidelines §1 "Understand distributed commerce systems" | Modular monolith (the strongest alternative: faster and simpler, but Kafka becomes ceremony and the distributed-systems objective is lost); 15 services, one per noun (empty boxes); a split by capability (two services write one fact) | ARC-D1, ARC-D2 |
| 2.2 | **Payment and shipment live inside `order`**, behind `PaymentProvider` and `ShippingProvider` interfaces | There is no compliance boundary and no cardholder data. A separate service would need a distributed transaction to record one fact | **Silent** | Separate services (an empty box plus a cross-database transaction) | ARC-D3, A9 |
| 2.3 | **Payments and carriers are simulated** with deterministic fakes that can be told to succeed, decline, time out or fail | What we need to show is how the saga behaves under each failure, not the integration itself | **Silent** — Part A says "payment workflow" and "shipment request" but names no provider | A real sandbox account, such as Stripe test mode (an external dependency the brief does not need) | A9, B10 |
| 2.4 | **No cart.** An order is placed with a line-item body | Neither Part A nor Part B mentions a cart | **Silent** | *(not in RFC)* A Redis-backed cart; a draft order that is later submitted | Q7 |
| 2.5 | **One database and one role per service**, on one PostgreSQL instance | A cross-service query fails when it connects, not at review. Eight Postgres containers will not fit on a laptop next to Kafka and Temporal | **Silent** | A shared database with a schema per service (the boundary becomes advisory); one instance per service | ARC-D5, A1, A2 |
| 2.6 | **No foreign key crosses a service.** Cross-service ids are plain UUIDs | Each service deploys, migrates and restores on its own | **Silent** | Cross-database foreign keys (impossible); a shared ORM model library | ARC-D6, A3 |
| 2.7 | **Snapshot data at the boundary instead of joining across it.** For example, an order line stores the name, SKU and price at purchase | A later price change must not rewrite history | Part A Goal 3: "accurate, durable" | Looking up the live product (wrong domain behaviour, and a runtime dependency) | ARC-D7, A14 |
| 2.8 | **Exactly one synchronous write-path call: `order → catalog`** at order creation. Every other cross-service effect is an event | Price and publish status at purchase must be authoritative | **Silent** | A local product projection in `order` (a staleness window on exactly the wrong fact) | ARC-D10, A13 |
| 2.9 | **Temporal runs multi-step processes, Kafka carries facts, Celery runs one-shot jobs** | They answer three different questions; using one tool for another's job is a known anti-pattern | Part A Tech Stack names all three; the split between them is ours | Kafka choreography for the saga (nothing shows where an order is stuck); Kafka for everything; Celery chains | ARC-D11, A12 |
| 2.10 | **Transactional outbox, at-least-once delivery, idempotent consumers** | Writing to the database and then publishing loses events on a crash between the two. Kafka's exactly-once guarantee stops at Kafka | Part A §4: "traceable and recoverable … avoid double-processing" | Publishing straight from the handler; Kafka transactions; event sourcing (too costly for five weeks) | ARC-D12, A10, A11 |
| 2.11 | **Enums are stored as `text`**, with an explicit transition table in code | A native Postgres enum cannot be downgraded (`ADD VALUE` has no inverse), and it checks the label, never the transition. The order enum would accept all 81 state pairs; the transition table allows 14 | Guidelines Week 2: "State transitions and validations" | Native enum; `CHECK (status IN …)`, kept as the escape hatch; a lookup table with a foreign key | ARC-D18, S4 |
| 2.12 | **Money is `numeric(12,2)` and `Decimal`**, never float | A float cannot represent 0.10, so totals drift from the sum of their lines | **Silent** | Float; integer cents *(not in RFC, also valid)* | S2 |
| 2.13 | **UUID primary keys** wherever an id leaves the database | Sequential ids leak volume and collide when merged in analytics | **Silent** | `bigserial` everywhere; UUID everywhere (random inserts into the hottest indexes) | ARC-D17, S1 |
| 2.14 | **Each service owns its audit trail**, written in the same transaction as the change | An audit row outside that transaction allows a change with no trail | Part A §1: "change history and auditability … who made it and when" | A central audit service; database triggers (cannot see who made the change) | ARC-D19, S7 |

---

## 3. Identity: users, roles, merchants, tokens

Owner: [RFC 0003](rfcs/0003-identity-service.md). Code: `services/identity/app/v1/`.

| # | Rule | Why | Source | Alternatives rejected | ID / status |
| --- | --- | --- | --- | --- | --- |
| 3.1 | Exactly **three roles**, `customer`, `merchant` and `admin`, owned by identity and carried as a JWT claim | Identity is the only writer of role | Part A §1: "user registration with appropriate roles: customer, merchant, admin"; Guidelines Week 1 | A native enum; a separate merchant service | IDN-D14 ✅ |
| 3.2 | **Nobody can register as admin.** Registration accepts only `customer` or `merchant` (anything else gets `422`). Admins come from the seed or from promotion by another admin. The last active admin cannot be demoted or deactivated | Every other privilege boundary is only as strong as this one | **Silent** | Letting the registrant pick any role | IDN-D14, I3 ✅ |
| 3.3 | **Merchant lifecycle:** starts `pending`; then `pending → approved`, `pending → suspended`, `approved ⇄ suspended`. There is **no `rejected` state** — a rejection is `suspended`. Repeating the current status is a no-op that emits no event | One transition table is the single source of truth | Part A §1 "Merchant onboarding" defines no states → **Silent** | A dedicated `rejected` state | IDN-D15 🟡 — the code also allows `suspended → pending` on a role change, which the RFC forbids |
| 3.4 | **An admin approves merchants** with `POST /merchants/{id}/status` | Onboarding implies a review step | **Silent** | Auto-approve on registration *(not in RFC)* | IDN-D26 ⏳ — endpoint not built; today status only changes through the admin role `PATCH` |
| 3.5 | **Only approved merchants may publish** | — | **Silent** | Catalog checks the `merchant_status` claim and returns `403` *(proposed in IDN-Q1)*; a projection table in catalog | ⏳ **Open question** — a `pending` merchant can currently create and publish |
| 3.6 | **Suspension:** login returns `403 merchant-suspended`, refresh returns `401`, and every live session is revoked | Caps the window during which a suspended merchant can still act at 15 minutes (one access-token lifetime) | **Silent** | Catalog calling identity on every request (breaks the one-sync-call rule) | IDN-D16 ✅ (the catalog side ⏳) |
| 3.7 | **Tokens:** RS256 access token valid for **15 min**; opaque refresh token valid for **7 days**, stored hashed and **rotated on every use**. Re-use of an old refresh token revokes all of that user's sessions | Services verify tokens locally, with no call to identity. No shared secret exists whose leak would let someone forge tokens. Re-use detection follows the OAuth security best practice | **Silent** — Guidelines say only "auth basics" | HS256 shared secret; token introspection on every request; server-side sessions; Keycloak/Auth0; a JWT as the refresh token | IDN-D1–D6, A4 ✅ |
| 3.8 | **Every service re-verifies the token.** The gateway only rejects early | Services can be called directly by internal callers | **Silent** | Treating the gateway as the security boundary | A5 ✅ |
| 3.9 | **Argon2id** password hashing (time cost 3, 64 MiB, parallelism 4). Passwords are 12–128 characters. A dummy hash makes timing equal for unknown and known emails | Memory-hard, and OWASP's first choice. The same 401 for both cases stops email enumeration | **Silent** | bcrypt (truncates at 72 bytes); PBKDF2 | IDN-D9–D12 ✅ |
| 3.10 | **Service tokens** for workers (client credentials, 5 min, scoped). `/internal/v1/*` routes accept only service tokens; user routes reject them | A workflow outlives its request and has no user token. Keeping the two token types apart means a stolen customer token cannot reserve stock | **Silent** | Forwarding the user's token; a shared `service` role | IDN-D7, ARC-D9 ✅ |
| 3.11 | **Authorisation is decided by the service that owns the row.** For example, catalog checks `product.merchant_id` against the token claim | Only the owner knows what a row means | **Silent** | A central policy service | ARC-D8 ✅ |

---

## 4. Catalog and the publishing workflow

Owner: [RFC 0004](rfcs/0004-catalog-service.md). Code: `services/catalog/app/v1/`.

### 4.1 Product state machine

`draft`, `publishing`, `published`, `inactive`, `archived`. Any other transition
returns `409 illegal-transition`.

| From | To | Trigger |
| --- | --- | --- |
| — | `draft` | create |
| `draft` ⇄ `inactive` | | merchant toggles |
| `draft` / `inactive` | `publishing` | publish request (`202`) |
| `publishing` | `published` | `mark_published` — the workflow only |
| `publishing` | `draft` | compensation (publish failed) |
| `published` | `draft` | unpublish |
| `published` | `inactive` | merchant suspended ⏳ |
| `draft` / `inactive` / `published` | `archived` | terminal |

- **Source:** Part A §1 "Product state handling — for example `draft`, `published`, `inactive`"; Part A §2 "marked as ready only after processing completes".
- **Our additions:** `publishing` and `archived` (the requirements are silent on both). `publishing` is a stored state, so a second publish request can see that a publish is already running.

### 4.2 Rules

| # | Rule | Why | Source | Alternatives rejected | ID / status |
| --- | --- | --- | --- | --- | --- |
| 4.1 | **Publishing is a Temporal workflow:** validate → process media → build search document → sync downstream (create stock rows in inventory) → `mark_published`. The final step writes status, audit and outbox event in one transaction | Only the last step is visible to anyone, so nothing earlier needs undoing | Part A §2 steps 1–5; Part A Goal 2 "search indexing, media optimization, variant synchronization …"; Guidelines Week 2 "Product publishing workflow using Temporal" | Setting the status directly; Kafka choreography; a Celery chain | CAT-D5, CAT-D7 ✅ |
| 4.2 | **A failed publish goes back to `draft`**, never half-published. The compensation records the failed step and the reason in `product_publish_failures`; the merchant reads them at `GET /products/{id}/publish-status`. It returns to `draft` even when the publish started from `inactive` | `draft` means "needs attention" | Part A §2: "partial failures must not corrupt product state … must not be left half-published" | Return to `inactive` (open question CAT-Q3); undoing each step in reverse (nothing visible to undo) | CAT-D7, CAT-D31 ✅ |
| 4.3 | **Validation for completeness:** name, description and category set; at least one active variant; at least one media item, which must be `ready` by `mark_published`. Failure is non-retryable and goes straight to compensation | "Published" must always mean "complete" | Part A §2 step 1: "validated for completeness"; the exact fields are **Silent** | *(not in RFC)* Required attributes per category; a configurable rule set | 🟡 — `base_price > 0` is not checked; a price of `0` passes |
| 4.4 | **No edits while `publishing`** (`409`) | Keeps the validation from step 1 still true at step 5 | Implied by Part A §2 | Allowing edits (the workflow could publish something it never validated) | CAT-D14 ✅ |
| 4.5 | `POST /publish` commits `publishing` **before** it starts the workflow, then returns **`202`**. The workflow id is `product-publish-{id}-v{version}` with `REJECT_DUPLICATE`. If the start fails, the product goes back to `draft` and the response is `503` | A workflow never runs against a product that is not in `publishing`. The response is asynchronous so the client's timeout does not become the workflow's timeout | Part A §4: "should not block the customer's request" | Starting the workflow first, then committing (can orphan a workflow); a random workflow id | CAT-D9, CAT-D10 ✅ |
| 4.6 | **Retries:** every step has a bounded retry policy (for example, media 3 attempts, sync 8 attempts). Compensation retries for up to 24 h. Workflow timeout 1 h | Every workflow is guaranteed to end | **Silent** (Part A §4 "recoverable") | Unbounded retries | CAT-D11 ✅ |
| 4.7 | **Media:** at most 10 MiB; jpeg, png or webp, checked by magic bytes; stored in MinIO. Each image becomes three WebP sizes (150, 600 and 1200 px) with EXIF stripped, in a Celery job. An image that fails 3 times is marked `failed`, which fails the publish; the next publish retries it. Only `ready` media are public | Validate before storing. CPU work stays off the workflow worker | Part A §1 "product assets"; Part A §2 step 2 "Images and media assets should be processed" | Presigned direct upload (deferred); processing inside the activity; a separate media service | CAT-D12, D13, D33 ✅ |
| 4.8 | **SKU is unique per merchant** (products) and per product (variants). A violation returns `409` | The invariant that justifies catalog being its own service | **Silent** | *(not in RFC)* Globally unique SKU; GTIN/EAN required | I1, I2 ✅ |
| 4.9 | **Variants:** free-form `options` stored as `jsonb` (for example size or colour), validated by Pydantic. Deleting a variant only deactivates it. The last active variant of a published product cannot be deactivated | Variant dimensions are defined by each merchant and differ per category | Part A Goal 1: "variants such as size or color"; `jsonb` is **Silent** | Fixed columns; a dimension schema per category | CAT-D21, D23 ✅ |
| 4.10 | **Pricing** = product `base_price` + one currency + an optional per-variant `price_override`. **No promotional-rules engine** | Part A asks only for "basic" support. Price is a product attribute, so there is no pricing service | Part A §1: "Basic pricing and promotional rule support" — **promotions are a deliberate scope cut** | A pricing service; *(not in RFC)* a `compare_at_price` with a date window, or a percent/fixed promotion table by product or category | RFC 0004 §2.2 🟡 scope cut |
| 4.11 | **Categories:** admin only, a tree with unique slugs. Cycles are rejected. A category cannot be deleted while products or child categories use it | Reference data owned by catalog | Part A §1 "categories"; admin-only is **Silent** | A shared reference-data service | I11 ✅ |
| 4.12 | **Audit log** for every product, variant, media and status change, in the same transaction. It records the actor, the action, a before/after diff and the workflow id. Exposed at `GET /products/{id}/history` | Shows who changed what, and when | Part A §1: "change history and auditability" | Database trigger; central audit service | CAT-D19 ✅ |
| 4.13 | **Optimistic concurrency:** every write carries `version`; a mismatch returns `409` | No silent overwrites | Part A §6: "High consistency"; the mechanism is **Silent** | Last writer wins | CAT-D20 ✅ |
| 4.14 | **Edits to a published product apply immediately** and emit `catalog.product.updated`. An edit that would break completeness returns `409` | No need to unpublish to fix a typo | Part A §1: "Each update should ensure consistency across all relevant systems" | Re-running the workflow on every edit | CAT-D15 ✅ |
| 4.15 | **Catalog stores no stock.** Responses link to inventory's availability instead | A second writer of stock is exactly the bug Part A describes | Part A Challenges: "Inconsistent inventory" | A `quantity` column on variants | CAT-D2 ✅ |
| 4.16 | **Merchant suspended → their published products become `inactive`**, through an event consumer | Hide a suspended merchant's products | **Silent** | A projection table (CAT-Q1) | CAT-D18 ⏳ |
| 4.17 | **Browse cache:** Redis, 60 s, cleared by `catalog.product.*` events | Bounded staleness on the hot read path | Part A Tech Stack "Redis — caching"; Part A "high latency" | No cache | CAT-D24 ⏳ |

---

## 5. Inventory

Owner: [RFC 0006](rfcs/0006-inventory-service.md). Code: `services/inventory/app/v1/`.

| # | Rule | Why | Source | Alternatives rejected | ID / status |
| --- | --- | --- | --- | --- | --- |
| 5.1 | **Inventory is the only writer of stock**, with one `stock_items` row per variant | Several writers (orders, restocks, returns) compete for one hot row | Part A Challenges: "Stock information disagrees across online stores, warehouses, and physical branches"; Guidelines §4 "Inventory consistency" | A stock column in catalog | INV-D1 ✅ |
| 5.2 | **Stock rows are created at 0 when a product is published**, by both the workflow's sync step and the `catalog.product.published` consumer. Both are idempotent. A republish never resets the quantity | Zero is the only honest starting quantity | Part A Goal 2: "Availability propagation"; the zero start is **Silent** | The merchant stocks before publishing (INV-Q4) | INV-D16 ✅ |
| 5.3 | **Merchants set quantity.** `POST /adjust` applies a relative change (preferred). `PUT` sets an absolute value and requires `If-Match: version`. Either returns `409 stock-below-reserved` if `on_hand` would fall below `reserved` | A relative change never overwrites a concurrent commit | Part A Goal 1: "Operations teams … monitor inventory"; who sets quantity is **Silent** | Absolute set only (lost updates) | INV-D19, D20 ✅ |
| 5.4 | **Invariant `on_hand ≥ reserved ≥ 0`.** Checked under `SELECT … FOR UPDATE`, which gives a clear `409 insufficient-stock`. A database `CHECK` constraint is the backstop | Overselling is impossible | Part A §3: "sustained correctness when order volume spikes"; Guidelines Week 2 "Reliable inventory reservation flow" | A single guarded `UPDATE` (cannot tell "unknown variant" from "out of stock"); `SERIALIZABLE` isolation (aborts transactions on the hot path) | INV-D6 ✅ |
| 5.5 | **`available = on_hand − reserved`** is a stored generated column | One definition, so no caller can compute it differently | Part A Goal 1: "product availability" | Computing it in code | INV-D4 ✅ |
| 5.6 | **Lock order and timeouts:** rows are locked in ascending `variant_id` order. `lock_timeout` 2 s, 3 retries, then `503 inventory-contended` (retryable) | A fixed lock order makes deadlock impossible, and no request hangs | Part A §3: "Backpressure handling" | Locking in request order (can deadlock) | INV-D7, D8 ✅ |
| 5.7 | **Flash-sale behaviour:** writers queue on the row ("overselling is worse than queueing"). An unlocked fast-reject read keeps sold-out variants out of the lock queue; it can refuse but never grant | Correctness first, with bounded latency | Part A §3: "High traffic order bursts"; Part A Goal 4 "Sudden spikes" | Sharded stock rows; reservations in Redis; sell first and cancel later | INV-D9 ✅ |
| 5.8 | **A reservation is all-or-nothing** across every line of the order. It is idempotent on `(order_id, variant_id)`: the same quantities replay; different ones return `409` | Every retry carries the same natural key | Part A §3 step 2 and "Idempotent retries" | A separate idempotency-key table | INV-D10 ✅ |
| 5.9 | **Reservation states:** `held` → `committed`, `released` or `expired`, all terminal. A committed reservation never goes back to `held` | — | Guidelines Week 2 "State transitions"; the states are **Silent** | — | I8 ✅ |
| 5.10 | **Reservations hold for 15 min** (TTL) | Long enough for slow payment retries, short enough that stranded stock comes back | **Silent** | No expiry (stock stranded forever); a shorter TTL (slow payments fail) | INV-D11 ✅ |
| 5.11 | **An expiry sweep runs every 60 s** (Celery Beat). It re-checks each candidate under a row lock, then moves it to `expired`, writes a ledger row and emits `inventory.released`. Safe to run on two workers | A safety net beneath saga compensation that does not depend on Temporal | Part A §4: "recoverable … avoid double-processing" | *(not in RFC)* `FOR UPDATE SKIP LOCKED` batch claim | INV-D12 ✅ (note: [business-flows.md](business-flows.md) wrongly says SKIP LOCKED) |
| 5.12 | **Commit follows status, not the clock.** A `held` reservation past its expiry that has not been swept yet can still commit | It still holds its stock, so committing cannot oversell, and refusing would cause a needless refund | **Silent** | Refusing past `expires_at` | INV-D13 ✅ |
| 5.13 | **Reverse commit** (shipment failed after commit) adds the quantity back to `on_hand`. The reservation stays `committed`; a ledger `compensation` row records the reversal | No new state needed | Part A §3: "cleanly reversible" | A new `reversed` state | INV-D14 ✅ |
| 5.14 | **Append-only ledger `stock_movements`.** Every change is a signed delta, written in the same transaction. Invariant: the sum of deltas equals the stored values | Makes "stock disagrees" detectable | Part A Challenges "Inconsistent inventory"; Part A §6 "diagnose … inventory sync" | Keeping the current value only; event sourcing | INV-D3 ✅ (daily reconciliation job ✅ since 2026-10-20, INV-D32; append-only grant ⏳) |
| 5.15 | **`inventory.stockout` event** fires on `depleted` (available reaches 0), `low_stock` (falls below 5) and `reservation_refused`. The refused case is written in its own transaction after the rollback | Feeds the stockout-rate metric. An event written inside a rolled-back transaction would vanish | Part A §5: "Inventory Stockout Rate" | Writing it inside the reserve transaction | INV-D21, D22 ✅ |
| 5.16 | **One location only:** stock is per variant. Multi-location would add `location_id` to the unique key | Out of scope for five weeks | Part A Overview mentions "warehouses, and physical branches" — **we narrowed this** | `location_id` now; a per-location table with an aggregated view | RFC 0006 §2.2 (scope cut) |

---

## 6. Order and checkout

Owner: [RFC 0005](rfcs/0005-order-service.md). Code: `services/order/app/v1/orders/`.

### 6.1 Creating an order

| # | Rule | Why | Source | Alternatives rejected | ID / status |
| --- | --- | --- | --- | --- | --- |
| 6.1 | **`Idempotency-Key` header required** (`400` if missing or malformed) | A retry has to be recognisable as the same request | Part A §3: "Idempotent retries — a retried request must not produce a second order"; "Prevention of duplicate orders"; Guidelines Week 2 | *(not in RFC)* Deriving the key from a hash of the body | ORD-7 ✅ |
| 6.2 | **The key is claimed with a unique insert**, scoped to the user (`ON CONFLICT (user_id, key) DO NOTHING`) | Read-then-write races under exactly the burst load Part A describes. Scoping per user stops one caller probing another's keys | Part A §3: "High traffic order bursts"; per-user scope is **Silent** | Read-then-write; Redis as the arbiter (not durable) | ORD-D6, B7 ✅ |
| 6.3 | **Replay rules:** same key and same body → the original `202` is replayed with `Idempotency-Replayed: true`. Same key and different body → `422`. First request still in flight → `409` with `Retry-After: 1`. Keys live 24 h. An in-progress claim older than 30 s can be taken over | A retry must see the original answer; a client bug must not alter an order | Part A §3; the exact codes are **Silent** | Replaying with a different status code | ORD-D7 ✅ |
| 6.4 | **Business rejections are stored and replayed**, but dependency failures (catalog `503`, a database error) **release the key** | A transient outage must not be replayed for ever | **Silent** | Storing every failure | ORD-D7 ✅ |
| 6.5 | **`POST /orders` returns `202 Accepted`** with `status: pending` | Holding the connection through payment is the "checkout delays under load" problem | Part A Challenges: "Checkout delays under load"; Part A §3 "Backpressure handling" | A blocking `201` | ORD-D8, B9 ✅ |
| 6.6 | **Price snapshot.** `order → catalog` (the user's token, 2 s timeout) fetches name, SKU and price; `unit_price = price_override ?? base_price`. The client cannot send a price. A product that is unknown or unpublished returns `422`; catalog being down returns `503` | Repricing must not change what a customer paid | Part A Goal 3: "accurate, durable" | A live price lookup; a local projection | A13, A14, ORD-D9, D10 ✅ |
| 6.7 | **Line limits:** 1–50 lines, quantity 1–999, at most one line per variant. **Stock is not checked at creation** — the reservation is the check | Checking twice would race | Guidelines Week 2 "validations"; the numbers are **Silent** | *(not in RFC)* Merging duplicate lines; a pre-check of stock | ORD-6 ✅ |
| 6.8 | **One currency per order** (mixed currencies → `422`, never converted). `total = Σ line_total + shipping_fee`, rounded `ROUND_HALF_EVEN` | A stored total never disagrees with its lines by a cent | **Silent** | Currency conversion; a currency per line | ORD-D11 ✅ |
| 6.9 | **One transaction:** order (`pending`) + lines + history + outbox `order.created` + the completed idempotency key. The workflow starts after the commit | "Recorded reliably" before anything else happens | Part A §3 step 1: "The order must be recorded reliably" | Publishing after the commit; event sourcing | ORD-D16 ✅ |
| 6.10 | **The order is accepted even if Temporal is down.** A Celery Beat job, `resume_unstarted_orders`, runs every 60 s. It starts any `pending` order older than 30 s that has no workflow. The workflow id `order-{order_id}` is deterministic, so a double start is harmless | Accepting an order must not depend on the orchestrator | Part A §3: "Recovery from partial failures"; "Backpressure handling" | Returning `503` (the order exists but the client thinks it failed); starting the workflow before the commit | ORD-D21, D35 ✅ |

### 6.2 Order state machine

Nine states. 14 of the 81 possible state pairs are legal; anything else
returns `409`.

| From | Allowed to |
| --- | --- |
| `pending` | `awaiting_payment`, `failed`, `cancelled` |
| `awaiting_payment` | `confirmed`, `failed`, `cancelled`, `refunding` |
| `confirmed` | `shipped`, `refunding` |
| `shipped` | `delivered`, `refunding` |
| `delivered` | `refunding` |
| `refunding` | `refunded`, `failed` |
| `cancelled`, `failed`, `refunded` | — (terminal) |

`confirmed` requires `payment_status = succeeded` **and**
`shipment_status = created`.

- **Payment sub-state:** `none → pending → succeeded | failed | refunded`, plus `succeeded → refunded`.
- **Shipment sub-state:** `none → created → shipped → (in_transit) → delivered`.
- **Source:** Guidelines Week 2 "State transitions and validations"; Part A Goal 3 lists "orders, payments, shipments, cancellations, and refunds". The specific states are our decision.
- **IDs:** ORD-D12–D15.

### 6.3 The saga (`OrderWorkflow`)

The steps run in this order:

1. reserve stock
2. `awaiting_payment`
3. charge
4. commit stock
5. create shipment
6. `confirmed`
7. notify

| # | Rule | Why | Source | Alternatives rejected | ID |
| --- | --- | --- | --- | --- | --- |
| 6.11 | **Temporal orchestrates checkout**, one workflow per order, with a 24 h timeout | An interrupted order must be resumable or reversible, and "where is order X?" becomes a query | Part A §3 "Recovery from partial failures"; Part A Tech Stack "Temporal" | Kafka choreography; 2PC/XA; try/except in process; Celery chains | ORD-D1 ✅ |
| 6.12 | **Reserve before charging; commit only after payment** | The cheap failure (out of stock) comes before the expensive one (a charge that then needs a refund) | Part A §3 lists step 2 (reserve) before step 3 (pay); Guidelines §4 "Inventory consistency" | Charging first; selling first and reconciling later | B4, ORD-D5 ✅ |
| 6.13 | **Each step has its own timeout and retry policy.** The worst-case forward path is under 10 min, so it fits inside the 15-min reservation TTL. Compensation retries for up to 1 h | A slow workflow never has its stock swept away. "A compensation that gives up is worse than one that is late" | **Silent** | Unbounded workflow time; the same retries for business and transient errors | ORD-D18 ✅ |
| 6.14 | **Every provider call carries an idempotency key** (`order-{id}:charge`, `:refund`, `:shipment`) | Exactly one charge, even after retries | Part A §3 "Idempotent retries"; Part A §4 "Avoid double-processing" | — | B3, ORD-D17 ✅ |

---

## 7. Checkout failure matrix

Compensations run in reverse order of the steps that succeeded. Every row is a
real branch in
[`order_workflow.py`](../services/order/app/v1/orders/workflows/order_workflow.py).

> **Payment failure ends the order as `failed`, not `cancelled`.** The two are
> different outcomes:
>
> - `cancelled` means a customer or admin asked to cancel.
> - `failed` means the system could not complete the order.
>
> A declined card first **releases the stock**, then sets
> `payment_status = failed` and order `failed` with reason `PAYMENT_DECLINED`,
> and emits `payment.failed` and `order.failed`. A decline wins even if a
> cancel signal arrived at the same time (ORD-D19).

| Fails at | Cause | Compensation | Final state | Reason |
| --- | --- | --- | --- | --- |
| Reserve | Not enough stock | none needed | `failed` | `INSUFFICIENT_STOCK` |
| Reserve | Inventory down after 60 s of retries | none | `failed` | `INVENTORY_UNAVAILABLE` |
| Charge | **Card declined** (not retried) | Release reservations | `failed`, payment `failed` | `PAYMENT_DECLINED` |
| Charge | **Timeout** after 3 attempts — the charge *may* have gone through | **Always refund** (keyed, so safe if nothing was captured), then release | `failed` | `PAYMENT_TIMEOUT` |
| Commit | Reservation expired or commit failed | `refunding` → refund → release | `failed` | `RESERVATION_EXPIRED` / `COMMIT_FAILED` |
| Ship | Address rejected or carrier down | `refunding` → refund → **reverse commit** (stock back) | `failed` | `FULFILMENT_FAILED` |
| Notify | Notification failed | none — the order stays confirmed; a null `notified_at` makes the failure visible | `confirmed` | — |

**Why each of these:**

- **Payment timeout → always refund.** "The customer is never charged for an order that did not complete." Rejected alternative: assume a timed-out charge failed (the customer might be charged with no order).
- **Shipment failure → reverse commit, not release.** A committed reservation cannot go back to `held`, so stock is returned through a ledger compensation.
- **Notification failure → order still confirmed.** Part A §3 says notifications "*may*" be sent. Part A's challenge was unreliable notifications, not orders failing because of notifications.
- **Source:** Part A §3: "Recovery from partial failures — an order interrupted mid-flow must be resumable or cleanly reversible". **Which** compensation applies at each step is our decision.

**Known gaps** (checked in code):

- If the `awaiting_payment` status write fails after the reservation succeeded, the order fails without releasing the stock. The 15-min sweep returns it.
- A refund that never succeeds leaves the order in `refunding`. `REFUND_FAILED` is defined but never set, and no event is written.
- On the timeout path, `payment.failed` is emitted with a null `reason`.

---

## 8. Cancellation and refunds

| # | Rule | Why | Source | Alternatives rejected | ID / status |
| --- | --- | --- | --- | --- | --- |
| 8.1 | **A customer (owner) or an admin can cancel only before stock is committed** (workflow stages `starting`, `reserving`, `charging`). `POST /orders/{id}/cancel` signals the workflow and returns `202`. After commit it returns `409`, because cancelling then is a refund, not an undo | After commit, stock has left the shelf | Part A Goal 3 mentions "cancellations"; **when** a cancel is allowed is **Silent** | Temporal Update to close the race (ORD-Q4); cancellation allowed until shipment *(not in RFC)* | ORD-D20 ✅ |
| 8.2 | **When a cancel is honoured:** refund if already charged → release stock → `cancelled` → `order.cancelled` | Same reverse-order compensation as a failure | Part A §3 "cleanly reversible" | — | ✅ |
| 8.3 | **Inventory consumes `order.cancelled` as a safety net** and releases anything still held (normally a no-op) | Covers a cancel path that bypassed the workflow | Part A §4 "Inventory synchronization after purchase" | — | INV-D31 ✅ |
| 8.4 | **Three independent layers release held stock:** saga compensation → the `order.cancelled` consumer → the 15-min expiry sweep. Each acts only if the reservation is still `held`, re-checked under lock | No stock stays locked for ever | Part A §3 "Recovery from partial failures"; Guidelines §4 "Inventory consistency" | — | ✅ ([stock-release-safety-nets.md](stock-release-safety-nets.md)) |
| 8.5 | **Refunds after confirmation (returns)** are designed in the state machine (`confirmed`, `shipped` or `delivered` → `refunding` → `refunded`) but **nothing triggers them** | Returns are a non-goal | Part A §4 lists "Return and refund events" as an example, not a mandatory flow | An admin refund endpoint with a small refund workflow (ORD-Q1) | ⏳ |
| 8.6 | **No separate refund topic.** A refund travels as `payment_status: refunded` inside `order.failed` or `order.cancelled` | Avoids changing the event catalogue without review | **Silent** | A `payment.refunded` topic (ORD-Q2) | ORD-D26 ✅ |

---

## 9. Events, messaging and background work

Owners: [RFC 0001](rfcs/0001-platform-and-shared-infrastructure.md),
[messaging-and-consistency.md](messaging-and-consistency.md).

| # | Rule | Why | Source | Alternatives rejected | ID / status |
| --- | --- | --- | --- | --- | --- |
| 9.1 | **Outbox relay:** polls every 500 ms in batches of 100 (`FOR UPDATE SKIP LOCKED`), with one leader per database (advisory lock). It marks a row published only after Kafka acknowledges it (`acks=all`, idempotent producer). A poison row holds back only its own aggregate | The worst failure is a duplicate, never a loss | Part A §4 "traceable and recoverable" | Debezium CDC; publishing from the handler | PLT-D1–D4 ✅ |
| 9.2 | **Consumers deduplicate on `(consumer_group, event_id)`** in `processed_events`, in the same transaction as their effect. Offsets are committed manually afterwards. Rows are kept 30 days | Delivery is at-least-once | Part A §3 "Idempotent retries"; Part A §4 "Avoid double-processing" | Kafka exactly-once (stops at Kafka) | PLT-D5, A11 ✅ |
| 9.3 | **Avro + Schema Registry with `BACKWARD` compatibility.** Producers never auto-register; CI checks schemas against `main` | An incompatible change fails in CI, not in a consumer at runtime | Part A Tech Stack: "Kafka + Schema Registry — schema governance"; Avro and BACKWARD are our choice | JSON; JSON Schema | PLT-D7, ARC-D13 ✅ |
| 9.4 | **The Kafka key is the aggregate id** (`order_id`, `product_id`, `variant_id` and so on): 3 partitions, 7-day retention, and `product.published` is compacted | Events for one aggregate stay in order | **Silent** | One partition; global ordering | C3, PLT-D8 ✅ |
| 9.5 | **3 retries, then `<topic>.dlq`.** A dependency outage pauses the consumer and is never dead-lettered. DLQ replay is manual (`make dlq-replay`) | One poison message must not stall its partition, and an outage must not flood the DLQ | Part A §4 "Handle failures gracefully" | Dead-lettering everything; retrying for ever; automatic replay | C6, PLT-D6, D40 ✅ |
| 9.6 | **Celery tasks:** `acks_late`, jittered retries, at most 3. Beat runs the sweeps (reservations, unstarted orders, notification recovery, analytics reconciliation) | Fire-and-forget and scheduled work | Part A Tech Stack "Celery Workers with RabbitMQ"; Guidelines Week 3 | A separate Beat container | PLT-D10, D12 ✅ |
| 9.7 | **Correlation id:** the gateway issues `X-Request-ID`, which is copied to every outbox row as `correlation_id` and sent as a Kafka header | Lets a failure be traced across services | Part A §6 "Ability to diagnose failures" | — | A8 ✅ |
| 9.8 | **Trace context (`traceparent`) crosses Kafka** | Part A asks to diagnose failures in background workers | Part A §6; Guidelines Week 3 "Distributed tracing enabled" | Starting a new trace at the relay | PLT-D14 ⏳ (envelope fields are always null — PLT-D41) |
| 9.9 | **One error shape:** RFC 9457 problem details everywhere | One handler in `common` | **Silent** | An error shape per service | A7 ✅ |
| 9.10 | **Pagination:** limit/offset, default 20, maximum 100 (larger values are clamped) | Listings sort by arbitrary columns | **Silent** | Cursor pagination | ARC-D14 ✅ |
| 9.11 | **Gateway:** the single public entry point. It verifies JWTs early, strips inbound `X-User-*` headers, enforces a CORS allow-list and body limits (1 MiB, or 10 MiB for uploads), never retries upstream calls, and answers `404` for `/internal/**` | Edge concerns need exactly one home | **Silent** (no gateway is named) | No gateway; Traefik, Kong or Envoy; nginx | GW-D1–D32 ✅ |
| 9.12 | **Rate limits:** login 5/min per IP and per email; default 100/min per IP; orders 30/min per user; chat 3 or 10/min. Fail-open Redis counters | Blocks brute force and LLM "denial of wallet" | **Silent** (loosely Part A §3 "Backpressure") | Sliding log; GCRA; failing closed | GW-D11–D15 ✅ built 2026-10-20 (`login_email` per-email limit ⏳) |

---

## 10. Notifications

Owner: [RFC 0007](rfcs/0007-notification-service.md). Code: `services/notification/`.

| # | Rule | Why | Source | Alternatives rejected | ID / status |
| --- | --- | --- | --- | --- | --- |
| 10.1 | **Event → notification map:** `user.registered` → welcome; `order.confirmed`, `order.failed` and `order.cancelled` → customer; `shipment.status_changed` (shipped, in transit, delivered) → customer; `inventory.stockout` → merchant (in-app only). **`payment.*` produces nothing** | The payment outcome already reaches the customer through the order event; notifying on both would be the duplicate this service exists to prevent | Part A Challenges: "Order confirmation, dispatch updates, and refund notices are unreliable"; Part A §4 "Customer notifications"; the mapping is **Silent** | Per-user preferences (deferred); a separate payment notification | NTF-D5, D36 ✅ |
| 10.2 | **Refunds have no notification type of their own.** The refund appears as a sentence inside the `order_failed` or `order_cancelled` message | — | Part A names "refund notices" | A `refund_issued` notification *(not in RFC)* | 🟡 weaker than Part A's wording |
| 10.3 | **Channels:** in-app is delivered live over SSE. Email and SMS are rendered and **logged, not sent**, through adapters. Push is not built | No provider account is in scope; a real provider is one adapter | **Silent** | A real SMTP or SMS provider | NTF-D22, D27 ✅ |
| 10.4 | **At most one notification per (event, user, channel)**, with two guards: `processed_events`, plus a unique index on `(event_id, user_id, channel)` | Still holds when a replay bypasses the dedupe table | Part A §4 "Avoid double-processing" | Kafka transactions | NTF-D3, D4 ✅ |
| 10.5 | **Text is rendered once, when the event is consumed**, and stored | The stored text is the record of what the user was told | **Silent** | Rendering on read | NTF-D6 ✅ |
| 10.6 | **Email and SMS dispatch** is a Celery task: up to 5 attempts, exponential backoff capped at 300 s. A 5-min recovery sweep re-queues anything stuck | Sending inline would block a Kafka partition behind an SMTP timeout | Part A Challenges: "unreliable during traffic spikes" | Sending inside the consumer; a Temporal workflow | NTF-D8, D9 ✅ |
| 10.7 | **SSE:** a single-use 60 s ticket (because `EventSource` cannot send headers); at most 3 streams per user; a 20 s keepalive. Catch-up covers 24 h and at most 50 frames, with a per-device cursor | A JWT in the URL leaks into logs. Unbounded replay is a self-inflicted denial of service | **Silent** | Polling; WebSockets; JWT in the query string | NTF-D10–D18 ✅ |
| 10.8 | **Failure isolation:** notification only consumes events. It calls no service and nothing calls it | A notification outage can never put checkout at risk | Part A §4 "Run independently from core request flows" | Folding notifications into `order` | NTF-D1 ✅ |

---

## 11. Analytics — how each metric is defined

Owner: [RFC 0008](rfcs/0008-analytics-service.md). Code: `services/analytics/`.
Store: MongoDB.

Part A and Part B name the metrics but do not define them, so **every
definition below is our decision**.

**Pipeline rules:**

- Each event is processed in one Mongo transaction: dedupe record, then archive to `raw_events`, then `$inc` the rollups (ANL-D4–D6).
- Events are bucketed by the UTC date of `occurred_at`, never by processing time, so replays and lag land in the right day (ANL-D7).
- Counters are computed when an event arrives. Weekly and monthly views, and rankings, are computed when read (C9).
- An hourly job rebuilds the last 6 days from `raw_events` to correct drift (ANL-D19).
- **Rejected alternatives:**
  - Postgres for analytics (would contend with checkout, and leaves "NoSQL" as a token table).
  - Querying the source databases (breaks the service boundaries).
  - Kafka Streams or Flink.

### Part A metrics (Part A §5)

| Metric | Our definition | Alternative not taken |
| --- | --- | --- |
| Total Customers | +1 per `identity.user.registered` with `role=customer` | — |
| Total Merchants | +1 per registration with `role=merchant` — **registered, not approved** | Count only approved or active merchants (Q-ANL-1) |
| Total Products Published | +1 on `catalog.product.published`, −1 on `unpublished` | — |
| Orders Over Time | Per day: created, confirmed and failed orders, plus the confirmation rate | — |
| Revenue Trends | **Gross revenue: the sum of `total` on `order.confirmed`, shipping included. Cancellations and refunds are *not* subtracted.** One series per currency | Net revenue, subtracting refunds and cancels after confirmation (Q-ANL-2, open) |
| Most Popular Products | Ranked by quantity sold (default) or by revenue | — |
| Average Order Value | Revenue in the window ÷ confirmed orders in the window — never an average of daily averages | — |
| Inventory Stockout Rate | (`depleted` + `reservation_refused` stockouts) ÷ orders created. `low_stock` is excluded | Variant-days out of stock ÷ total variant-days *(not in RFC)* |
| Failed Events / Workflow Issues | `failure_stats` grouped by source and reason: `order.failed` reason, `payment.failed` reason, and DLQ arrivals counted from headers | **Failed product publishes are not counted yet** — no event exists for them (Q-ANL-5) |

### Part B metrics (Part B §Analytics)

| Metric | Our definition | Status |
| --- | --- | --- |
| AI Assistant Usage | +1 per `assistant.interaction` (chat or generation), with tokens and cost | ✅ counters (only in `/overview`) |
| Questions Asked / Answered | Asked = every chat. Answered = the model finished normally **and** retrieval returned sources **and** the request was not refused. Refusals are counted separately | ✅ counters |
| Conversion After AI Interaction | Any `order.confirmed` by the same user within **24 h** of their first interaction, reported next to a baseline and labelled *correlational* | ⏳ designed only |
| Average AI Response Latency | Mean and p95 of `latency_ms` per day | 🟡 only a latency total is stored, so p95 is not built |

**Scoping:** merchants see only their own data (the scope comes from the
token). The overview and failure metrics are admin-only (ANL-D21, D22).

---

## 12. AI assistant (Part B)

Owner: [RFC 0009](rfcs/0009-assistant-service.md). Code: `services/assistant/`.

| # | Rule | Why | Source | Alternatives rejected | ID / status |
| --- | --- | --- | --- | --- | --- |
| 12.1 | **Indexing is a Kafka consumer** of `catalog.product.*`. It checks the version and skips stale events. A product that is no longer published is removed | Catalog must not know a retrieval layer exists. Visibility fails closed | Part B §1 Content Publishing Workflow, steps 1–4 | Polling `catalog_db`; a synchronous call from catalog | AST-D3, D4 ✅ |
| 12.2 | **Chunking:** typed chunks (identity, description, attributes, variants, category). Every chunk starts with the product name. Descriptions are packed at about 256 tokens (512 max) with 80 tokens of overlap. At most 12 chunks per product | A chunk with only attributes cannot be retrieved and invites wrong-product facts. The cap stops one merchant dominating results | Part B §1 step 1 "Product text should be chunked"; the strategy is **Silent** | One vector per product; embedding raw JSON | AST-D5, D6 ✅ |
| 12.3 | **Embeddings:** Voyage `voyage-3.5`, 1024 dimensions. A deterministic `hashing-v1` fake is used locally and in CI | Anthropic has no embeddings endpoint and recommends Voyage. 1024 dimensions halve storage compared with 1536 | Part B §1 step 2; the model is **Silent** | OpenAI `text-embedding-3-small` (the original choice, AST-D8, superseded); local sentence-transformers | AST-D31 🟡 (changed from the RFC) |
| 12.4 | **Vector store:** pgvector inside `assistant_db`, HNSW index. Filters (published, category, price) are applied in SQL before the vector scan | No extra container for a catalogue of a few thousand products. Filters and similarity run in one query | Part B Tech Stack "Vector DB" → **pgvector** | Qdrant (kept behind a `VectorStore` interface); Weaviate, Milvus, Elasticsearch | ARC-D21, AST-D2 ✅ |
| 12.5 | **Hybrid retrieval:** dense search + Postgres full-text search, 50 candidates each, fused with RRF (k=60). If embeddings are down, search falls back to keyword only, marked `degraded` | Dense search is weak on SKUs and brands; keyword search is weak on vague intent | Part B Problem: "keyword-based and lacks semantic understanding" | Dense only; a reranker (deferred) | AST-D9, D10 ✅ |
| 12.6 | **Price and stock are fetched live** from catalog and inventory, never taken from the index (2 s timeout). On failure the value is `null` and the model is told to say "unknown" | A stale price must never be shown as current | Part B §2A "Use business context"; the mechanism is **Silent** | Embedding price and stock | AST-D11 ✅ |
| 12.7 | **A LangGraph state graph:** guard → memory → classify intent → retrieve / compare / follow-up / clarify / smalltalk / refuse → hydrate → assemble the context. Tools are called by nodes; the model never chooses tools | The path really branches. Model-chosen tools are non-deterministic and widen the prompt-injection surface | Part B Tech Stack "LangGraph / LangChain" | A linear chain; an agent that calls tools on its own | AST-D14 ✅ |
| 12.8 | **Vague queries** (one content word or less, or the classifier says "too vague") get at most two clarifying questions (budget, purpose, recipient) | Don't answer from weak matches | Part B §2A: "Handle vague or incomplete queries" | Retrieving anyway; returning nothing | AST-D15 ✅ |
| 12.9 | **Compare:** up to 3 products, one retrieval per term, falling back to a whole-message search | — | Part B Goal 1 "Compare these two laptops"; Guidelines Week 5 "Product compare assistant" | — | E6 ✅ |
| 12.10 | **Budget and gift questions** are handled by semantic search plus the LLM. Price limits apply only through explicit filters in the request | — | Part B Goal 1 examples; the mechanism is **Silent** | *(not in RFC)* Extracting a price ceiling or age range from the text into filters | 🟡 not committed in the RFC |
| 12.11 | **Grounding and guardrails:** answer only from the provided `<products>`, cite `[product:<id>]`, sanitise merchant text when indexing, block messages that look like injection, max 2000 characters. The assistant never reads orders or personal data | Any merchant can plant instructions in product text that reach other customers' prompts | **Silent** (Part B §2A "meaningful answers") | A system-prompt warning alone | AST-D16–D18 ✅ |
| 12.12 | **Cost limits:** 50,000 tokens per day per user and 2,000,000 per day globally, checked before streaming starts (`429`). Context budget 6000 tokens | A public endpoint that calls a paid API is a denial-of-wallet target | Part B Expected Outcomes: "Scalable AI processing layer"; the numbers are **Silent** | No budget; Redis counters (designed; Postgres is used instead) | E10, AST-D21, D34 🟡 |
| 12.13 | **Streaming:** SSE over `POST`, events in the order `status` → `sources` → `token`… → `usage` → `done`. Sources are sent **before** the first token. On disconnect the provider call is cancelled and the partial answer saved | One-way and tied to one request. Sending sources first means a mid-stream failure still shows products | Part B Goal 3: "streaming or asynchronous delivery"; Guidelines Week 5 | WebSockets; polling | E3, E5, AST-D19 ✅ |
| 12.14 | **LLM provider: Anthropic.** `claude-sonnet-5-5` answers and generates; `claude-haiku-5-5` classifies intent. A scripted fake is the default for local runs and CI | It is the account we have. The small model keeps classification cheap. Tests make no network calls | Part B Tech Stack "LLM Provider (OpenAI / Groq / Anthropic)" → **Anthropic** | OpenAI; LangChain provider adapters | AST-D12, D30 ✅ |
| 12.15 | **Merchant content:** `description`, `seo_title` (≤60 characters), `faq` (4–6 pairs), `promotional`, `benefits_summary`, `cross_sell`. The output is a **draft**; applying it is a separate catalog call. A bulk job covers up to 200 products (Celery, `202`) | The merchant stays in control of what is published | Part B Goal 2 and §2B | Generating and applying in one call | AST-D23, D24 ✅ |
| 12.16 | **Prompts are versioned rows**, seeded from `ops/prompts/`. Every generation records its prompt version | Output can be traced to the prompt that produced it | **Silent** | Prompts hard-coded in the service | AST-D26 🟡 (chat messages don't record the version) |
| 12.17 | **Every interaction emits `assistant.interaction`** through the outbox. Analytics computes the AI metrics from it | Metrics live in one place | Part B Analytics | Computing metrics inside the assistant | AST-D25 ✅ |

---

## 13. Tech stack — mandated versus chosen

Where Part A or B names a technology, we used it. Where they name only a
category, the product choice is ours.

| Technology (version) | Mandated by | Our choice and reason | Rejected alternatives |
| --- | --- | --- | --- |
| Python 3.12 | Part A: "Python" | 3.12 gives `StrEnum` and PEP 695 | 3.11; 3.13 (Temporal and asyncpg wheels lagged) |
| FastAPI 0.115 | Part A: "FastAPI" | Async, Pydantic validation, generated OpenAPI | Django + DRF, Flask, Litestar |
| Uvicorn + uvloop | Silent (server not named) | One process per container | Gunicorn with workers, Hypercorn |
| Pydantic v2 / pydantic-settings | Silent | One validation library; settings fail at start-up | marshmallow, attrs + cattrs |
| PostgreSQL 16 | Part A: "PostgreSQL" | One instance, one database and role per service | A schema per service; one container per service |
| SQLAlchemy 2.0 async + asyncpg | Silent | Typed models, `ON CONFLICT`, `FOR UPDATE` | SQLModel, Tortoise, raw asyncpg |
| Alembic | Silent | One history per service, mandatory `downgrade` | `create_all`, raw SQL files |
| **MongoDB 7** | Part A: "NoSQL DB" → **our pick** | Analytics: append-only documents of varying shape that must never contend with checkout | Another Postgres database (a token NoSQL table) |
| Redis 7.4 | Part A: "Redis — caching" | SSE tickets, pub/sub, limiter (only notification uses it today) | Redis as source of truth; Redis Streams |
| Celery 5.4 + RabbitMQ 3.13 | Part A: "Celery Workers with RabbitMQ" | Sweeps, media processing, notification dispatch, bulk AI. RabbitMQ is used only by Celery (broker, plus the `rpc://` result backend for `process_media`); no service publishes to or consumes from it directly, and events go on Kafka | A separate Beat container; RabbitMQ as an event bus |
| Kafka 7.7 (KRaft) + Schema Registry, Avro via fastavro | Part A: "Kafka + Schema Registry"; **Avro is our pick** | Compatibility checks in CI | JSON, JSON Schema |
| Temporal 1.25 | Part A: "Temporal"; Guidelines Week 2 | Publishing workflow and order saga | Celery chains, Kafka choreography |
| **MinIO** + Pillow | Silent (Part A says only "upload product media") | S3-compatible storage that runs locally | Other S3 servers; database blobs |
| httpx | Silent | One async client for services, the gateway and tests | aiohttp, requests |
| PyJWT (RS256), argon2-cffi | Silent | See §3 | Keycloak/Auth0 |
| **pgvector** (HNSW) | Part B: "Vector DB" → **our pick** | No extra container; filters in SQL | Qdrant (kept behind an interface), Weaviate, Milvus |
| LangGraph (without LangChain) | Part B: "LangGraph / LangChain" | Explicit branching graph | A linear chain; a tool-calling agent |
| **Anthropic** (Sonnet + Haiku) | Part B: "OpenAI / Groq / Anthropic" → **our pick** | The account we hold; a cheap classifier | OpenAI, Groq |
| **Voyage `voyage-3.5`** embeddings | Silent | Anthropic recommends it; 1024 dimensions | OpenAI embeddings; local models |
| Hand-written FastAPI gateway | Silent | Shares `common` (JWT, problem details) | Traefik, Kong, Envoy, nginx |
| Prometheus, Grafana, Jaeger, OpenTelemetry | Part A: Observability table | Eight dashboards designed | Elastic APM; Loki (deferred) — ⏳ **not deployed** |
| Docker + Compose with profiles | Part A: DevOps | `core`, `workflow`, `events`, `ai` | Kubernetes (kind/k3d), Tilt |
| pytest (85% branch gate), ruff, mypy, GitHub Actions | Silent; Guidelines "Code quality" | — | — |

---

## 14. Every decision taken where the requirements are silent

A checklist for review. For each item, the requirements give no answer; this
lists what we chose and the strongest alternative.

| Topic | Our decision | Strongest alternative |
| --- | --- | --- |
| Number of services | 8, using the ownership test | Modular monolith |
| Payment and shipment | Inside `order`, simulated | Separate services; a Stripe sandbox |
| Cart | None — direct line items | A Redis cart; draft orders |
| Who can be admin | Seed or promotion only | — |
| Merchant approval | `pending → approved` by an admin (endpoint ⏳) | Auto-approve |
| Gating publishing on approval | **Not enforced (open)** | Catalog checks the `merchant_status` claim |
| Token model | RS256, 15 min, rotating 7-day refresh | Server sessions; Keycloak |
| Publish failure target | Back to `draft`, with the reason recorded | Back to `inactive` |
| Promotions | Out of scope; a variant price override only | A promotion table with a date window |
| Initial stock | 0, created on publish | The merchant stocks before publishing |
| Reservation TTL | 15 min, swept every 60 s | No expiry; a shorter TTL |
| Stock vs payment order | Reserve → charge → commit | Charge first |
| **Payment declined** | **Release stock → order `failed` (`PAYMENT_DECLINED`)**, not `cancelled` | Mark it `cancelled`; let the customer retry payment on the same order *(not in RFC)* |
| Payment timeout | Always refund (keyed), then `failed` | Assume it failed |
| Shipment failure | Refund + reverse commit → `failed` | Leave the order charged and stuck |
| Notification failure | The order stays `confirmed` | Fail the order |
| Cancel window | Before stock commit only | Until shipment |
| Post-delivery refunds | Designed, no trigger | An admin refund workflow |
| Idempotency codes | Replay `202` / `422` / `409`; 24 h | A body-hash key |
| Order limits | 50 lines, quantity ≤ 999, one currency | Merge duplicate lines |
| Revenue | Gross, confirmed orders only | Net of refunds |
| Total Merchants | Registered merchants | Approved merchants |
| Stockout rate | Stockouts ÷ orders created | Variant-days out of stock |
| AI conversion | Same user, 24 h, correlational | A window per interaction |
| Locations | One location | `location_id` per warehouse |
| Notification channels | In-app live; email/SMS logged | A real SMTP provider |
| LLM cost | 50k tokens/user/day, 2M/day global | No budget |
| Embeddings | Voyage, 1024 dimensions | OpenAI, 1536 dimensions |

---

## 15. Designed but not built, and known deviations

Verified against the code on 2026-10-20. Each item is a known gap, not a hidden
one.

**Missing against an explicit requirement** (fix these first):

1. **Observability stack** — Prometheus, Grafana, Jaeger and the OTel collector are not in Compose, and `OTEL_SDK_DISABLED=true`. Part A Tech Stack and Guidelines Week 3 ("Monitoring dashboards ready", "Distributed tracing enabled") ask for it.
2. **Trace context across Kafka** — the `traceparent` field is always null (PLT-D41).
3. **Promotional rules** — Part A §1 asks for "basic … promotional rule support"; only a price override exists.
4. **Refund notices** — Part A names them; there is no refund notification type.
5. **Failed publish workflows** — not counted in the "Failed Events / Workflow Issues" metric.

**Designed but not built:**

- The per-email login limit (`login_email`, GW-8). The rest of gateway rate limiting, including 30 orders/min per user, was built after the 2026-10-20 review.
- Admin merchant-status endpoint, and the `identity.merchant.status_changed` event.
- Approval gating for publishing (open question IDN-Q1 / CAT-Q1).
- Catalog reacting to merchant suspension (products → `inactive`).
- Redis caches for browse (60 s) and availability (10 s); the Redis fast path for idempotency.
- HMAC-signed carrier webhook (only an admin simulation route exists).
- Append-only grant on `stock_movements`. (The daily ledger reconciliation was built after the 2026-10-20 review.)
- AI conversion and p95-latency analytics endpoints.
- `sweep_idempotency_keys` (expired keys are reclaimed lazily).

**Code that departs from the RFC:**

- The order write path takes a `FOR UPDATE` lock, while the RFC says optimistic concurrency only (ORD-D15).
- `notify` runs only on success, while the RFC says it also runs after a failure or cancel.
- `payment.failed` has a null `reason` on the timeout path.
- When the `awaiting_payment` write fails, reservations are not released; the sweep recovers them.
- `REFUND_FAILED` is never set, so a stuck refund leaves the order in `refunding` with no event.
- Merchant `suspended → pending` is allowed in code but forbidden in the RFC.
- [business-flows.md](business-flows.md) §7 says the sweep uses SKIP LOCKED; it does not.
- The README still describes Milestone 2; Milestone 3 components are running.
