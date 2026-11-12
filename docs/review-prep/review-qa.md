# SmartRetail — review questions with answers

Self-test sheet for mentor reviews. Each question has a short answer you should
be able to say out loud, plus where the proof lives. The questions-only version
is [review-questions.md](review-questions.md), with the same numbering.

- ⭐ = very likely to be asked, or asked in an earlier review.
- 🪤 = trick question, or a known gap. Say it's a gap honestly; don't defend it as a feature.
- Answers were checked against the code on branch `review/2026-10-20` (2026-11-05).

How to use it: read only the questions file, answer each one out loud, then
check yourself here. If you can't explain an answer in your own words, open the
file it points to.

---

## 1. Requirements and scope

### Q1.1 ⭐ What problem is SmartRetail solving? Give the Part A challenges in one breath

Slow manual product onboarding, stock that disagrees across channels, slow
checkout under load (flash sales), data spread across systems, notifications
that fail during spikes, and behavioural data nobody uses. Part A asks for a
scalable, event-driven, resilient commerce backend. Part B adds an AI shopping
assistant and merchant content generation.

### Q1.2 ⭐ Which of your decisions come from the requirements, and which did you make where the requirements say nothing?

The requirements describe capabilities, not mechanisms. Mandated: roles
(customer/merchant/admin), the publish steps, the order steps (record → reserve
→ pay → ship → notify → analytics), idempotent retries, the tech stack names, and
the metric names. Our own decisions ("silent"): 8 services, no cart, payment and
shipment inside order, the token model, the reservation TTL, reserve before
charge, payment decline → `failed`, the cancel window, every metric definition,
the LLM and vector DB choice. The full list is in `docs/business-logic-and-decisions.md` §14.

### Q1.3 What did you cut from scope on purpose?

Promotional rules (only `price_override` exists), multi-location stock (one
location per variant), cart, post-delivery returns/refund trigger, real
payment/carrier providers (fakes), real email/SMS sending (logged only), and
the observability stack (not deployed yet).

### Q1.4 🪤 Which explicit requirements are not met yet?

The observability stack (Prometheus/Grafana/Jaeger/OTel not in Compose;
`traceparent` always null), promotional rules, refund notices (no notification
type of their own), failed publishes not counted in the "Failed events" metric,
and approval gating (a `pending` merchant can still publish). See
`business-logic-and-decisions.md` §15.

### Q1.5 Why is there no cart?

Neither Part A nor Part B mentions one. An order is placed with a list of line
items. A cart would be a Redis-backed draft or a draft order; it's an easy
addition and doesn't change the saga.

---

## 2. Architecture and service boundaries

### Q2.1 ⭐ List the services and what each one owns

- **gateway**: no state. Routing, early JWT check, CORS, body limits, rate limits, merged OpenAPI.
- **identity**: users, credentials, roles, merchant profiles, refresh sessions. The only service that mints tokens.
- **catalog**: products, variants, categories, media, audit log, publishing workflow.
- **order**: orders, lines, payment and shipment sub-states, idempotency keys, the order saga.
- **inventory**: stock rows, reservations, the movement ledger.
- **notification**: notifications, delivery attempts, SSE stream.
- **analytics**: rollups in MongoDB.
- **assistant**: conversations, chunks and embeddings (pgvector), generations, prompts.

### Q2.2 ⭐ How did you decide what becomes a service?

Split by bounded context and data ownership, using a four-part test. A
candidate becomes a service only if it has all four:
1. owned state that only it writes;
2. an invariant worth protecting in a transaction;
3. an independent reason to change;
4. a distinct operational profile (load, store, consistency, failure containment).

18 candidates were tested (RFC 0000 §3.1.1.2). Payment, shipment, cart, search,
media, pricing, review, recommendation, merchant and audit were rejected.

### Q2.3 ⭐ Why are payment and shipment not separate services?

No compliance boundary (no card data, provider is simulated), and no state a
real provider wouldn't own. A separate service would need a distributed
transaction to record one fact that's really a sub-state of the order. The
*interface* (`PaymentProvider`, `ShippingProvider`) is kept separate, so pulling
it out later is an extraction, not a rewrite.

### Q2.4 Why is the gateway a service if it fails the ownership test?

It's a deliberate exception. Edge concerns (routing, early token rejection,
correlation id, CORS, rate limit, aggregated docs) need exactly one home. The
rule that bounds it: no business logic and no database in the gateway.

### Q2.5 Why not a modular monolith?

It's the strongest alternative: simpler and faster. But Kafka would become
ceremony, and the assignment's goal is distributed-systems behaviour. The
boundaries follow data ownership, so they'd survive as module boundaries if we
merged later.

### Q2.6 ⭐ What are the boundary rules R1–R8?

- R1: each service touches only its own DB. Separate Postgres roles enforce this, so a violation fails at connect time.
- R2: no FK crosses services.
- R3: one writer per fact.
- R4: cross-service reads go through an API or an event, never a join.
- R5: a sync call only where the caller can't proceed without the answer. On the write path there's one: order → catalog.
- R6: snapshot or project data at the boundary.
- R7: don't need another service up to serve data you already have.
- R8: `common` has no business rules.

### Q2.7 ⭐ How many synchronous cross-service calls are on the write path, and why that one?

Exactly one: `order → catalog` at order creation, for the price snapshot. Price
and publish status at the moment of purchase must be authoritative. A local
projection would be stale on exactly the fact that matters. (The saga's calls
to inventory are also HTTP, but they run inside async Temporal activities,
not inside the user's request.)

### Q2.8 One Postgres instance for all services: isn't that a shared database?

One *instance*, but one *database and one role* per service. Role grants make a
cross-database query fail at connection time. One instance is a laptop
constraint (8 Postgres containers next to Kafka and Temporal won't fit).
Splitting the busiest DBs onto their own servers is the scaling path (B5).

### Q2.9 What goes in `common` and what never does?

Mechanism only: JWT verification, problem-details errors, pagination, DB
session helpers, outbox writer, relay, consumer base, dedupe, Temporal helpers,
Celery factory, Redis limiter, idempotency hashing. Never a business rule (R8,
checked by a vocabulary grep test).

### Q2.10 Explain the code layout inside a service

Version first: `app/v1/<entity>/` holds everything for that entity (models,
repo, routes, schemas, state_machine, workflows, consumers, tasks, clients,
providers). `v2/` would hold only the files it changes and import the rest from
v1. Models are never versioned (a table has one shape). `routes.py` does HTTP
only. `repo.py` is the only place business rules live, so routes, activities,
consumers and tasks all call the same rule.

### Q2.11 Why do activities, consumers and tasks all call the repo instead of having their own logic?

So each rule has one implementation and one set of tests. An activity is a
thin adapter: open a session, call one repo method, turn a 4xx `DomainError`
into a non-retryable failure.

---

## 3. Request flow (edge to database)

### Q3.1 ⭐ Walk me through what happens when a request hits `POST /api/v1/orders`

1. **Gateway**: per-IP rate limit (100/min default) → JWT verified locally (RS256 via JWKS) → role checked → per-user `order_create` limit (30/min). Strips inbound `X-User-*`/`X-Internal-*` headers, sets `X-Request-ID`, forwards. No retries.
2. **Order API**: verifies the JWT again (services never trust the gateway). Requires an `Idempotency-Key` header (400 if missing).
3. Claims the key: `INSERT … ON CONFLICT (user_id, key) DO NOTHING`.
4. Validates shape (1–50 lines, qty 1–999, one line per variant) and calls catalog for each product with the user's token, 2 s timeout. Builds the price snapshot: `unit_price = price_override ?? base_price`.
5. One transaction: `orders` (pending) + `order_items` + `order_status_history` + outbox `order.created` + idempotency key marked completed with the stored reply.
6. Starts the Temporal workflow `order-{id}` (REJECT_DUPLICATE, 5 s timeout). If that fails, the order stays pending and Celery resumes it later.
7. Returns `202 Accepted` `{order_id, order_number, status: pending}` with a link. The client polls `GET /orders/{id}` or waits for a notification.

### Q3.2 What does the gateway do and not do?

Does: route table, early JWT rejection, role check, CORS allow-list, body
limits (1 MiB, 10 MiB uploads), rate limits, correlation id, strip spoofable
headers, 404 for `/internal/**`, merged OpenAPI. Doesn't: business logic,
DB access, retries, final authorisation (the owning service decides).

### Q3.3 ⭐ Why does every service re-verify the JWT if the gateway already did?

Services can be reached by internal callers, not only through the gateway.
The gateway is early rejection, not the security boundary. Verification is
local (public key), so it costs no network call.

### Q3.4 Why doesn't the gateway retry upstream calls?

A proxy can't prove a request is safe to repeat. The client owns the retry,
with an `Idempotency-Key` where it matters.

### Q3.5 How do you trace one request across services?

The gateway issues `X-Request-ID`. It's carried in context, logged on every
line, copied to outbox rows as `correlation_id` and to Kafka headers, passed
into the workflow input, and written into `stock_movements.correlation_id` and
`order_status_history`. 🪤 Real distributed tracing (OTel `traceparent`) is not
wired: the envelope field is always null.

### Q3.6 What error format do APIs return?

RFC 9457 problem details everywhere (`type`, `title`, `status`, `detail`, extras).
Repos raise `DomainError`s; shared handlers in `common` map them.

### Q3.7 What happens on `GET /orders/{id}` while the saga is still running?

It reads the order row and its status history from order_db. Status moves
`pending → awaiting_payment → confirmed/failed/cancelled`. `GET
/orders/{id}/workflow` shows live progress: Temporal `describe` plus the
workflow's `get_state` query (stage, failure reason, cancel requested).

---

## 4. Identity and auth

### Q4.1 ⭐ Describe the token model

Access token: RS256 JWT, 15 min, with claims for role and `merchant_status`.
Refresh token: opaque, 7 days, stored hashed, rotated on every use. Re-use of an
old refresh token revokes all of that user's sessions (theft detection).
Services verify access tokens locally with the public key (JWKS).

### Q4.2 Why RS256 and not HS256?

With HS256 every verifier holds the secret that can also *sign*. One leaked
service could forge tokens. With RS256 only identity holds the private key.

### Q4.3 How do workers call internal APIs without a user token?

Client-credentials service tokens: 5 min, scoped (for example
`inventory:internal`), cached and refreshed at 80 % of their lifetime, and
dropped on a 401. `/internal/v1/*` routes accept only service tokens. User
routes reject them, so a stolen customer token can't reserve stock.

### Q4.4 Can someone register as admin?

No. Registration accepts only `customer` or `merchant` (else 422). Admins come
from the seed or from promotion by another admin. The last active admin can't
be demoted.

### Q4.5 What's the merchant lifecycle?

`pending → approved | suspended`, `approved ⇄ suspended`. No `rejected` state:
rejection is `suspended`. Repeating the current status is a no-op with no event.
🪤 The code also allows `suspended → pending` on a role change, which the RFC forbids.

### Q4.6 🪤 Can a pending merchant publish products?

Yes, today. Approval gating is an open question (IDN-Q1/CAT-Q1). The proposal is
that catalog checks the `merchant_status` claim and returns 403. The admin
`POST /merchants/{id}/status` endpoint and the `merchant.status_changed` event
are also not built.

### Q4.7 How long can a suspended merchant keep acting?

At most 15 minutes: one access-token lifetime. Suspension revokes refresh
sessions, refresh returns 401, login returns 403.

### Q4.8 How are passwords stored, and how do you stop email enumeration?

Argon2id (time cost 3, 64 MiB, parallelism 4); passwords 12–128 chars. A dummy
hash runs for unknown emails, so timing and the 401 response are the same
either way.

### Q4.9 Who decides authorisation, for example "can this merchant edit this product"?

The service that owns the row. Catalog compares `product.merchant_id` with the
token's subject. The gateway only checks the role.

---

## 5. Catalog and product publishing

### Q5.1 ⭐ What's the product state machine?

`draft`, `publishing`, `published`, `inactive`, `archived`.
- create → `draft`; `draft ⇄ inactive`
- `draft`/`inactive` → `publishing` (on publish, 202)
- `publishing` → `published` (only `mark_published`)
- `publishing` → `draft` (compensation)
- `published` → `draft` (unpublish); `published` → `inactive` (merchant suspended, ⏳)
- `draft`/`inactive`/`published` → `archived` (terminal)

Anything else is `409 illegal-transition`.

### Q5.2 ⭐ Walk through publishing a product

1. `POST /products/{id}/publish`: commit `publishing` first, then start `ProductPublishingWorkflow` with id `product-publish-{id}-v{version}` and REJECT_DUPLICATE. Return 202. If the start fails, the product goes back to `draft` and the response is 503.
2. `validate_product`: completeness (name, description, category, ≥1 active variant, ≥1 media item). Failure is non-retryable.
3. `process_media` for each image: a Celery job makes 150/600/1200 px WebP files with EXIF stripped, stored in MinIO under deterministic keys. The activity heartbeats.
4. `build_search_document`.
5. `sync_downstream`: `POST /internal/v1/inventory/stock-items` creates zero-quantity stock rows (idempotent).
6. `mark_published`: status + audit + outbox `catalog.product.published` in one transaction.

Any failure → `record_publish_failure` (retries up to 24 h) → back to `draft`
with the failed step and reason, readable at `GET /products/{id}/publish-status`.

### Q5.3 Why commit `publishing` before starting the workflow?

So a workflow never runs against a product that isn't in `publishing`.
Starting first and committing after can leave an orphan workflow if the commit
fails.

### Q5.4 Why does a failed publish not need step-by-step undo?

Only the last step (`mark_published`) makes anything visible, and it's atomic.
Media derivatives and stock rows at 0 are harmless leftovers and are
idempotent on the next attempt.

### Q5.5 Why is the workflow id versioned (`-v{version}`)?

`REJECT_DUPLICATE` stops a double publish of the same version. A later
re-publish after edits has a new version, so it gets a new id. `PublishSuperseded`
is non-retryable for a stale run.

### Q5.6 Can a merchant edit a product while it's publishing?

No, `409`. Otherwise the workflow could publish something it never validated.
Edits to a `published` product apply at once and emit `catalog.product.updated`.
An edit that would break completeness is refused.

### Q5.7 How do you stop lost updates on product edits?

Optimistic concurrency: every write carries `version`, and a mismatch returns `409`.

### Q5.8 Where is stock stored for a product?

Not in catalog. Inventory is the only writer of stock. Catalog links to
inventory's availability. A second writer of stock is exactly the bug Part A
describes.

### Q5.9 Why is media processing on Celery and not inside the activity?

CPU work stays off the Temporal worker. The activity enqueues the Celery task
on `catalog.media` and waits for the result over RabbitMQ (`rpc://`), so
Temporal still owns retry and compensation. 3 failures → media `failed` → the
publish fails; the next publish retries it.

### Q5.10 🪤 Any validation gaps?

`base_price > 0` isn't checked, so a price of 0 passes. The browse Redis cache
(60 s) is designed but not built.

---

## 6. Inventory: stock, reservations and the ledger (deep)

### Q6.1 ⭐ Describe the inventory tables

- **`stock_items`**: one row per variant. `variant_id` unique; `product_id`, `merchant_id`, `sku` are snapshots (no FKs). `on_hand`, `reserved`, `available` (generated: `on_hand - reserved`, stored), `low_stock_threshold` (default 5), `version`. `CHECK (on_hand >= reserved AND reserved >= 0)`.
- **`reservations`**: `order_id` (no FK, cross-service), `variant_id` (a real FK to `stock_items.variant_id`), `quantity > 0`, `status` (`held`/`committed`/`released`/`expired`), `expires_at`. `UNIQUE (order_id, variant_id)`. Partial index on `expires_at WHERE status='held'`, exactly the sweeper's query.
- **`stock_movements`**: append-only ledger. bigserial id, `variant_id`, `delta_on_hand`, `delta_reserved`, `reason`, `reference_type/id`, `actor`, `correlation_id`, `created_at`.
- Plus the shared `outbox` and `processed_events`.

### Q6.2 ⭐⭐ How does stock decrease when an order is placed? Walk the numbers

Stock doesn't drop at order creation. It moves in two steps inside the saga.
Example: 10 on hand, an order for 2.

| Step | on_hand | reserved | available | reservation |
| --- | --- | --- | --- | --- |
| before | 10 | 0 | 10 | — |
| **reserve** (start of saga) | 10 | 2 | 8 | `held`, expires in 15 min |
| **commit** (after payment succeeds) | 8 | 0 | 8 | `committed` |

Reserve (`ReservationRepo.reserve`):
1. Merge lines per variant and sort by `variant_id`.
2. Fast reject: an unlocked read. If clearly short → refuse (it can refuse but never grant). Skipped if the order already has reservations (so a retry replays).
3. In one transaction with `lock_timeout 2 s` and `statement_timeout 5 s`: `SELECT … FOR UPDATE` the stock rows in `variant_id` order.
4. Replay check *after* the lock: same order with the same lines and all still `held` → return the existing reservations (HTTP 200). Different lines → `409` conflict.
5. Unknown variant → 422. Any shortage → refuse the whole order (all-or-nothing).
6. For each line: `reserved += q`, `version += 1`, insert a `held` reservation, ledger row `reserve (0, +q)`, outbox `inventory.reserved`, and maybe `inventory.stockout` (`depleted` at 0, `low_stock` when crossing the threshold).
7. Commit. HTTP 201 with the reservation ids.

Commit (`ReservationRepo.commit`): lock the stock row, then the reservation.
Already `committed` → no-op. Not `held` → `409 reservation-not-held`. Else
`held → committed`, `reserved -= q`, `on_hand -= q`, ledger `commit (−q, −q)`.
No Kafka event for a commit.

### Q6.3 ⭐⭐ How is stock given back? Cover every reverse path

| Path | When | Reservation | Counters | Ledger | Event |
| --- | --- | --- | --- | --- | --- |
| **release** (saga compensation) | decline, ambiguous charge, commit failure, cancel before commit | `held → released` | `reserved -= q` | `release (0, −q)` | `inventory.released` reason `compensation` |
| **release_for_order** (Kafka consumer) | `order.cancelled` arrives | only rows still `held` → `released` | `reserved -= q` | `release`, actor `consumer:order.cancelled` | reason `order_cancelled` |
| **expire** (Celery sweep) | `expires_at` passed and still `held` | `held → expired` | `reserved -= q` | `expire (0, −q)`, actor `task:sweep_expired_reservations` | reason `expired` |
| **reverse** (after commit) | shipment failed, or operator termination after the pivot | stays `committed` | `on_hand += q` | `compensation (+q, 0)`, ref = reservation id | none |

Release of an already `released`/`expired` reservation is success. Release of
a `committed` one is refused (`ReservationCommitted`): sold stock comes back
only through reverse.

### Q6.4 ⭐ How do you stop two customers buying the last unit?

Pessimistic row lock: every reserve does `SELECT … FOR UPDATE` on the stock row,
so reserves of the same variant run one at a time. The second sees
`available = 0` and gets `409 insufficient-stock`. The DB `CHECK on_hand >=
reserved` is the backstop, even for a buggy writer. Proven by
`test_last_unit_goes_to_exactly_one_of_many_orders` and e2e "50 orders for 10
units confirm exactly 10".

### Q6.5 Why pessimistic locking and not optimistic (version) or SERIALIZABLE?

On a hot row, optimistic locking turns contention into retry storms. SERIALIZABLE
aborts transactions on the hot path. A single guarded `UPDATE … WHERE available
>= q` can't tell "unknown variant" from "out of stock" and gives poor errors.
`FOR UPDATE` queues writers: "overselling is worse than queueing".

### Q6.6 ⭐ How do you prevent deadlocks when an order has several variants?

A fixed lock order: stock rows first, ascending `variant_id`, in one statement,
then reservation rows. Every path (reserve, commit, release, reverse, expire,
adjust) uses the same order, so a wait cycle can't form. As a second line,
deadlock (40P01), lock timeout (55P03) and serialisation failure (40001) are
retried 3 times with jittered backoff (25 ms base), then `503
inventory-contended` (retryable by Temporal).

### Q6.7 What's the fast-reject read for?

During a flash sale, sold-out variants shouldn't queue on the row lock. An
unlocked read can *refuse* quickly but never *grant*: the grant is re-checked
under the lock. It's skipped when the order already has reservations, so a
retried reserve replays instead of being refused against its own holding.

### Q6.8 ⭐ Isn't `stock_items.reserved` a duplicate source of truth next to the reservation rows? (asked 2026-10-20)

Reservation rows are the truth for *who holds what*. `reserved` is a
denormalised aggregate (invariant I5: `reserved = Σ held quantities`). It's kept
because:
1. `available` can be a stored generated column (O(1), indexable for low-stock listings);
2. the DB `CHECK` can enforce "never oversell". A CHECK can't express a sum over another table.

Dropping it doesn't remove the row lock: reserves must still serialise. Drift
risk is closed by: one write path in one transaction under the same lock; tests
that check I5 after every race; and the daily `reconcile_inventory_ledger` job
(03:30), which *reports* drift and never fixes it. A human fixes it with a
manual adjustment, so the ledger stays the record. Alternatives considered: a
computed SUM, a DB trigger, event sourcing.

### Q6.9 What does the ledger give you?

Every change is a signed delta in the same transaction. Invariant I6: `on_hand
= Σ delta_on_hand` and `reserved = Σ delta_reserved`. It makes "stock
disagrees" detectable and shows who changed stock and why (actor, reason,
correlation id). 🪤 The append-only DB grant (revoke UPDATE/DELETE) is designed
but not applied.

### Q6.10 Why is the reservation idempotent on `(order_id, variant_id)` and not a separate idempotency key?

The natural key is already in every retry: the same order reserves the same
variants. A unique constraint plus a replay check after the lock covers a lost
reply (F32: inventory reserved but the response was lost; the retry gets the
same reservations).

### Q6.11 ⭐ What's the reservation TTL, and why 15 minutes?

900 s. Long enough for slow payment retries: the forward path's worst case is
~10 min. Short enough that stranded stock comes back. No expiry would strand
stock forever. Shorter would fail slow payments.

### Q6.12 ⭐ How does the expiry sweep work, and is it safe on two workers?

Celery Beat every 60 s → `sweep_expired_reservations` on `inventory.maintenance`.
`expire_due()` reads candidates *without* a lock (held, `expires_at < now`,
oldest first, batch 500). Then for each one, in its own short transaction:
lock the stock row, then the reservation; re-check still `held` and still past
due; `held → expired`; `reserved -= q`; ledger `expire`; outbox
`inventory.released`. Two sweeps (or the sweep vs a commit) block on the same
locks. The loser sees a non-held status and skips. 🪤 It is NOT `SKIP LOCKED`
(business-flows.md §7 wrongly says it is).

### Q6.13 ⭐ The reservation is past `expires_at` but the sweep hasn't run. Can it still commit?

Yes. Commit follows status, not the clock (INV-D13). It still holds its stock,
so committing can't oversell, and refusing would cause a needless refund.

### Q6.14 What if a merchant reduces stock while customers are reserving?

`POST /adjust` (relative, preferred) or `PUT` (absolute, needs `If-Match:
version`) takes the same row lock. If `on_hand` would fall below `reserved` →
`409 stock-below-reserved`. The CHECK holds in every interleaving
(`test_stock_reduction_racing_reserves_never_oversells`).

### Q6.15 When are stock rows created, and with what quantity?

At publish, with `on_hand = 0`, by two idempotent paths: the workflow's
`sync_downstream` (HTTP) and the `catalog.product.published` consumer (`INSERT …
ON CONFLICT DO NOTHING`). A republish never resets quantity. Zero is the only
honest starting value. The merchant then adjusts stock.

### Q6.16 When is `inventory.stockout` emitted?

Edge-triggered: `depleted` (available hits 0), `low_stock` (crosses below the
threshold, 5), and `reservation_refused`. The refused case is written in its
*own* transaction after the rollback, because an outbox row inside a rolled-back
transaction would vanish.

### Q6.17 How do two reverses of the same commit not add stock twice?

Under the lock, reverse checks for an existing `compensation` ledger row that
references that reservation id. If it's there → no-op. The reservation stays
`committed`, so the ledger row is the marker (`test_reverse_twice_at_once_returns_stock_once`).

### Q6.18 Why no `reversed` reservation state?

Not needed: the ledger row records the reversal, and `committed` stays
terminal (I8). Fewer states, fewer transitions to guard.

### Q6.19 How is availability exposed to customers?

A public stock read returns `available` (`on_hand − reserved`). Catalog and the
assistant fetch it live. 🪤 The designed 10 s Redis cache isn't built.

---

## 7. Order and checkout

### Q7.1 ⭐ How do you prevent duplicate orders?

Three layers:
1. `Idempotency-Key` claimed with a unique insert on PK `(user_id, key)`.
2. Workflow id `order-{order_id}` with `REJECT_DUPLICATE`.
3. Provider keys `order-{id}:charge|:refund|:shipment`.

Test: 10 parallel requests with one key → one order.

### Q7.2 ⭐ Walk through the idempotency-key rules

- New key → `INSERT … ON CONFLICT DO NOTHING RETURNING` wins → this request owns it (`in_progress`).
- Same key, same body hash, completed → replay the stored status and body (`Idempotency-Replayed: true`).
- Same key, still in progress → `409` with `Retry-After: 1`.
- Same key, different body → `422 idempotency-key-reused`; the existing order is untouched.
- In progress for over 30 s (the holder died) → reclaimed by a conditional `UPDATE` only one contender can win.
- Expired (24 h TTL) → treated as absent and reclaimable.
- Business rejection (bad product, mixed currency) → stored and replayed. Dependency failure (catalog 503, DB error) → claim *deleted*, so a retry starts clean.

### Q7.3 Why a unique insert and not "SELECT, then INSERT if missing"?

Read-then-write has a race window as wide as the burst. Two requests both read
"missing" and both create orders. The DB arbitrates with the PK.

### Q7.4 Why is the key scoped per user?

So one caller can't probe or collide with another's keys.

### Q7.5 ⭐ Why does `POST /orders` return 202 and not 201?

Holding the connection through payment is the "checkout delays under load"
problem. The order is durably recorded (`pending`), and the rest runs on
Temporal. The client polls or gets notified.

### Q7.6 ⭐ What is the price snapshot, and why?

`order_items` stores product name, SKU, variant options, `unit_price`
(`price_override ?? base_price`), quantity and `line_total` at checkout,
fetched from catalog. The client can't send a price. A later price change
doesn't rewrite history, and the saga never re-reads the price.

### Q7.7 How is the total computed? Any money rules?

`numeric(12,2)` and `Decimal`, never float. `total = Σ line_total +
shipping_fee`, `ROUND_HALF_EVEN`. One currency per order (mixed → 422, never
converted).

### Q7.8 Why isn't stock checked at order creation?

The reservation *is* the check. Checking twice races: stock could change in
between. Order creation stays cheap.

### Q7.9 ⭐ Order state machine: states and legal transitions?

Nine states: `pending`, `awaiting_payment`, `confirmed`, `shipped`,
`delivered`, `cancelled`, `failed`, `refunding`, `refunded`. 14 of 81 pairs are
legal:
- `pending` → `awaiting_payment`, `failed`, `cancelled`
- `awaiting_payment` → `confirmed`, `failed`, `cancelled`, `refunding`
- `confirmed` → `shipped`, `refunding`
- `shipped` → `delivered`, `refunding`
- `delivered` → `refunding`
- `refunding` → `refunded`, `failed`
- `cancelled`, `failed`, `refunded` are terminal

Guard: `confirmed` needs `payment_status = succeeded` and `shipment_status = created`.

### Q7.10 Payment and shipment sub-states?

Payment: `none → pending → succeeded | failed | refunded`, and `succeeded →
refunded`. Shipment: `none → created → shipped → (in_transit) → delivered`. All
checked by `transition()` in `state_machine.py`, the only code that assigns them.

### Q7.11 Why store status as `text` and not a Postgres enum?

A native enum can't drop a value (no inverse of `ADD VALUE`), and it checks the
label, never the *transition*. It would accept all 81 pairs. The code table
allows 14. Unknown DB values read as a fallback state with no outgoing edges
("freeze loudly").

### Q7.12 How does `OrderRepo.advance` work?

It's the single writer. Lock the order row, optional version check, validate
each requested transition, apply the `confirmed` guard, update, `version += 1`,
history row, outbox events, commit. A change already in effect is a no-op, so a
retried activity never fails on its own earlier success. 🪤 It takes `FOR
UPDATE`, while the RFC says optimistic only (a known deviation).

### Q7.13 Which events does order emit, and when?

`order.created` (at checkout), `payment.succeeded`/`payment.failed` (payment
sub-state change), `order.confirmed`, `order.failed`, `order.cancelled`,
`shipment.status_changed` (carrier statuses only). All go through the outbox,
keyed by `order_id`. No separate refund topic: the refund shows as
`payment_status: refunded` inside `order.failed`/`order.cancelled`.

### Q7.14 ⭐ What if Temporal is down when the order is placed?

The order is still accepted (202, `pending`, no `workflow_run_id`). Celery Beat
runs `resume_unstarted_orders` every 60 s: it finds pending orders older than
30 s with no run id (batch 100) and starts them. `WorkflowAlreadyStartedError`
counts as success, and the run id is read back. This closes the commit-then-start gap.

### Q7.15 Why not start the workflow before committing the order?

The workflow could run for an order that never got committed. Commit first,
then start, with a recovery job: it's the outbox idea again, with the order row
playing the outbox row.

### Q7.16 ⭐ How does cancellation work?

`POST /orders/{id}/cancel` (owner or admin). The API rejects terminal orders,
queries the workflow stage, and returns `409 past-cancellable-point` unless the
stage is `starting`, `reserving` or `charging`. Then it sends the
`request_cancellation` signal and answers `202 requested`. The workflow checks
the flag *between* steps: refund if charged → release stock → status
`cancelled` → `order.cancelled`. After commit, cancel isn't an undo, it's a
refund flow.

### Q7.17 🪤 Is there a race in cancellation?

Yes, a narrow one. The API sees a cancellable stage and answers 202, but the
workflow passes the commit before the signal lands, so the signal is ignored and
the order confirms. That's why the API says `requested`, not `cancelled`. The
fix is a Temporal *Update* (synchronous accept/reject), open question ORD-Q4.
The order is never half-cancelled either way.

### Q7.18 What's the difference between `failed` and `cancelled`?

`cancelled` = a customer or admin asked. `failed` = the system couldn't complete
it (stock, payment, commit, shipment). A declined card is `failed /
PAYMENT_DECLINED`. A decline wins even if a cancel arrived at the same time.

### Q7.19 Can merchants see orders?

`list_for_merchant` returns orders that contain the merchant's products, with
**only that merchant's lines**. Customers see their own orders. Admins see all.

---

## 8. Temporal and the saga

### Q8.1 ⭐ Why Temporal for checkout instead of Kafka choreography or Celery chains?

- Choreography: the flow exists only as the union of every subscription, so nobody can answer "where is order X stuck?", and compensation is spread over services.
- Celery chain: no durable state. If the worker dies between charge and commit, nothing knows the order is half done, and nothing refunds.
- Temporal: the workflow is one readable function, its state is durable history, it resumes on another worker, retries and timeouts sit per step, and its state can be queried.

### Q8.2 ⭐ Explain the saga pattern as you use it

A sequence of local transactions with a compensating transaction for each
visible step, run in reverse on failure. The result is eventual consistency
with a guaranteed end state: `confirmed`, or `failed`/`cancelled` with
everything undone.
- Compensatable steps: reserve (undo: release) and charge (undo: refund).
- **Pivot**: commit stock. After it, only forward.
- Retriable steps after the pivot: ship, confirm, notify.
- Compensations are *semantic*: a refund is a new provider operation, and a reversal is a new ledger row. Nothing is deleted.

### Q8.3 ⭐ What are the saga steps in order, and why reserve before charge?

reserve → `awaiting_payment` → charge → commit → ship → `confirmed` → notify.
Reserve first because the cheap failure (out of stock) comes before the
expensive one (a charge that would need a refund). Commit only after payment,
so stock isn't sold for an unpaid order.

### Q8.4 ⭐ Failure matrix: what happens at each step?

| Fails at | Compensation | Final |
| --- | --- | --- |
| reserve, no stock | none | `failed` INSUFFICIENT_STOCK |
| reserve, inventory down after 60 s | none | `failed` INVENTORY_UNAVAILABLE |
| charge declined | release | `failed` PAYMENT_DECLINED, payment `failed` |
| charge timeout ×3 | refund (keyed; may be "nothing captured") → release | `failed` PAYMENT_TIMEOUT |
| commit (reservation not held / error) | `refunding` → refund → release | `failed` RESERVATION_EXPIRED / COMMIT_FAILED |
| ship (address rejected / carrier down) | `refunding` → refund → **reverse commit** | `failed` FULFILMENT_FAILED |
| notify | none | stays `confirmed`; `notified_at` null shows it |

### Q8.5 ⭐ Give the retry policies and the reasoning

| Step | per attempt | total | attempts | non-retryable |
| --- | --- | --- | --- | --- |
| reserve | 10 s | 60 s | 5 | insufficient-stock, unknown-variant |
| status writes / notify | 5 s | 300 s | unlimited | IllegalTransition |
| charge | 15 s | 60 s | 3 | PaymentDeclined |
| commit | 10 s | 300 s | unlimited | reservation-not-held |
| ship | 15 s | 120 s | 5 | AddressRejected |
| compensations | 15 s | 3600 s | unlimited | — |

Rules: business refusals are never retried; the forward path (~10 min) fits
inside the 15 min TTL; "a compensation that gives up is worse than one that is late".

### Q8.6 How does an HTTP error become retryable or not inside an activity?

`raise_for_activity` (common/temporal.py): 4xx → non-retryable
`ApplicationError` with the problem slug as its type (for example
`insufficient-stock`), except 401 (drop the cached token and retry), 408 and 429
(retry). 5xx or a network error → retryable `DependencyUnavailable`.
`as_activity_errors` does the same for in-process `DomainError`s.

### Q8.7 ⭐ Ambiguous payment timeout: how do you avoid charging for a failed order?

Retrying the charge is safe (same key). If all 3 attempts time out, the saga
*always* refunds, keyed to the charge. The provider answers `refunded` or
`nothing_captured`, and both are safe. `nothing_captured` records payment
`failed`. Then release, and `failed / PAYMENT_TIMEOUT`.

### Q8.8 ⭐ The worker crashes after charging, before committing. What happens?

Temporal has the charge's completion in the history. Another worker (or the
restarted one) replays the workflow code. Completed activities return their
recorded results without running again, and execution continues at commit. If
the charge activity itself was mid-flight, the retry hits `payment_attempts`
with the same key, so there's no second charge (ORD-D36).

### Q8.9 ⭐ Two worker replicas run the same charge. Double charge?

No. `payment_attempts` (PK = idempotency key) is shared in Postgres, and the
first written outcome wins. Before this fix (2026-10-20), dedupe was in process
memory, so scaling the worker or restarting it could double-charge.

### Q8.10 ⭐ What does "deterministic workflow" mean, and how do you respect it?

On recovery the workflow code is *replayed* against its history, so it must
make the same decisions every time. Rules: no I/O in the workflow (only
activities), time from `workflow.now()`, no random or uuid4, no threads, sandbox
on, imports through `workflow.unsafe.imports_passed_through()`. All side
effects go through activities.

### Q8.11 Workflow vs activity: what goes where?

Workflow: sequence, branching and compensation only, with no business rules
(B1). Activity: one side effect, thin, idempotent, calling a repo or a client.

### Q8.12 Signal vs query vs update?

- **Signal**: async, fire-and-forget input recorded in history (`request_cancellation`).
- **Query**: read-only view of state with no history entry (`get_state` → stage, failure, cancel requested).
- **Update**: synchronous, can validate and return a result. Not used yet. It's the fix for the cancel race (ORD-Q4).

### Q8.13 What's a task queue here, and where's the "queue" for orders?

`order-lifecycle` (orders) and `catalog-publishing`. Each accepted order is a
workflow execution whose tasks wait on the queue. `order-worker` replicas poll
at most 50 activities and 50 workflow tasks at a time (ORD-D37). A burst of
1,000 orders = 1,000 durable workflows drained at the workers' pace.

### Q8.14 Explain `start_to_close`, `schedule_to_close` and heartbeat timeouts

`start_to_close`: one attempt's limit. `schedule_to_close`: the limit across all
attempts, including backoff. Heartbeat: a long activity must report progress
or it's treated as dead (used for `process_media`, 30 s). The workflow
execution timeout for orders is 24 h; for publishing it's 1 h.

### Q8.15 What's `REJECT_DUPLICATE` for?

A workflow id can't be reused once a run has existed. A second start of
`order-{id}` fails with `WorkflowAlreadyStartedError`. It's the second line of
defence behind the idempotency key, and what makes `resume_unstarted_orders`
safe.

### Q8.16 What happens on an operator cancel (Temporal cancel) of an order workflow?

Before the pivot it behaves like a customer cancel (refund if mid-charge,
release, `cancelled`). After it: `refunding` → refund → reverse commit (or
release) → `failed / OPERATOR_TERMINATED`.

### Q8.17 🪤 What if you change the workflow code while orders are in flight?

Not handled today: no `workflow.patched()` and no worker versioning. Replaying
an old history against changed code can raise a non-determinism error. Fix
options: `workflow.patched()` branches, worker build-id versioning, or a new
workflow name for the new logic while old ones drain.

### Q8.18 🪤 Known gaps in the saga?

- If the `awaiting_payment` status write fails after the reserve succeeded, the error lands in the RESERVING branch → `failed` without releasing. The sweep returns the stock in ≤ 15 min.
- A refund that never succeeds leaves the order in `refunding`. `REFUND_FAILED` is defined but never set, and no event is written.
- `payment.failed` on the timeout path has a null reason.
- `notify` runs only on success.
- If compensation fails for the whole hour, the workflow execution fails and no `order.failed`/`order.cancelled` is written. The stock still comes back via the sweep, but the order needs an operator.

### Q8.19 Why does the notify step exist if notification consumes Kafka events?

It only stamps `notified_at` (first write wins). The real notification comes
from the `order.confirmed` event. A null `notified_at` on a confirmed order makes
a failed step visible.

---

## 9. Inter-service communication

### Q9.1 ⭐ Table every way services talk to each other

| Caller → callee | How | Auth | Timeout / retry | Idempotency |
| --- | --- | --- | --- | --- |
| client → gateway → service | HTTP proxy | user JWT | connect 2 s, read 10 s; no retry | client `Idempotency-Key` |
| order API → catalog | HTTP in the request | user's token | 2 s, one retry on connect error | read-only |
| order-worker → inventory | HTTP from an activity | service token `inventory:internal` | 10 s; Temporal retry policy | `(order_id, variant_id)`; no-op repeats |
| order-worker → payment/shipping | in-process fake provider | — | Temporal | provider keys + `payment_attempts` |
| catalog-worker → inventory | HTTP from an activity | service token | Temporal | `ON CONFLICT DO NOTHING` |
| any service → another's state change | Kafka event | — | consumer retry → DLQ | `processed_events` |

### Q9.2 ⭐ Are internal endpoints synchronous or asynchronous? (asked 2026-10-20)

The `/internal/v1/...` routes are plain synchronous HTTP. They're asynchronous
*relative to the user*, because only Temporal activities call them, long after
the 202. The activity needs the answer (reservation ids, or 409 for no stock) to
decide the next step, so making it fire-and-forget would need a reply channel.
That's what Temporal already is.

### Q9.3 ⭐ Why HTTP and not Kafka for order → inventory reserve?

The saga needs a yes/no before it charges a card. Request/reply over Kafka
would need correlation ids, reply topics, timeouts and a matching table:
rebuilding HTTP badly. Kafka carries *facts*; HTTP from an activity carries
*commands* that need an answer.

### Q9.4 How is the internal API protected from being called by users?

The gateway answers 404 for `/internal/**` and rejects service tokens on public
routes. Inventory's `require_internal` accepts only service tokens with scope
`inventory:internal`, and refuses even an admin's user token.

### Q9.5 Is there a circuit breaker?

No. Temporal backoff (1 s doubling to 10–60 s) already limits the attempt rate,
and no user is waiting. A breaker is the next step once real external providers
exist (B11).

### Q9.6 How does inventory protect itself under load?

`lock_timeout` 2 s, `statement_timeout` 5 s, 3 internal retries on contention,
then `503 inventory-contended`, which Temporal retries with backoff.

---

## 10. Kafka, the outbox and consumers

### Q10.1 ⭐ What is the transactional outbox, and why?

The state change and an `outbox` row are written in **one** local transaction.
A separate `outbox-relay` publishes the rows to Kafka. A dual write
(`db.commit(); kafka.send()`) loses the event if the process crashes between the
two. With the outbox, the worst case is a duplicate, which consumers absorb.

### Q10.2 ⭐ How does the relay work?

Every 500 ms: `SELECT … WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR
UPDATE SKIP LOCKED` → produce with `key = aggregate_id`, `acks=all`, idempotent
producer → mark `published_at` only for acked rows. One active relay per DB
(session `pg_try_advisory_lock`); other replicas are hot standbys, so batch
order is kept. Broker error → roll back the batch and back off from 0.5 s up to
30 s. Published rows are swept later.

### Q10.3 What's a poison outbox row, and how is it handled?

A row that can't be serialised or published. It's put on hold *per aggregate*:
that row and every later row of the same aggregate are skipped (to keep order),
while other aggregates keep flowing. The hold is retried every 60 s.

### Q10.4 ⭐ How is Kafka partitioned, and why?

Key-hash: key = aggregate id (`order_id`, `product_id`, `variant_id`,
`user_id`, `conversation_id`); `murmur2(key) mod 3`. All events of one entity go
to one partition, so they're consumed in order (for example `created` before
`cancelled`). Different entities run in parallel. 3 partitions, RF 1 (single
broker locally), 7-day retention, and `product.published` is compacted.

### Q10.5 Why 3 partitions and not 1?

With 1, per-key ordering and multi-partition rebalancing are never exercised.
3 lets a group run up to 3 replicas (cooperative-sticky assignment).

### Q10.6 ⭐ When do consumers commit offsets?

Auto-commit is off. The offset is committed manually only after (a) the effect
and its dedupe row have committed in the consumer's DB, or (b) the message is
on the DLQ. A crash can then only cause redelivery, never loss.

### Q10.7 ⭐ How are duplicates handled?

At-least-once delivery plus idempotent consumers. `INSERT INTO processed_events
(consumer_group, event_id) ON CONFLICT DO NOTHING` runs in the **same
transaction** as the effect. Conflict → already done → roll back, commit the
offset, move on. Rows are kept 30 days (swept by Celery).

### Q10.8 Compacted topics and 30-day dedupe retention: what's the catch?

A replay of a compacted topic can deliver events older than 30 days, after
their dedupe rows are gone. So those handlers must also be idempotent *by
state*. Stock initialisation is an `INSERT … ON CONFLICT DO NOTHING`.

### Q10.9 ⭐ Retry and DLQ policy for consumers?

- Poison (can't deserialise, unknown type) → DLQ at once, commit.
- Dependency down (`DependencyUnavailable`, lock timeout) → roll back, **pause** the partition, back off 0.5→8 s. The attempt is not counted, and it never goes to the DLQ.
- Other errors → 3 attempts with backoff → `<topic>.dlq` with headers (`dlq.error.class`, message, attempts, group, failed_at) → commit.

Replay is manual (`make dlq-replay`), because automatic replay of a poison
message is an infinite loop.

### Q10.10 Why not Kafka exactly-once (transactions)?

It covers Kafka→Kafka only. Our effects are Postgres writes (and emails).
Dedupe in the effect's own DB is the only thing that covers the whole path.

### Q10.11 Who consumes what?

- `inventory`: `catalog.product.published` (create stock rows) and `order.cancelled` (release held).
- `notification`: user registered, `order.*`, `shipment.status_changed`, `inventory.stockout`. `payment.*` is consumed but produces nothing.
- `assistant-indexer`: `catalog.product.*`.
- `analytics`: every topic.
- `analytics-dlq-observer`: every `*.dlq`, counted from headers only.

### Q10.12 Schema governance?

Avro + Schema Registry, `BACKWARD` compatibility. Producers never
auto-register. The outbox writer validates the payload against the Avro schema
at write time. CI checks schemas against `main` (`make schemas-check`).
`catalogue.json` lists every topic, key, cleanup policy and schema.

### Q10.13 What's in the event envelope?

`event_id`, `event_type`, `event_version`, `occurred_at`, `producer`,
`correlation_id`, `causation_id` (for example the `order.created` event id),
`actor_id`, `client_id`, `traceparent` (always null for now), then the payload.
The Kafka key is the outbox row's `aggregate_id`. Payload schemas are composed
with the envelope per topic.

### Q10.14 `order.created` and `order.cancelled` are separate topics. Doesn't that break ordering?

Kafka doesn't order across topics. It doesn't matter here: the inventory
consumer subscribes only to `order.cancelled`, and its handler is idempotent by
state.

### Q10.15 Kafka is down. Can customers still check out?

Yes. Writes commit with their outbox rows. Events wait in Postgres and are
published in per-aggregate order when Kafka comes back. Notification and
analytics are delayed, not lost.

---

## 11. Celery and RabbitMQ

### Q11.1 ⭐ Why Kafka *and* RabbitMQ *and* Temporal?

They answer different questions.
- Kafka: "this happened; whoever cares reacts." A log with fan-out, replay, schemas, per-key order.
- RabbitMQ/Celery: "do this job soon or on a schedule." A work queue: one consumer per message, deleted on ack, per-task retry, Beat scheduler.
- Temporal: "this multi-step process must finish or compensate, and tell me where it is."

Each does the others' job badly.

### Q11.2 What runs on Celery?

- `sweep_expired_reservations` (60 s), `reconcile_inventory_ledger` (daily 03:30), `sweep_processed_events` (daily) on `inventory.maintenance`
- `resume_unstarted_orders` (60 s) on `order.maintenance`
- `process_media` on `catalog.media`
- notification dispatch (5 attempts, backoff capped at 300 s) plus a 5 min redispatch sweep
- bulk AI generation and the analytics reconciliation

### Q11.3 Celery reliability settings and why?

`acks_late` (ack after the task finishes, so a killed worker's task is
redelivered), `reject_on_worker_lost`, `prefetch=1` (don't hoard tasks), at most
3 retries, jittered and capped at 60 s, one durable queue per workload. Every
task must be idempotent because of redelivery.

### Q11.4 Why not Kafka for tasks?

No per-message ack or retry: one slow job blocks its whole partition, because
Kafka redelivers by offset. No scheduler, no per-task time limits, and
parallelism is capped by the partition count.

### Q11.5 Why not RabbitMQ for events?

A message is deleted once consumed, so a consumer added later (indexer,
analytics backfill) can't read history. Fan-out needs a queue per consumer
declared by the producer, so the producer knows its consumers. No schema
registry.

### Q11.6 Is there a DLQ for Celery?

No DLX. After its retries run out, a task is logged as failed. The domain
record (a still-`pending` order, a failed delivery attempt) is the audit trail,
and the next sweep picks it up.

---

## 12. Database design

### Q12.1 ⭐ Database-per-service: what's in each?

- identity_db: users, merchant_profiles, refresh sessions, outbox
- catalog_db: products, variants, categories, media, audit, publish failures, outbox, processed_events (for the planned merchant-suspension consumer)
- order_db: orders, order_items, order_status_history, idempotency_keys, payment_attempts, outbox
- inventory_db: stock_items, reservations, stock_movements, outbox, processed_events
- notification_db: notifications, delivery attempts, subscriptions
- analytics: MongoDB (raw_events, rollups, dedupe)
- assistant_db: Postgres + pgvector (conversations, messages, chunks, embedding cache, generations, prompts)

### Q12.2 ⭐ How do you reference data across services without FKs?

Plain indexed UUIDs (`order_id` in reservations, `variant_id` and `merchant_id`
in order_items) plus **snapshots** of the fields needed (name, SKU, price).
Inside one service, real FKs are used (`reservations.variant_id →
stock_items.variant_id`, `order_items.order_id → orders.id ON DELETE CASCADE`).

### Q12.3 Why UUID primary keys, and where are there exceptions?

Ids that leave the DB shouldn't leak volume, and they must not collide when
merged in analytics. Exceptions: append-only logs (`stock_movements`,
`order_status_history`, outbox) use bigserial for cheap ordered inserts. The
human-friendly `order_number` (`SR-2026-0000123`) comes from a sequence and is
never used as a key across services.

### Q12.4 Which constraints protect invariants in the DB itself?

`CHECK (on_hand >= reserved AND reserved >= 0)`; generated `available`;
`UNIQUE (order_id, variant_id)`; `quantity > 0`; order money `CHECK >= 0`;
`idempotency_keys` PK `(user_id, key)`; `processed_events` PK `(consumer_group,
event_id)`; `outbox.event_id` unique; SKU unique per merchant and per product.

### Q12.5 Which indexes matter, and why?

- Partial `reservations(expires_at) WHERE status='held'` = exactly the sweeper's predicate.
- Partial outbox index `WHERE published_at IS NULL` = the relay's backlog.
- `orders(customer_id, created_at DESC)` for "my orders".
- `orders(status, created_at)` for `resume_unstarted_orders`.
- `stock_movements(reference_type, reference_id)` for "who moved this order's stock" and the reverse check.
- `idempotency_keys(expires_at)` for the sweep.

### Q12.6 How are audit trails stored?

Each service owns its audit trail, written in the same transaction as the
change: catalog's audit log (actor, action, before/after diff, workflow id),
order's `order_status_history`, inventory's ledger. A central audit service was
rejected: it can't share the transaction. Triggers were rejected: they can't
see who made the change.

### Q12.7 Why JSONB for variant options and shipping address?

Variant dimensions are per-merchant and per-category (size, colour…). They're
validated by Pydantic on the way in. Fixed columns or a per-category schema
would be rigid.

### Q12.8 Migrations?

Alembic per service, numbered `NNNN_<change>.py`, mandatory downgrade. A test
runs head → base → head and `alembic check`. Shared tables (`outbox`,
`processed_events`) come from `common` DDL.

### Q12.9 Why MongoDB for analytics?

Append-only documents of different shapes, and analytics must never contend
with checkout. Each event is one Mongo transaction: dedupe → archive to
`raw_events` → `$inc` rollups, bucketed by the UTC date of `occurred_at` (not
processing time, so replays land on the right day). An hourly job rebuilds the
last 6 days from `raw_events`.

---

## 13. Concurrency and race conditions

### Q13.1 ⭐ List the stock races you tested, and who wins each

`test_races.py`: commit vs release, commit vs expiry sweep, saga release vs
consumer release, reverse vs reverse, adjust vs reserve, set vs reserve, many
reserves on the last unit. Each has exactly one winner, and every test ends
with `find_drift() == []`.

### Q13.2 ⭐ How do the three release layers avoid double-releasing?

None of them knows about the others. Each one asks the DB, under the row lock,
"is this reservation still `held`?" The loser waits on the lock, re-reads, sees
`released`/`expired`, and skips. The saga's release also treats
already-released/expired as success.

### Q13.3 Two concurrent reserves for the *same* order (an activity retry overlapping the first attempt)?

The replay check runs *after* the stock-row locks, so the second waits, then
sees the first's reservations and replays them (or the unique constraint
stops a duplicate).

### Q13.4 Ten parallel `POST /orders` with the same key?

One wins the insert. The others get 409 (in progress) or the replayed 202.
Exactly one order (`test_ten_parallel_requests_with_one_key_make_one_order`).

### Q13.5 Sweep vs commit on the same reservation?

Both lock the stock row then the reservation. If commit wins: `committed`, and
the sweep skips. If the sweep wins: `expired`, the commit gets
`reservation-not-held` → refund → `failed / RESERVATION_EXPIRED`.

---

## 14. "What if…" failure questions

### Q14.1 ⭐ Inventory is down when the saga reserves

Reserve is retried 5 times within 60 s. If inventory recovers, the order
proceeds. Otherwise `failed / INVENTORY_UNAVAILABLE`, and nothing was charged.

### Q14.2 ⭐ Inventory is down during a compensation release

Release retries for 1 h. Meanwhile stock stays held. When inventory returns,
the saga retry or the sweep (whichever is first) frees it. If it fails for the
full hour, the workflow execution fails, no order event is written, and the
sweep still returns the stock.

### Q14.3 ⭐ Temporal (or the order worker) is down mid-saga

The workflow pauses with its state saved. After 15 min the sweep expires the
reservation. When the workflow resumes, a release is a no-op, or a commit gets
`reservation-not-held` → refund → `failed / RESERVATION_EXPIRED`. The customer
is not charged for stock they no longer hold.

### Q14.4 Catalog is down at checkout

503 before anything is written. The idempotency claim is released, so the
client's retry starts clean.

### Q14.5 Celery Beat is down

Sweeps and resume pause: held stock waits longer and stranded orders wait
longer. The saga's own releases still work. An in-process sweeper
(`inventory_sweeper_inprocess`) can be switched on, and running both is safe.

### Q14.6 Redis is down

Rate limits fail open (requests admitted, logged as `failed_open`). Nothing on
the order path depends on Redis. SSE fan-out for notifications is affected.

### Q14.7 Identity is down

User JWTs keep verifying locally. Workers' cached service tokens work until
they expire, then activities retry under their policies. Login and register fail.

### Q14.8 Postgres restarts

In-flight transactions roll back. Temporal activities, consumers (outage wait)
and Celery (`OperationalError` autoretry) all retry. HTTP clients retry with
the same idempotency key.

### Q14.9 The network drops the reply from inventory after a successful reserve

The activity times out and retries. Inventory finds the same `(order_id,
variant_id)` rows and returns them (200 replay). Nothing is reserved twice.

### Q14.10 The relay crashes after Kafka acked but before marking rows published

The rows are re-sent on restart, and consumers skip them by `event_id`.

### Q14.11 The address is undeliverable

After commit: `refunding` → refund → reverse commit (`on_hand += q`) → `failed
/ FULFILMENT_FAILED`.

### Q14.12 A refund keeps failing

The order stays in `refunding`, retried for an hour, visible to operators. 🪤 No
`REFUND_FAILED` is set and no event is emitted.

### Q14.13 Someone runs a manual SQL update that breaks `reserved`

The daily reconciliation logs the variant and its values. It does not fix it: a
human corrects it through a ledger adjustment.

### Q14.14 How would you find out who released order X's stock?

Look at the reservation status (`expired` → sweep) and the `stock_movements`
actor/reason (`service:…` → saga, `consumer:order.cancelled` → Kafka, `task:sweep…`
→ Celery), or the `inventory.released.reason`. Then open the workflow history
in the Temporal UI (localhost:8088).

---

## 15. Scalability and performance

### Q15.1 ⭐ 500 customers press Buy in the same second. What happens?

Gateway: per-user limit 30/min (one account flooding gets 429). Order API: one
indexed insert, one catalog read, one short transaction each. All get 202
(`pending`). Temporal: 500 workflows wait on `order-lifecycle`. Workers: each
replica runs ≤ 50 activities, 3 replicas → 150 at a time. Inventory: reserves
on different variants run in parallel; on the same variant they queue on the
row lock. That's on purpose: it's what makes overselling impossible. "Accepted
at once, executed at a configured pace."

### Q15.2 ⭐ What are the top bottlenecks, and the next step for each?

1. Hot SKU row lock → split stock into N bucket rows, or one queue per hot SKU. Signal: `inventory-contended` rate.
2. Single Temporal node → a cluster or Temporal Cloud.
3. Worker throughput → autoscale on schedule-to-start latency (safe now thanks to ORD-D36).
4. Catalog in the checkout path → a local price projection fed by events.
5. One Postgres server → split order/inventory onto their own servers, PgBouncer, read replicas.
6. Relay → a standby replica, CDC (Debezium) if lag grows.
7. Kafka 3 partitions → more partitions (remaps keys, so do it while quiet).

### Q15.3 How do you scale order-worker, and why was it unsafe before?

`--scale order-worker=N`; concurrency is explicit (ORD-D37). Before ORD-D36 the
fake provider's dedupe was in memory, so two replicas (or a restart) could
charge twice.

### Q15.4 How would you handle a real flash sale on one SKU?

Today: short transactions, fast reject once sold out, bounded lock wait plus
retry. Next: shard the stock row into buckets, or put a single reserver per hot
SKU behind a queue. Rejected options: reservations in Redis (not durable),
oversell-then-cancel (bad UX).

### Q15.5 What's cached?

Rate-limit counters, SSE tickets and pub/sub in Redis. 🪤 The browse (60 s) and
availability (10 s) caches are designed but not built.

---

## 16. Notifications

### Q16.1 Which events produce notifications?

- `user.registered` → welcome
- `order.confirmed`, `order.failed`, `order.cancelled` → customer
- `shipment.status_changed` → customer
- `inventory.stockout` → merchant (in-app)

`payment.*` → nothing (the customer already hears through the order event).

### Q16.2 How do you avoid sending a notification twice?

`processed_events`, plus a unique index on `(event_id, user_id, channel)`. That
still holds if a replay bypasses the dedupe table.

### Q16.3 How is in-app delivery done?

SSE. `EventSource` can't send headers, so the client gets a single-use 60 s
ticket (no JWT in the URL, which would leak into logs). Max 3 streams per user,
20 s keepalive, catch-up of 24 h / 50 frames with a per-device cursor. Redis
pub/sub fans out across replicas.

### Q16.4 Email and SMS?

Rendered at consume time, stored, and dispatched by Celery (5 attempts, backoff
≤ 300 s, plus a 5 min recovery sweep). Logged, not really sent (an adapter per
channel).

### Q16.5 Can a notification outage affect checkout?

No. Notification only consumes events. It calls no service and nothing calls
it, and order doesn't know it exists.

---

## 17. Analytics

### Q17.1 ⭐ How is revenue defined?

Gross: the sum of `total` on `order.confirmed`, shipping included, one series per
currency. Refunds and cancellations are *not* subtracted (net revenue is open
question Q-ANL-2).

### Q17.2 How are AOV and the stockout rate computed?

AOV = revenue in the window ÷ confirmed orders in the window (never an average
of daily averages). Stockout rate = (`depleted` + `reservation_refused`) ÷
orders created; `low_stock` is excluded.

### Q17.3 How does analytics stay correct with at-least-once delivery and replays?

Dedupe, raw archive and `$inc` all happen in one Mongo transaction, bucketed by
`occurred_at`. An hourly rebuild of the last 6 days from `raw_events` corrects
drift.

### Q17.4 Who can see which metrics?

Merchants see only their own (scope from the token). Overview and failure
metrics are admin-only.

---

## 18. AI assistant (Part B)

### Q18.1 ⭐ How does the product index stay in sync with the catalog?

The `assistant-indexer` consumes `catalog.product.*`: published → embed,
updated → re-embed (version check, stale events skipped), unpublished → remove.
If the embedding provider is down, it raises `DependencyUnavailable`, so the
consumer waits and the event stays in Kafka.

### Q18.2 Chunking and embeddings?

Typed chunks (identity, description, attributes, variants, category). Each
starts with the product name. Descriptions are about 256 tokens (512 max) with
80 overlap, max 12 chunks per product. Voyage `voyage-3.5`, 1024 dims, with a
deterministic hashing fake for local runs and CI.

### Q18.3 Retrieval?

Hybrid: pgvector HNSW dense search plus Postgres full-text, 50 candidates each,
fused with RRF (k = 60). Filters (published, category, price) run in SQL before
the vector scan. If embeddings are down, it falls back to keyword only, marked
`degraded`.

### Q18.4 ⭐ Why are price and stock not taken from the index?

A stale price must never be shown as current. They're fetched live from catalog
and inventory (2 s). On failure the value is null, and the model is told to say
"unknown".

### Q18.5 How does the LangGraph flow work, and why not a tool-calling agent?

guard → memory → classify intent (Haiku) → retrieve / compare / follow-up /
clarify / smalltalk / refuse → hydrate → assemble context → answer (Sonnet).
Nodes call the tools; the model never chooses them. That's deterministic and
gives a smaller prompt-injection surface.

### Q18.6 Guardrails and cost control?

Answer only from the given `<products>` and cite `[product:<id>]`. Merchant text
is sanitised at index time, messages that look like injection are blocked, max
2000 chars, and the assistant never reads orders or personal data. Budget:
50k tokens/user/day and 2M/day globally, checked before streaming (429). Context
budget 6000 tokens. Gateway chat limit 3–10/min.

### Q18.7 Streaming?

SSE over POST, in this order: `status` → `sources` → `token`… → `usage` → `done`.
Sources come first, so a mid-stream failure still shows products. On disconnect,
the provider call is cancelled and the partial answer saved.

---

## 19. Security

### Q19.1 What stops header spoofing?

The gateway strips inbound `X-User-*` and `X-Internal-*` headers and overwrites
`X-Request-ID`. Services build identity from the verified JWT only.

### Q19.2 What are the rate limits, and why fail open?

Login 5/min per IP; default 100/min per IP; orders 30/min per user; chat 3 or
10/min. A Redis fixed-window counter shared by every gateway replica; rejected
requests still count. Fail open: a limiter outage must not become a platform
outage, and it's logged. 🪤 The per-email login limit isn't built.

### Q19.3 Prompt injection: where's the risk?

Any merchant can plant instructions in product text that end up in other
customers' prompts. Mitigations: sanitise at index time, grounded answers only,
fixed graph (no model-chosen tools), and no access to personal data.

---

## 20. Testing

### Q20.1 ⭐ What is your test strategy?

- Unit tests (no DB): state machines, workflow branches with stub activities, policies.
- Integration tests against real Postgres migrated to head: repos, races, the idempotency claim, migrations head→base→head.
- Temporal tests with a test environment: every saga branch and worker kill/resume.
- e2e through the gateway on the real stack: burst, cancel race, reconciliation, two-replica charging, Temporal-down resume, declined payment restores stock.
- 85 % branch coverage gate.

### Q20.2 How do you test race conditions for real?

Integration tests fire concurrent sessions at the real DB (for example 50
parallel reserves on 10 units → exactly 10), then assert the invariants (I5, I6,
`find_drift() == []`).

### Q20.3 How do you prove "exactly one charge"?

`test_payment_store.py` (two replicas charging at once; a restarted worker sees
the earlier charge), the flaky-provider tests, and e2e
`test_two_worker_replicas_charge_each_order_once`.

---

## 21. Observability and operations

### Q21.1 🪤 What observability exists today?

JSON logs with request/trace id on every line, the correlation id through
outbox/Kafka/workflow/ledger, the Temporal UI (localhost:8088 here), consumer
lag via `kafka-consumer-groups`, DLQ counts in analytics `failure_stats`, health
endpoints. Prometheus/Grafana/Jaeger/OTel are designed but not deployed
(`OTEL_SDK_DISABLED=true`).

### Q21.2 What signals would you alert on?

A rising share of `expired` releases (layer 1 failing), outbox lag (age of the
oldest unpublished row), consumer lag, DLQ arrivals, schedule-to-start latency
on `order-lifecycle`, `inventory-contended` rate, orders stuck in `refunding` or
`pending` with no run id, and reconciliation drift.

### Q21.3 How is the local stack run?

`make keys up migrate seed`, with Compose profiles `core`, `workflow`, `events`
and `ai`. `make topics` creates topics, DLQs and schemas. `make test-e2e` runs
the e2e suite. `make dlq-replay`, `make review-ui`.

---

## 22. Curveballs and "why not X"

### Q22.1 Why not 2PC/XA across services?

It ties every service's availability to every other's, holds locks while the
coordinator decides, and payment providers, Kafka and Temporal don't take part
in XA anyway.

### Q22.2 Why not Debezium CDC instead of a polling relay?

CDC removes polling but adds a connector cluster and WAL config. The polling
relay is about 200 testable lines. CDC is the upgrade path if outbox lag grows.

### Q22.3 Why not event sourcing?

Too costly for five weeks, and every read would fold history. The ledger gives
the audit benefit for stock without it.

### Q22.4 Why not charge first and reserve later?

A stock failure after a charge needs a refund: the expensive failure. Reserve
first makes the cheap failure first.

### Q22.5 Why not let the customer retry payment on the same order after a decline?

Considered, not built. Today a decline ends the order (`failed`) and releases
stock immediately, so other customers can buy it. A retry-payment flow would
need a longer hold and a new saga branch.

### Q22.6 Why allow cancellation only before commit and not until shipment?

After commit, stock has left the shelf. Undoing it is a refund + reverse flow
(post-confirmation refunds are designed in the state machine, but no trigger is
built). Keeping the pivot clean keeps compensation simple.

### Q22.7 What would you change first if this went to production?

Real observability (OTel, metrics, tracing across Kafka); the Temporal Update
for cancellation; `REFUND_FAILED` plus an event and an alert; workflow
versioning; approval gating for merchants; a circuit breaker and real provider
webhooks (HMAC); a Temporal cluster; the append-only ledger grant; and caches.

### Q22.8 What's the one invariant you'd defend above all?

`on_hand ≥ reserved ≥ 0`: never oversell. It's enforced three times: the lock
and check in the repo, the DB `CHECK` constraint, and the daily reconciliation.

### Q22.9 Where does the system choose consistency over availability?

Stock reservation (queue on the lock rather than risk oversell), order creation
(catalog must answer, or 503), refunds (retry for an hour, never give up
silently). Availability over consistency: rate limiter fails open, notification
and analytics are eventually consistent, and the order is accepted even when
Temporal is down.
