# RFC 0007 — `notification` Service

| | |
| --- | --- |
| **Owner** | @muhammad.adnan |
| **Collaborators** | SarwanAhmed |
| **Status** | **DRAFT** · IN-REVIEW · DONE |
| **Created** | Sep 29, 2026 |
| **Supersedes** | [archive/0003 — Events & Observability](archive/0003-events-and-observability.md) (the `notification` service, §3.1.1a delivery path, C15–C23, notification endpoints, Redis `nt:*` keys, notification tests) · [archive/0006 — Service Decomposition & Data Model](archive/0006-service-decomposition-and-data-model.md) (`notification_db` catalogue, ownership-test row 6, opaque client token reference kind) · [archive/0000 — System Architecture](archive/0000-system-architecture.md) (notification rows of the service inventory, surface map and event catalogue) |
| **Depends on** | [RFC 0000 — Architecture](0000-architecture.md) · [RFC 0001 — Platform and Shared Infrastructure](0001-platform-and-shared-infrastructure.md) · [RFC 0002 — Gateway](0002-gateway-service.md) · [RFC 0003 — Identity](0003-identity-service.md) · [RFC 0005 — Order](0005-order-service.md) · [RFC 0006 — Inventory](0006-inventory-service.md) |

> **Reading note.** This RFC specifies the `notification` service (:8005) and everything it owns: which events it consumes and what it renders from them, the at-most-one invariant, the channel adapters, the Celery dispatch and maintenance tasks, the SSE push stream with its tickets, fan-out, cursors, scoping and acknowledgement, the read-state API, and the `notification_db` schema. Cross-cutting rules — the boundary rules R1–R8, the decisions A1–A14, schema conventions S1–S15, the event envelope and catalogue, and the shared `outbox`/`processed_events` DDL — live in [RFC 0000](0000-architecture.md) and are linked, not restated. Hosting of the Celery worker, Redis, Kafka and the observability stack is in [RFC 0001](0001-platform-and-shared-infrastructure.md). Decision IDs from the archive (C4, C15–C23, Q11–Q18, G5a–G5e …) are kept verbatim; decisions new to this RFC carry the prefix `NTF-`.

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
     - 3.2.1. [Decision register](#321-decision-register)
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

**The `notification` service — event-driven, at-most-one-per-(event, user, channel) notifications, rendered at consume time, dispatched by Celery through channel adapters, and pushed to connected clients over a resumable, device-scoped SSE stream that sits over a durable store.**

---

## 2. Overview

Checkout returns `202` ([RFC 0005](0005-order-service.md) B9): the order exists, but its outcome is decided later by a Temporal workflow on a worker, with no request in context. Something has to tell the customer how it ended. [Part A](../part-a.md) §1 names exactly that as broken today — *"order confirmation, dispatch updates, and refund notices are unreliable during traffic spikes"* — and §4 lists notifications among the work that must run independently, be traceable and recoverable, handle failure gracefully and avoid double-processing. §6 requires that failures in notifications be diagnosable.

`notification` is a pure event consumer. It reads facts other services publish (`order.*`, `payment.*`, `shipment.status_changed`, `inventory.stockout`, `identity.user.registered`), records **at most one** notification per `(event, user, channel)`, renders it from a template at consume time, and delivers it:

- **`in_app`** — pushed over Server-Sent Events to any connected device, fanned out between replicas over Redis Pub/Sub, resumable by a monotonic `seq`, bounded on catch-up, scoped to the device that caused it where that matters, and acknowledged by a best-effort delivery report. Returning later, the durable list is the source of truth.
- **`email`** and **`sms`** — rendered and dispatched by a Celery task through a logging adapter, with every attempt recorded. A real provider is an adapter swap.
- **`push`** (Web Push / FCM / APNs) — specified (the `push_subscriptions` table and the adapter interface) but not registered, so the offline half of delivery is an adapter rather than a redesign.

Recording a notification is not delivering one, and the design turns on one constraint: **the push path is an accelerant over a durable store, never the source of truth** (C16). Every failure in delivery therefore degrades to latency or to a list entry, never to loss.

### 2.1. Goals

| # | Goal | How it is verified |
| --- | --- | --- |
| G3 (part) | Replaying the entire topic into `notification` changes nothing | Reset the `notification` consumer group to earliest, re-consume, assert the `notifications` and `delivery_attempts` row sets are unchanged |
| G5 | Notifications are produced for order confirmed, failed, cancelled, shipped, delivered, and stockout (and a welcome on registration) | Integration test per event type; delivery records persisted with attempt history |
| G5a | A connected client receives a notification by server push, without polling, within a second of the event being consumed | Two `notification` replicas: a client streams from one, the event is consumed by the other, the frame arrives (C15–C19) |
| G5b | **A stream that drops loses nothing.** Reconnection replays every notification missed, in `seq` order with no duplicates and no holes, **up to the resume bound of 50 frames and 24 hours**; past the bound the client is told the count and the durable list carries the remainder. The notification is never lost — only its *delivery mode* changes from frame to list entry (C22) | Kill a connection mid-flight, publish N ≤ 50, reconnect → exactly N frames in `seq` order. Repeat with N = 200 → 50 frames plus one `catchup_truncated` with `omitted_count: 150`, and `GET /notifications` returns all 200 |
| G5c | **A device the server has never seen receives a badge, not a backlog.** A first-ever `client_id` replays zero historical frames and learns its unread count from the `hello` frame (C22) | Seed 500 notifications; connect with an unseen `client_id` and no `Last-Event-ID` → 0 `notification` frames, one `hello` with `cold_start: true` and the correct `unread_count`, and live frames from that point |
| G5d | **A notification scoped to the originating device does not toast on the others, and is never lost by them** (C21) | Two clients for one user; place an order from client A → the `order_confirmed` frame arrives only at A, while B's `unread_count` still increments and `GET /notifications` on B shows it |
| G5e | **A delivered notification is distinguishable from a merely recorded one** (C23) | A client receives a frame and acks; `delivered_at` and `delivered_client_id` are set; a second ack of the same ids changes nothing; a never-acked notification keeps `delivered_at IS NULL` and appears in the admin view |
| G9 (part) | Email rendering and dispatch are off the request path and off the consumer's transaction | No handler or consumer calls an adapter inline; dispatch is a Celery task |
| G10 (part) | "A customer got no notification" and "a customer got it late, or only on refresh" are diagnosable from a symptom | The two walkthroughs in §7, performed at review |

### 2.2. Non-goals

| Excluded | Why / where it lives instead |
| --- | --- |
| Real email or SMS delivery | `notification` records, renders, and **delivers `in_app` over SSE**. The email and SMS adapters log. A provider is an interface swap, and no account is in scope |
| Web Push / FCM / APNs to a backgrounded or closed app | The offline half of delivery. The `NotificationChannel` vocabulary gains `push` and `push_subscriptions` is specified (§3.1.3) so it is an adapter, not a redesign — but VAPID keys and a service worker are a client project, and there is no client |
| WebSockets | The traffic is server → client only. SSE is plain HTTP, so the gateway, JWT verification, rate limiting, tracing, and problem-detail errors all apply unchanged. See C15 and §5, Alternative 10 |
| Acknowledgement as a **delivery protocol** — blocking sends, server-side retry on a missing ack, at-least-once push | Out, for C16's reason: the durable row is the receipt, and rebuilding TCP badly buys nothing. What *is* in scope is a best-effort delivery **report** (C23) — `delivered_at` plus a per-device cursor — which records what happened without anything depending on it |
| Customer-facing notification preferences and unsubscribe | Deferred; the schema has a `channel` column so it is additive. Per-user delivery scope would join it there (Q16) |
| Non-notification events on the stream (order status, inventory changes) | One stream per concern (Q12). A general-purpose client event bus deserves its own RFC |
| Alerting (Alertmanager, PagerDuty, on-call) | No operational context to alert into; dashboards and traces cover the requirement ([RFC 0001](0001-platform-and-shared-infrastructure.md)) |
| Producing events | `notification` is a terminal consumer. It has no `outbox` and nothing depends on its facts (§3.1.2.3) |
| Hosting the Celery worker, Beat, Redis, Kafka, RabbitMQ | [RFC 0001](0001-platform-and-shared-infrastructure.md). This RFC specifies the task bodies and schedules; the platform hosts them |
| Minting or validating `X-Client-Id` at the edge | The gateway passes it through and validates its shape ([RFC 0002](0002-gateway-service.md)); producers persist it into their outbox rows ([RFC 0000](0000-architecture.md#3124-event-envelope)) |

---

## 3. Solution Overview

### 3.1. Specification

**Diagram index.**

| Kind | Diagram | Section |
| --- | --- | --- |
| Component | Service components: consumer, routers, stream, worker, stores | [§3.1.1](#311-architectural-changes) |
| Flow | Consumer decision flow per event | [The consumer](#the-consumer) |
| Flow | Choosing the replay floor | [The SSE stream](#the-sse-stream) |
| Flow | Per-connection frame filter | [The SSE stream](#the-sse-stream) |
| Sequence | Consume to frame, across replicas | [The consumer](#the-consumer) |
| Sequence | Stream ticket, open and catch-up | [The SSE stream](#the-sse-stream) |
| Sequence | `origin_first` promotion sweep | [The SSE stream](#the-sse-stream) |
| Sequence | Dispatch with its two triggers | [Celery tasks](#celery-tasks) |
| Sequence | Batched ack | [`POST /notifications/ack`](#post-notificationsack) |
| State | Notification lifecycle (concurrent facts) | [§3.1.1.3](#3113-state-machines) |
| State | Delivery attempt | [§3.1.1.3](#3113-state-machines) |
| State | Stream cursor | [§3.1.1.3](#3113-state-machines) |
| State | Stream connection | [§3.1.1.3](#3113-state-machines) |
| Data | `notification_db` tables | [`notification_db`](#notification_db) |

#### 3.1.1. Architectural changes

```mermaid
flowchart TB
    kafka[["Kafka smartretail.order, payment, shipment, inventory, identity topics<br/>consumer group notification, keyed by aggregate_id"]]
    subgraph svc["notification (:8005, N replicas)"]
        consumer["notifications/consumers/<br/>dedupe (processed_events), resolve recipient,<br/>derive type, channels, scope, render template,<br/>INSERT notifications (at-most-one), COMMIT"]
        api["v1/notifications/routes.py<br/>list, read, read-all, unread-count, ack, admin"]
        streamapi["v1/stream/routes.py<br/>stream-ticket, stream"]
        stream["stream/<br/>ticket redeem, cursor or cold start,<br/>bounded catch-up, SUBSCRIBE, frames"]
    end
    db[("notification_db (PG 16)<br/>notifications, delivery_attempts,<br/>push_subscriptions, stream_cursors,<br/>processed_events")]
    redis[("Redis<br/>nt:user pub/sub, nt:ticket, nt:conns,<br/>nt:client presence, rl:*")]
    rabbit[["RabbitMQ queues notifications and maintenance"]]
    worker["celery-worker + Beat<br/>notifications/tasks/, stream/tasks/<br/>dispatch_notification, promote_undelivered_notifications,<br/>sweep_stale_stream_cursors, redispatch_undispatched_notifications"]
    gateway["gateway (:8000)<br/>no buffering on text/event-stream"]
    client["client (EventSource)"]

    kafka --> consumer
    consumer --> db
    consumer -- "after commit: PUBLISH nt:user:{id}" --> redis
    consumer -- "after commit: enqueue dispatch_notification (non in_app)" --> rabbit
    rabbit --> worker
    worker --> db
    worker -- "re-PUBLISH promoted rows" --> redis
    api --> db
    api -- "unread_count message" --> redis
    streamapi --> stream
    stream --> db
    stream -- "SUBSCRIBE nt:user:{id}" --> redis
    client --> gateway
    gateway --> api
    gateway --> streamapi
```

**Decisions that bind this service.** Carried from the archive with their original IDs:

- **C4 — At-least-once delivery with idempotent consumers,** not Kafka transactions. The consumer records `(consumer_group, event_id)` in `processed_events` before acting, in the same transaction as the effect. For `notification` the unique `(event, user, channel)` constraint is a second, domain-level guard behind it.
- **C6 — Retry in-process with backoff, then DLQ.** Three in-process attempts with exponential backoff, then the event goes to `<topic>.dlq` with its error, and the consumer commits the offset and moves on. A poison message must not stop a partition. (Refined by NTF-14 for infrastructure outages.)
- **C7 — Celery handles one-shot jobs; Kafka handles facts; Temporal handles processes.** Notification dispatch is a Celery task enqueued by the consumer; periodic sweeps run on Celery Beat.
- **C11 — Trace context propagates over Kafka.** The consumer restores W3C `traceparent` from the envelope, so a checkout's trace ends in the `notification` consumer span, and the Celery dispatch task continues it.
- **C15 — Notifications are pushed to connected clients over SSE, not polled for.** Checkout returns `202` (B9), so the notification is the customer's only push-based window into a workflow that completes after the request ended. Polling `GET /notifications` makes latency a function of the poll interval and load a function of idle users — both wrong. SSE is chosen over WebSockets for the same reasons [RFC 0009](0009-assistant-service.md) E4 chooses it for the assistant: the traffic is one-way, SSE is plain HTTP, so the gateway, JWT verification, rate limiting, tracing, and problem-detail errors apply unchanged, and reconnection is native to `EventSource`.
- **C16 — The push path is an accelerant over a durable store, never the source of truth.** The `notifications` row stays authoritative; SSE only makes it arrive sooner. This single constraint licenses everything below — the fan-out bus may be lossy, replicas may restart, and a frame that never arrives costs only latency, because the row is already durable. Frames *are* acknowledged (C23), but the acknowledgement is a **record, not a protocol**: no send blocks on it, no missing ack triggers a resend, and no notification is ever marked failed for want of one. The single decision an ack participates in is widening an origin-scoped notification's audience after a grace period (C21) — which degrades toward broadcast, never toward loss. Inverting C16, and treating the open connection as the delivery guarantee, is what makes real-time systems fragile.
- **C17 — Fan-out between replicas is Redis Pub/Sub, one channel per connected user.** Kafka partitions by `aggregate_id`, so the replica that *consumes* an event is almost never the replica *holding* that user's connection. On stream open the replica issues `SUBSCRIBE nt:user:{user_id}`; on close, `UNSUBSCRIBE`. The consumer `PUBLISH`es after its transaction commits. At-most-once is sufficient under C16, and Redis is already in the stack, so this adds no new infrastructure. The message body carries `origin_client_id` and `scope`, and the *delivering* replica filters per connection (C21) — fan-out stays one channel per user rather than one per device, because a per-device channel would need the consumer to know channel names it cannot derive and would multiply `SUBSCRIBE` churn for a three-line predicate at the edge. That is Q11's reasoning, and scoping does not reopen it.
- **C18 — Every notification carries a monotonic `seq`, and streams resume with `Last-Event-ID`.** Each SSE frame sets `id: <seq>`. `EventSource` resends the last received id on reconnect, so the handler replays `WHERE user_id = ? AND seq > ?` before delivering live frames. Network blips, pod restarts, laptop sleep, and dropped Pub/Sub messages all recover by the same path. The client never asks "anything new?", it reconnects and is told. But `Last-Event-ID` is a *hint*, not the only cursor: `EventSource` retains it only across its own automatic reconnects — a page reload, a new tab, or a fresh browser profile sends none — so the resume floor is server-side (C22), and catch-up is bounded in both count and age.
- **C19 — SSE connections authenticate with a short-lived single-use ticket, not a bearer header.** `EventSource` cannot set `Authorization`. `POST /notifications/stream-ticket` exchanges the bearer token for a 60-second single-use ticket held in Redis; the stream is opened with it. Putting the JWT in the query string instead would leak it into access logs, proxy logs, and `Referer` headers.
- **C20 — `notification` replicas hold connections but no *replica-affine* state.** Nothing recoverable is pinned to a replica: any replica serves any reconnect, there are no sticky sessions, and no directory maps a client to a pod. Two pieces of *shared, advisory* state exist and are deliberately not per-replica — the per-`(user_id, client_id)` replay cursor in Postgres (C22) and a short-TTL presence key in Redis (C21). Both are reconstructible and both fail safe: lose the cursor and a client cold-starts, lose the presence key and an origin-scoped notification broadcasts. The line that must not be crossed is a registry recording *where* a connection lives, because that is the one that would make routing sticky.
- **C21 — Delivery is scoped by the *originating client*, not only by the user.** A browser profile or app install mints a stable `client_id` and sends it as `X-Client-Id` on mutating requests. The producing service records it in the outbox row; it travels in the event envelope; `notification` stores it as `origin_client_id` and derives a `delivery_scope` — `origin_only` \| `origin_first` \| `all_devices` — from the notification type. A checkout begun on a laptop must not toast on a phone in a pocket; a shipment update, which has no originating client at all, must reach every device. **Scope affects frames only: `unread_count` and `GET /notifications` are never scope-filtered, so every device's badge still moves.** That is the property that makes targeting safe rather than lossy, and it is the one an implementer must not quietly optimise away.
- **C22 — Catch-up is bounded, and the replay floor lives on the server.** `stream_cursors(user_id, client_id)` holds `cursor_seq`, advanced by the ack endpoint (C23). The replay floor is `greatest(Last-Event-ID, cursor_seq)`, and catch-up is additionally capped at **50 frames and 24 hours**. A `client_id` seen for the first time replays **zero** frames: it receives a `hello` frame carrying `unread_count` and reads history from `GET /api/v1/notifications`. An unbounded `WHERE seq > cursor` on a fresh device replays a lifetime of notifications as live frames, which is not a resume — it is a denial of service the user performs on themselves.
- **C23 — The ack is a report, not a protocol.** `POST /api/v1/notifications/ack` is batched, idempotent, rate-limited, fire-and-forget from the client, and returns `204` unconditionally. It stamps `delivered_at` (first writer wins) and advances the client's cursor. It is explicitly **not** at-least-once push: the server never resends because an ack is missing, never retries an ack, never queues one, and a stream never waits for one. A client that acks nothing is a supported configuration — it cold-starts on every reload and every origin-scoped notification broadcasts after the grace period, which is exactly the behaviour the design had before the ack existed.

**Decisions new to this RFC.** Each closes a gap the archive left implicit; each is flagged for review with the rest of this RFC.

- **NTF-1 — `notifications.payload` stores the whole source envelope, and the at-most-one constraint is a unique expression index on it.** archive/0006 states the invariant "at-most-one per `(event_id, user_id, channel)`" but gives `notifications` no `event_id` column. Rather than add a column the schema catalogue does not have, the constraint is `UNIQUE ((payload->>'event_id'), user_id, channel)`. `payload` is already "the source event, for rendering and for support" (S8), so the envelope's `event_id` is always present; the expression index is the column the invariant needs without a second copy of the id. If review prefers an explicit column, it is an additive migration (§3.3.1, NTF-Q6).
- **NTF-2 — One row per channel; only `in_app` rows are listed, counted, or streamed.** The invariant is per channel, so an event producing an in-app notification and an email produces two rows. `GET /notifications`, `unread-count`, `read-all`, the SSE stream and the catch-up query all filter `channel = 'in_app'`; `email`/`sms`/`push` rows are delivery records, visible in the admin view.
- **NTF-3 — The channel set is a per-type constant in code** (§3.1.1.2). Preferences are deferred (§2.2), so a per-type table is the honest shape, and it is where preferences would plug in.
- **NTF-4 — `in_app` has no `delivery_attempts` rows.** Its delivery record is `delivered_at`/`delivered_client_id` (C23). `delivery_attempts` records dispatches through an adapter — `email`, `sms`, and one day `push`.
- **NTF-5 — Dispatch retry policy.** `dispatch_notification` retries up to 5 times with exponential backoff (2 s base, ×2, capped at 300 s, full jitter), `acks_late=True`, and records one `delivery_attempts` row per attempt.
- **NTF-6 — An orphan sweep backs up the post-commit enqueue.** A crash (or a RabbitMQ outage) between the consumer's commit and its enqueue would otherwise leave a dispatched-channel row with no attempt, forever. `redispatch_undispatched_notifications` (Beat, every 5 minutes) re-enqueues such rows; the task is idempotent, so a double enqueue is harmless.
- **NTF-7 — `payment.*` and non-customer-visible shipment transitions are consumed but produce no notification.** The event catalogue lists `notification` as a consumer of `payment.succeeded` / `payment.failed`, but `NotificationType` has no payment value: the customer-visible outcome of a payment is `order.confirmed` or `order.failed`, emitted by the same workflow, and notifying both would be the duplicate the service exists to prevent. Likewise `shipment.status_changed` to `created` produces nothing; `shipped`, `in_transit` and `delivered` produce `shipment_update`. The events are still recorded in `processed_events`, so replay is uniform.
- **NTF-8 — Minimum payload fields are a contract `notification` places on producers** (§3.1.2.4). No archived RFC fixes event payloads; the fields `notification` renders from are listed so producers design against them. *Aligned to the registered Avro schemas on Oct 15, 2026 (NTF-D33).*
- **NTF-9 — An `origin_first` notification whose origin device is not present at consume time is widened immediately.** The consumer checks `nt:client:{user_id}:{origin_client_id}` before insert; absent, the row is inserted with `promoted_at = now()` and published as `all_devices`. This is the reading of the "C20 retreat" (§3.1.3): the presence key exists precisely so the common case of an offline origin does not wait out the 45-second grace.
- **NTF-10 — The stream subscribes before it catches up.** The archive's ordering (catch-up, then `SUBSCRIBE`) has a window in which a notification committed between the catch-up query and the subscription is neither replayed nor received live. The handler `SUBSCRIBE`s first, buffers live messages into the connection queue while the catch-up runs, then drains the buffer, skipping any `id` already sent. The client already de-duplicates by `id` (the visibility trap), so this adds no new client obligation.
- **NTF-11 — `unread_count` travels in the Pub/Sub message.** The consumer computes the recipient's in-app unread count once, after commit, and includes it in the message, so a delivering replica emits the `unread_count` frame without a query per connection. Mark-read and read-all publish a bare `{kind: "unread_count"}` message on the same channel, so every device's badge moves when one device reads.
- **NTF-12 — A stream ticket without a `client_id` opens an ephemeral stream.** A missing or stripped `X-Client-Id` must never fail a request (C21). The ticket then carries `client_id: null`; the stream has no `stream_cursors` row, its floor is `Last-Event-ID` alone (cold start when absent), it is never the "origin" of anything, and `origin_only` frames are never delivered to it.
- **NTF-13 — The notification lifecycle is derived from timestamps, not stored as a status.** archive/0006 gives `notifications` no status column; `read_at`, `delivered_at` and `promoted_at` are independent facts written by different actors, and a single status would force an order on them that does not exist (§3.1.1.3).
- **NTF-14 — Infrastructure outages pause the consumer; they do not dead-letter.** C6's three-attempts-then-DLQ is for poison messages. A `notification_db` or Redis-for-presence outage is not the event's fault: the consumer pauses the partition and retries with capped backoff until the dependency returns. This is a requirement on the `common` consumer base class ([RFC 0001](0001-platform-and-shared-infrastructure.md)); failing to meet it would DLQ every event for the duration of an outage.
- **NTF-15 — The ack rate limit is enforced in-service,** keyed `rl:notif-ack:{user_id}:{client_id}`, because the gateway cannot see the body's `client_id`.
- **NTF-16 — `since_seq` switches list ordering to `seq ASC`.** Without it, the list is `created_at DESC` (newest first, for a UI); with it, the list is forward paging for a truncated client, and only ascending `seq` lets that client advance contiguously.

##### 3.1.1.1. Responsibility and boundaries

**Owns.**

| Fact / capability | Store |
| --- | --- |
| Rendered notifications, read state, `seq` stream cursor, origin client and delivery scope, delivery report | `notifications` |
| Per-channel dispatch attempts and provider errors | `delivery_attempts` |
| Web Push subscriptions (specified, unused) | `push_subscriptions` |
| Per-`(user, device)` SSE replay floor | `stream_cursors` |
| Consumer dedupe | `processed_events` |
| Stream tickets, per-user connection counts, presence hints, the per-user fan-out channel | Redis `nt:*` (§3.1.3) |
| Templates for the six `NotificationType`s | code, `app/v1/notifications/templates/` |
| The SSE stream contract and frame vocabulary | code, `app/v1/stream/` (`app/v1/stream/` for the HTTP contract) |

**Does not own.**

| Not owned | Owner |
| --- | --- |
| Users, roles, which user is a merchant | `identity` ([RFC 0003](0003-identity-service.md)) — read as a JWT claim; `user_id` columns are plain `uuid`s with no FK (R2) |
| Order, payment and shipment state; `orders.origin_client_id`; `orders.notified_at` | `order` ([RFC 0005](0005-order-service.md)) — read only as event payload |
| Stock and the stockout fact; which merchant owns a variant | `inventory` ([RFC 0006](0006-inventory-service.md)) — read only as event payload |
| `X-Client-Id` shape validation and pass-through; SSE proxying without buffering | `gateway` ([RFC 0002](0002-gateway-service.md)) |
| The event envelope (including `client_id`) and the outbox that carries it | [RFC 0000](0000-architecture.md#3124-event-envelope) |
| Celery worker/Beat hosting, Redis, RabbitMQ, Kafka, consumer base class, DLQ replay tool | [RFC 0001](0001-platform-and-shared-infrastructure.md) |

**Ownership-test row** ([RFC 0000 §3.1.1.2](0000-architecture.md#3112-the-ownership-test)):

| # | Candidate | State it would own | Invariant | Independent change | Distinct profile | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| 6 | `notification` | notifications, delivery attempts | at-most-one per (event, user, channel) | yes | bursty, fully async, failure-tolerant | **Service** |

Since the ownership test was applied, the service has also acquired the per-device stream cursor and a second operational profile — it holds long-lived connections, so capacity is measured in sockets as well as RPS. Both strengthen the verdict.

**Rules and decisions that bind it.**

| Rule / decision | How it applies here |
| --- | --- |
| R1 | `notification` connects only to `notification_db` with its own role |
| R2 / S15 | `user_id` columns carry no FK; `delivery_attempts.notification_id` is a real FK because it is the same database |
| R3 | Sole writer of every fact above; ownership map row "Notification, delivery record, and per-device stream cursor" |
| R4 / R7 | Never calls another service. Every read endpoint serves from `notification_db`; nothing on its request path requires another service to be up |
| R5 | No synchronous outbound call exists |
| A3 | No cross-service foreign keys |
| A4 / A5 | Every endpoint verifies the RS256 JWT locally; the stream verifies it at ticket issue and carries its expiry |
| A6 | Async throughout — FastAPI, SQLAlchemy async, `asyncpg`, `redis.asyncio`; the SSE handler is an asyncio task per connection |
| A7 | Every error, before a stream opens, is an RFC 9457 problem detail; after it opens, an `error` frame carrying one |
| A8 / C12 | `correlation_id` on every row is the trace id of the causing request |
| A11 / S13 | `processed_events` in the same transaction as the insert |
| A12 / C7 | Dispatch is a Celery task; nothing here is a Temporal workflow |
| S1 | `uuid` keys; `notifications.seq` is the S1 `bigserial` exception |
| S4 | Four `DomainEnum` vocabularies (§3.1.1.2) |
| S8 | `payload jsonb` holds the source envelope — never filtered except by the NTF-1 expression index, which is a uniqueness guard, not a query path |

**Module layout** ([RFC 0000 §3.1.1.7](0000-architecture.md#3117-module-structure-inside-a-service); entity-first per [file-structure §4–§5](../file-structure.md#4-service-layout-every-service)). The entities are `notifications/` (`notifications`, `delivery_attempts`), `subscriptions/` (`push_subscriptions`) and `stream/` (`stream_cursors` and the SSE machinery). The Kafka consumer and the Celery tasks live under the entity whose rows they write ([ARC-D27](0000-architecture.md#321-decision-register), NTF-D25): `notifications/consumers/`, `notifications/tasks/` (dispatch, promotion, redispatch) and `stream/tasks/` (the cursor sweep). There is no app-level `consumers/` or `tasks/`.

```
services/notification/
├── main.py                       create_application(): settings, logging, DB, router, /health, consumer start/stop
├── alembic.ini
├── alembic/versions/             0001_initial.py, 0002_stream_cursors.py, 0003_enum_comments.py
├── app/
│   ├── router.py                 includes only: owns /api/v1 and the /notifications prefix for both entity routers
│   ├── models.py                 imports every entity's ORM models onto one metadata
│   ├── core/
│   │   ├── config.py             NotificationSettings (§3.1.1.6)
│   │   ├── db.py  logger.py  dependencies.py  errors.py  schemas.py  utils.py
│   ├── health/routes.py          /health, /health/ready
│   ├── notifications/
│   │   ├── models.py             Notification, DeliveryAttempt
│   │   ├── repo.py               NotificationRepo: list, mark-read, read-all, unread count, admin view
│   │   ├── recording_repo.py     event → recipient → type → channels → scope → rows (called by consumers/)
│   │   ├── delivery_report_repo.py  ack: delivered_at stamping; calls stream/cursor_repo.py for the clamped advance
│   │   ├── promotion_repo.py     the origin_first promoter (called by tasks/)
│   │   ├── dispatch_repo.py      attempt rows and the adapter call (called by tasks/)
│   │   ├── dependencies.py
│   │   ├── enums.py              NotificationType, NotificationChannel, DeliveryStatus, NotificationDeliveryScope
│   │   ├── utils.py              scope derivation, channel table, DeliveryStatus transition table; no I/O
│   │   ├── templates/            one module per NotificationType
│   │   ├── consumers/            thin handlers per topic → recording_repo.py
│   │   ├── tasks/                Celery task bodies (hosted by celery-worker): dispatch, promote, redispatch → *_repo.py
│   │   ├── adapters/
│   │   │   ├── base.py           NotificationAdapter protocol
│   │   │   ├── email_log.py      logging email adapter
│   │   │   ├── sms_log.py        logging SMS adapter
│   │   │   └── web_push.py       specified, not registered
│   │   └── v1/
│   │       ├── routes.py         list, read, read-all, unread-count, ack, admin
│   │       └── schemas.py
│   ├── subscriptions/
│   │   ├── models.py             PushSubscription (specified, unused)
│   │   └── repo.py               PushSubscriptionRepo, read by adapters/web_push.py; no v1/ until an endpoint exists
│   └── stream/
│       ├── models.py             StreamCursor
│       ├── cursor_repo.py        cold start, greatest() upsert with clamp, last_seen_at
│       ├── tickets.py            mint / redeem (GETDEL)
│       ├── catchup.py            floor, bounded query, truncation
│       ├── fanout.py             PUBLISH / SUBSCRIBE, message schema
│       ├── connection.py         per-connection queue, heartbeat, presence, cap
│       ├── frames.py             typed frame serialisers
│       ├── utils.py              per-connection frame filter; no I/O
│       ├── tasks/                sweep_stale_stream_cursors (hosted by celery-worker) → cursor_repo.py
│       └── v1/
│           ├── routes.py         stream-ticket, stream (HTTP only; delegates to the modules above)
│           └── schemas.py
└── tests/  unit/<entity>/  integration/<entity>/
```

Both entity routers mount under `/notifications`. Literal segments (`/unread-count`, `/read-all`, `/ack`, `/admin`, `/stream-ticket`, `/stream`) are declared before `/{id}` in their `v1/routes.py`, and `app/router.py` includes `stream/v1` before `notifications/v1` so the stream literals are never captured by `/{id}`.

##### 3.1.1.2. Domain model

**Entities.**

| Entity | Aggregate | Identity | Notes |
| --- | --- | --- | --- |
| Notification | root | `id` (`uuid`); ordered by `seq` | One per `(event, user, channel)`. Rendered once, at consume time; `title` and `body` never change afterwards |
| Delivery attempt | child of Notification | `id` | One per adapter call; `attempt` is 1-based |
| Stream cursor | root | unique `(user_id, client_id)` | The device's replay floor; doubles as a device registry, but its job is the cursor |
| Push subscription | root | unique `(user_id, endpoint)` | Specified, unused |

**Invariants.**

| # | Invariant | Enforced by |
| --- | --- | --- |
| I1 | At most one notification per `(event_id, user_id, channel)` | NTF-1 unique index; `INSERT … ON CONFLICT DO NOTHING`; behind it, `processed_events` (C4) |
| I2 | `delivery_scope` is `all_devices` whenever `origin_client_id IS NULL` — no exception for types whose default is narrower | The scope-derivation function; unit-tested over all twelve `type` × origin-presence combinations |
| I3 | `delivered_client_id IS NULL` iff `delivered_at IS NULL`; `delivered_at` is written once (first writer wins) | `SET delivered_at = coalesce(delivered_at, now())` guarded by `delivered_at IS NULL` |
| I4 | `promoted_at` is only ever set on an `origin_first` row, at most once | The promoter's `WHERE delivery_scope = 'origin_first' AND promoted_at IS NULL`; NTF-9 at insert |
| I5 | `stream_cursors.cursor_seq` never decreases, and never exceeds the newest settled `seq` for the user | `greatest()` upsert with the 5-second clamp (§3.1.1.4) |
| I6 | Unread count and the notification list are never scope- or cursor-filtered | Code review; G5d; the risk register in §8 |
| I7 | `client_id` is always stored and queried under an authenticated `user_id` | Every query takes `user_id` from the verified token or the redeemed ticket |
| I8 | A read notification stays read; `read_at` is written once | `SET read_at = coalesce(read_at, now())` |

**Vocabularies** (S4; `app/v1/notifications/enums.py`, imported by `stream/` for the scope filter).

| `DomainEnum` | Values | Column(s) | Read policy |
| --- | --- | --- | --- |
| `NotificationType` | `order_confirmed`, `order_failed`, `order_cancelled`, `shipment_update`, `stockout`, `welcome` | `notifications.type` | `passthrough` — displayed, not branched on by readers. The vocabulary most likely to grow in this platform |
| `NotificationChannel` | `in_app`, `email`, `sms`, `push` | `notifications.channel`, `delivery_attempts.channel` | `passthrough` (archive/0006 names `notifications.channel` as its example). `push` is a vocabulary value with no registered adapter; see NTF-Q5 on the schema note that lists only three |
| `DeliveryStatus` | `pending`, `sent`, `failed` | `delivery_attempts.status` | `fallback` → `pending` (a state machine reads it) |
| `NotificationDeliveryScope` | `origin_only`, `origin_first`, `all_devices` | `notifications.delivery_scope` | `fallback` → `all_devices` — the safe value, because scope narrows delivery and the default must not |

**Event → notification.** Recipient, type and channel set (NTF-3, NTF-7).

| Consumed event | Condition | Recipient (`user_id`) | `NotificationType` | Channels |
| --- | --- | --- | --- | --- |
| `identity.user.registered` | always | `payload.user_id` | `welcome` | `in_app`, `email` |
| `order.confirmed` | always | `payload.customer_id` | `order_confirmed` | `in_app`, `email` |
| `order.failed` | always | `payload.customer_id` | `order_failed` | `in_app`, `email` |
| `order.cancelled` | always | `payload.customer_id` | `order_cancelled` | `in_app`, `email` |
| `shipment.status_changed` | new status `shipped`, `in_transit`, `delivered` | `payload.customer_id` | `shipment_update` | `in_app`, `email` |
| `shipment.status_changed` | new status `created` | — | none (recorded in `processed_events` only) | — |
| `inventory.stockout` | always | `payload.merchant_id` | `stockout` | `in_app` |
| `payment.succeeded` / `payment.failed` | always | — | none — the outcome is carried by `order.confirmed` / `order.failed` (NTF-7) | — |

`sms` is in the vocabulary and has an adapter, but no type selects it by default: with no preferences there is no user who asked for it.

**Delivery scope per `NotificationType` (C21).** Absent `origin_client_id` coerces to `all_devices` unconditionally, with no exception for types whose default is narrower.

| Type | Scope | Why | When the origin client is not connected |
| --- | --- | --- | --- |
| `order_confirmed` | `origin_first` | The tab that submitted the checkout is where the user is waiting. But a confirmation the user never sees is a support ticket | Promoted to `all_devices` once |
| `order_failed` | `origin_first` | Same, and it is money. This is the case that justifies the promoter existing | Promoted — highest priority |
| `order_cancelled` | `all_devices` | Frequently *not* user-initiated: the reservation sweeper, an admin, a merchant. Even when it is, the user may have moved devices | n/a |
| `shipment_update` | `all_devices` | Originates from a carrier webhook; there is no origin client to speak of | n/a |
| `stockout` | `all_devices` | Merchant-facing, triggered by somebody else's order entirely | n/a |
| `welcome` | `origin_only` | Only meaningful in the session that just registered. A welcome toast arriving on a second device two days later is noise, and it is already in the list | Nothing. It expires into the list, which is the correct outcome |

Scope applies to the `in_app` row only; dispatched channels have no devices and ignore it (the column is still written, with the same derivation, so the rule has one implementation).

**Templates.** Rendered at consume time into `title` and `body`; the full envelope is kept in `payload` so a template change never rewrites history and support can see what was rendered from. Templates are plain Python format functions per type — no template engine, because there is no HTML (email bodies are logged as text).

| Type | `title` | `body` (fields from `payload`) |
| --- | --- | --- |
| `welcome` | "Welcome to SmartRetail" | "Hi {full_name}, your {role} account is ready." |
| `order_confirmed` | "Order confirmed" | "Order {order_number} for {total} {currency} is confirmed." plus "Tracking number: {tracking_number}." when present |
| `order_failed` | "Order could not be completed" | "Order {order_number} failed: {failure_reason_text}." then "You have not been charged." when `payment_status` is `none`, `pending` or `failed`, or "Your payment has been refunded." when it is `refunded` |
| `order_cancelled` | "Order cancelled" | "Order {order_number} was cancelled{by_whom}." plus the refund sentence when `payment_status` is `refunded` |
| `shipment_update` | "Your order has shipped" / "…is on its way" / "…has been delivered" (by `to_status`) | "Order {order_number} is now {status}{ (tracking {tracking_number})}." |
| `stockout` | "Out of stock" (`depleted`, `reservation_refused`) / "Running low on stock" (`low_stock`) | "{sku} is out of stock." / "{sku} is running low: {available} left (threshold {low_stock_threshold})." / "{sku} could not cover an order: {requested} requested, {available} available." |

*Amended Oct 15, 2026 (NTF-D33):* the order is referred to by its `order_number` (the customer-facing reference every order event carries) rather than the first eight characters of its id, and `stockout` renders the `sku` because `inventory.stockout` carries no product name. `failure_reason_text` maps order's `OrderFailureReason` code (RFC 0005 ORD-5) to a phrase; an unknown code renders as the code, never fails the render. A template that raises is a poison message (C6) — it goes to DLQ, and the render functions are unit-tested per type so that never happens in practice.

##### 3.1.1.3. State machines

**Notification lifecycle (derived, NTF-13).** There is no status column. The lifecycle below is the set of facts a row can accumulate; each is written by a different actor, and they are independent except where a guard says otherwise.

| From | To | Written | Triggered by | Guard |
| --- | --- | --- | --- | --- |
| — | **recorded** | row inserted | `notification` consumer | at-most-one (I1); `processed_events` miss |
| — | **recorded + promoted** | row inserted with `promoted_at = now()` | consumer, NTF-9 | `delivery_scope = 'origin_first'` and presence key absent |
| recorded | **promoted** | `promoted_at` | `promote_undelivered_notifications` (Beat, 15 s) | `delivery_scope = 'origin_first'`, `delivered_at IS NULL`, `promoted_at IS NULL`, created between 45 s and 1 h ago |
| recorded / promoted | **delivered** | `delivered_at`, `delivered_client_id` | `POST /notifications/ack` naming the `id` | `delivered_at IS NULL`; `user_id` matches the caller |
| any | **read** | `read_at` | `POST /{id}/read`, `POST /read-all` | `read_at IS NULL`; owner |

Notes: a row may be *read* without ever being *delivered* (read from the list on a device that never streamed it) — that is correct and is why the two are separate facts. A *delivered* row is never *promoted* afterwards. `origin_only` and `all_devices` rows never enter *promoted*. No transition goes backwards; nothing clears a timestamp.

The three facts are independent, so the diagram draws them as concurrent regions rather than one chain. Not shown, because it is a guard rather than a state: a row that is already *delivered* is never *promoted* afterwards, and only `origin_first` rows enter the promotion region at all.

```mermaid
stateDiagram-v2
    [*] --> Notification : consumer inserts the row (I1)
    state Notification {
        [*] --> not_promoted
        [*] --> promoted : inserted with promoted_at, origin_first and presence absent (NTF-9)
        not_promoted --> promoted : promote_undelivered_notifications, origin_first, undelivered, 45 s to 1 h old
        --
        [*] --> undelivered
        undelivered --> delivered : POST /notifications/ack names the id, first writer wins
        --
        [*] --> unread
        unread --> read : POST read or read-all, owner only
    }
```

**Delivery attempt** (`DeliveryStatus`, dispatched channels only, NTF-4).

| From | To | Triggered by | Guard |
| --- | --- | --- | --- |
| — | `pending` | `dispatch_notification`, before calling the adapter | no `sent` attempt exists for the notification; `attempt = max(attempt) + 1` |
| `pending` | `sent` | adapter returned success | — |
| `pending` | `failed` | adapter raised or returned an error; `error` recorded | — |

`sent` and `failed` are terminal *for the attempt row*. A retry inserts attempt *n + 1* as a new `pending` row rather than reopening a failed one, so the history is append-only. After the fifth failure (NTF-5) no further row is written; the notification's effective state is "dispatch exhausted", visible in the admin view as a notification whose newest attempt is `failed`. A `pending` row older than the task's hard time limit is a crashed attempt: the next retry, or the NTF-6 sweep, supersedes it with attempt *n + 1*.

```mermaid
stateDiagram-v2
    [*] --> pending : dispatch_notification inserts attempt n + 1 (no sent attempt exists)
    pending --> sent : adapter returned Sent
    pending --> failed : adapter raised or returned Failed, error recorded
    sent --> [*]
    failed --> [*] : retry writes a new pending row, max 5 (NTF-5)
```

**Stream cursor.**

| From | To | Triggered by | Guard |
| --- | --- | --- | --- |
| — | exists, `cursor_seq = max(seq)` for the user | stream open, cold start | no row for `(user_id, client_id)`; `client_id` not null |
| — | exists, `cursor_seq = clamp(up_to_seq)` | ack from a `client_id` with no row | upsert |
| exists | exists, `cursor_seq` raised | ack | `greatest()`, clamped (I5) |
| exists | exists, `last_seen_at` refreshed | stream open, ack | — |
| exists | deleted | `sweep_stale_stream_cursors` | `last_seen_at` older than 90 days, or beyond the 20 newest for the user |

```mermaid
stateDiagram-v2
    [*] --> exists : stream open cold start, cursor_seq = max(seq), client_id not null
    [*] --> exists : ack from a client_id with no row, cursor_seq = clamp(up_to_seq)
    exists --> exists : ack raises cursor_seq with greatest() and the clamp (I5)
    exists --> exists : stream open or ack refreshes last_seen_at
    exists --> [*] : sweep_stale_stream_cursors, idle 90 days or beyond the 20 newest
```

**Stream connection** (in memory, per replica — not persisted).

| From | To | Trigger |
| --- | --- | --- |
| — | `opening` | `GET /stream` with a ticket |
| `opening` | rejected | ticket invalid, expired or spent (`401`); fourth concurrent stream (`429`) |
| `opening` | `catching_up` | ticket redeemed; slot claimed; `SUBSCRIBE` issued (NTF-10) |
| `catching_up` | `live` | `hello` (and `catchup_truncated`) sent; replay and buffered live messages drained |
| `live` | `closing` | JWT expiry reached (`error` frame), queue overflow, client disconnect, replica shutdown (`retry:` hint) |
| `closing` | — | `UNSUBSCRIBE`; `DECR nt:conns:{user_id}`; `DEL nt:client:{user_id}:{client_id}` on clean close |

```mermaid
stateDiagram-v2
    [*] --> opening : GET /stream with a ticket
    opening --> rejected : ticket invalid, expired or spent (401), fourth concurrent stream (429)
    rejected --> [*]
    opening --> catching_up : ticket redeemed, slot claimed, SUBSCRIBE issued (NTF-10)
    catching_up --> live : hello and catchup_truncated sent, replay and buffer drained
    live --> closing : JWT expiry, queue overflow, client disconnect, replica shutdown
    closing --> [*] : UNSUBSCRIBE, DECR nt-conns, DEL presence key on clean close
```

##### 3.1.1.4. Workflows and async jobs

`notification` owns no Temporal workflow. It owns one Kafka consumer, the SSE stream handler, and four Celery tasks, all hosted by the platform ([RFC 0001](0001-platform-and-shared-infrastructure.md)).

###### The consumer

Consumer group `notification`, one process per replica subscribing to every consumed topic (archive/0003 Q2: per service, not per topic). Handler, per event, using the `common` consumer base class:

```
 restore traceparent from envelope (C11); open span "notification.consume {event_type}"
 BEGIN
   INSERT processed_events (consumer_group='notification', event_id, event_type, processed_at)
     ON CONFLICT DO NOTHING
   if no row inserted → COMMIT; commit offset; return                      -- C4 dedupe hit
   resolve (recipient, type, channels) from the §3.1.1.2 table; none → COMMIT; commit offset; return
   origin := envelope.client_id                                            -- may be null
   scope  := origin is null ? 'all_devices' : default_scope(type)          -- I2
   promoted_at := null
   if scope = 'origin_first' and not EXISTS nt:client:{recipient}:{origin} -- NTF-9
        promoted_at := now()
   for channel in channels:
     render (title, body) from template(type, envelope.payload)
     INSERT notifications (…, payload = envelope, origin_client_id = origin,
                           delivery_scope = scope, promoted_at, correlation_id = envelope.correlation_id)
       ON CONFLICT ((payload->>'event_id'), user_id, channel) DO NOTHING   -- I1, NTF-1
       RETURNING id, seq
 COMMIT
 after commit:
   for the in_app row, if inserted:
     unread := count(in_app, read_at IS NULL) for recipient               -- NTF-11
     PUBLISH nt:user:{recipient} {kind:"notification", id, seq, type, title, body, created_at,
                                  origin_client_id, scope: promoted_at ? 'all_devices' : scope,
                                  unread_count: unread}
   for each dispatched-channel row inserted: dispatch_notification.delay(id)
 commit offset
```

A `PUBLISH` failure after commit is logged and counted (`notification_fanout_publish_errors_total`) and never fails the event: the row is durable and the next connect replays it (C16, C18). An enqueue failure is likewise logged and counted; NTF-6 recovers it. A render or recipient-resolution error is a poison message: three in-process attempts, then `<topic>.dlq`, then offset commit (C6). An infrastructure error pauses the partition (NTF-14).

**Decision flow per event.**

```mermaid
flowchart TD
    start(["event from Kafka"]) --> dedupe{"INSERT processed_events<br/>inserted a row?"}
    dedupe -- "no, duplicate (C4)" --> commitOnly["COMMIT, commit offset"]
    dedupe -- yes --> resolve{"event maps to a NotificationType?<br/>(§3.1.1.2 table)"}
    resolve -- "no: payment.*, shipment created (NTF-7)" --> commitOnly
    resolve -- yes --> origin{"envelope.client_id present?"}
    origin -- no --> all["scope = all_devices (I2)"]
    origin -- yes --> def["scope = default_scope(type)"]
    def --> first{"scope = origin_first?"}
    first -- no --> insert
    first -- yes --> presence{"nt:client presence key<br/>for the origin exists?"}
    presence -- yes --> insert
    presence -- "no (NTF-9)" --> promote["promoted_at = now()"] --> insert
    all --> insert["per channel: render template,<br/>INSERT ... ON CONFLICT DO NOTHING (I1)"]
    insert --> commit["COMMIT"]
    commit --> inapp{"in_app row inserted?"}
    inapp -- yes --> publish["count unread, PUBLISH nt:user (NTF-11)"]
    inapp -- no --> enqueue
    publish --> enqueue["dispatch_notification.delay for each<br/>dispatched-channel row inserted"]
    enqueue --> offset["commit offset"]
    insert -. "render or recipient error" .-> poison["3 attempts, then topic.dlq (C6)"]
    insert -. "DB or Redis outage" .-> pause["pause partition, capped backoff (NTF-14)"]
```

**Consume to frame, across replicas** (C17, G5a). The replica that consumes is usually not the one holding the connection.

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka
    participant B as Replica B consumer
    participant DB as notification_db
    participant R as Redis
    participant MQ as RabbitMQ
    participant A as Replica A stream
    participant C as Client EventSource
    K->>B: order.confirmed envelope (traceparent, client_id)
    B->>DB: BEGIN, INSERT processed_events
    B->>R: EXISTS nt:client presence key (origin_first only)
    B->>DB: INSERT notifications per channel ON CONFLICT DO NOTHING
    B->>DB: COMMIT
    B->>DB: count unread in_app rows
    B->>R: PUBLISH nt:user:{id} with scope and unread_count
    B->>MQ: dispatch_notification.delay(email row id)
    B->>K: commit offset
    R-->>A: message on nt:user:{id}
    A->>A: per-connection scope filter
    A-->>C: notification frame (id = seq) and unread_count frame
```

###### The SSE stream

**Opening a stream.**

```
POST /api/v1/notifications/stream-ticket     Authorization: Bearer <jwt>
                                             X-Client-Id: b3f1…
  → { "ticket": "…", "expires_in": 60 }      single-use; Redis holds
                                             {user_id, client_id, jwt_exp}

GET  /api/v1/notifications/stream?ticket=…   Last-Event-ID: 84213   (often absent)
  1. redeem ticket (atomic GETDEL nt:ticket:{ticket}) → {user_id, client_id, jwt_exp}, or 401
  2. claim a slot: INCR nt:conns:{user_id}, EXPIRE 60; > 3 → DECR, 429
  3. SUBSCRIBE nt:user:{user_id}; buffer live messages into the connection queue (NTF-10)
  4. SELECT … FROM stream_cursors WHERE user_id = ? AND client_id = ?
     client_id null → EPHEMERAL (NTF-12): floor = Last-Event-ID, or cold start if absent; no row written
     no row  → COLD START: insert at the user's current max(seq);
               emit hello {cold_start: true, unread_count}; replay NOTHING
     row     → RESUME: floor = greatest(cursor_seq, coalesce(Last-Event-ID, 0));
               bounded catch-up (below); emit hello, then catchup_truncated if hit, then frames
     either way: refresh last_seen_at (and user_agent)
  5. drain buffered live messages, skipping ids already sent; → live
  6. SET nt:client:{user_id}:{client_id} 1 EX 45   (presence, refreshed by the heartbeat; not for ephemeral streams)
  7. `: keepalive` comment and a `ping` frame every 20s; each refreshes nt:client:* (EX 45) and nt:conns:* (EX 60)
  8. at jwt_exp: emit error frame (type …/stream-token-expired) and close
```

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant G as Gateway
    participant N as notification
    participant R as Redis
    participant DB as notification_db
    C->>G: POST /api/v1/notifications/stream-ticket (Bearer, X-Client-Id)
    G->>N: forward, X-Client-Id shape-checked or stripped
    N->>R: SET nt:ticket:{ticket} user_id, client_id, jwt_exp EX 60
    N-->>C: 200 ticket, expires_in 60
    C->>G: GET /api/v1/notifications/stream?ticket=... (Last-Event-ID optional)
    G->>N: forward, unbuffered
    N->>R: GETDEL nt:ticket:{ticket}
    alt ticket missing, expired or spent
        N-->>C: 401 problem detail
    end
    N->>R: INCR nt:conns:{user_id}, EXPIRE 60
    alt more than 3 streams
        N->>R: DECR nt:conns:{user_id}
        N-->>C: 429 problem detail
    end
    N->>R: SUBSCRIBE nt:user:{user_id}, buffer live messages (NTF-10)
    N->>DB: SELECT stream_cursors for (user_id, client_id)
    alt no row (cold start)
        N->>DB: INSERT cursor at the user's max(seq)
        N-->>C: hello cold_start true, unread_count
    else row exists (resume)
        N->>DB: bounded catch-up, LIMIT 51
        N-->>C: hello, catchup_truncated if 51 rows, then up to 50 frames
    end
    N-->>C: drained buffered frames, skipping ids already sent
    N->>R: SET nt:client presence key EX 45
    loop every 20 s
        N-->>C: keepalive comment and ping frame
        N->>R: refresh presence (EX 45) and nt:conns (EX 60)
    end
    N-->>C: error frame stream-token-expired at jwt_exp, then close
```

**Choosing the replay floor.**

```mermaid
flowchart TD
    open(["ticket redeemed, SUBSCRIBE issued"]) --> cid{"ticket client_id null?"}
    cid -- "yes (NTF-12)" --> eph{"Last-Event-ID sent?"}
    eph -- yes --> ephFloor["EPHEMERAL: floor = Last-Event-ID,<br/>no cursor row"]
    eph -- no --> cold
    cid -- no --> row{"stream_cursors row exists?"}
    row -- no --> cold["COLD START: hello cold_start true,<br/>replay nothing"]
    row -- yes --> resume["RESUME: floor = greatest(cursor_seq, Last-Event-ID or 0)"]
    ephFloor --> q
    resume --> q["catch-up: in_app, last 24 h, seq above floor or last 5 s,<br/>scope filter, ORDER BY seq LIMIT 51"]
    q --> n{"51 rows?"}
    n -- yes --> trunc["hello, catchup_truncated with omitted_count,<br/>then the oldest 50"]
    n -- no --> all["hello, then every row"]
    cold --> drain
    trunc --> drain["drain buffer, skip ids already sent, live"]
    all --> drain
```

**The bounded catch-up query (C22).** The 5-second overlap window is the `seq` visibility trap's fix; the age bound, the scope filter, and the limit are the bound.

```sql
SELECT id, seq, type, title, body, created_at, origin_client_id, delivery_scope
  FROM notifications
 WHERE user_id = :user_id
   AND channel = 'in_app'                                -- NTF-2
   AND created_at > now() - interval '24 hours'          -- hard age bound
   AND ( seq > :floor
         OR created_at > now() - interval '5 seconds' )  -- the visibility window
   AND ( delivery_scope = 'all_devices'
         OR origin_client_id IS NULL
         OR origin_client_id = :client_id
         OR promoted_at IS NOT NULL )                    -- scope filter (C21)
 ORDER BY seq
 LIMIT 51;                                               -- 50 + 1 to detect truncation
```

When 51 rows come back, the handler sends the **oldest 50** and emits `catchup_truncated` — never the newest 50, because a hole *below* the sent range is worse than a delay above it, and sending the oldest lets the client's cursor advance contiguously into the remainder it pages from `GET /notifications?since_seq=`. `omitted_count` is a second, bounded `count(*)` over the same predicate without the `LIMIT`.

**The frame contract.** Typed events, matching the style [RFC 0009](0009-assistant-service.md) E5 sets for the assistant stream.

| Event | Payload | Notes |
| --- | --- | --- |
| `hello` | `{client_id, cold_start, unread_count, server_seq, resumed_from}` | Always first. `cold_start: true` tells the client to fetch the list rather than expect a backlog (C22). `server_seq` is the user's current max `seq`; `resumed_from` is the floor used, or null on cold start |
| `notification` | `{id, seq, type, title, body, created_at, origin}` | `id:` is set to `seq` — this is what the browser echoes back. `origin: true` when this client is the originating device, so the UI can show an inline confirmation rather than a generic toast |
| `catchup_truncated` | `{omitted_count, oldest_seq_sent}` | Emitted before the replayed frames when the 50/24h bound was hit |
| `unread_count` | `{count}` | Sent on connect and after each notification, so the badge needs no second request. **Never scope-filtered** (C21) — it is delivered to every connection of the user even when the `notification` frame is filtered out |
| `ping` | — | Heartbeat; also the liveness signal for connection reaping, and what refreshes the presence key |
| `error` | RFC 9457 problem detail | Emitted before close, so a stream never simply stops without explanation |

Wire example:

```
retry: 4211
event: hello
data: {"client_id":"b3f1…","cold_start":false,"unread_count":3,"server_seq":84231,"resumed_from":84213}

id: 84214
event: notification
data: {"id":"5c0e…","seq":84214,"type":"order_confirmed","title":"Order confirmed","body":"…","created_at":"…","origin":true}

event: unread_count
data: {"count":3}

: keepalive
```

**Per-connection filter.** A delivering replica, for each Pub/Sub message on `nt:user:{user_id}` and each of its connections for that user:

```
deliver notification frame  iff  scope = 'all_devices'
                                 or origin_client_id is null
                                 or (connection.client_id is not null and origin_client_id = connection.client_id)
always deliver unread_count frame
```

```mermaid
flowchart LR
    msg(["Pub/Sub message on nt:user"]) --> kind{"kind = unread_count?"}
    kind -- yes --> uc["deliver unread_count frame<br/>(never scope-filtered)"]
    kind -- "no, notification" --> s1{"scope = all_devices?"}
    s1 -- yes --> deliver["deliver notification frame,<br/>then unread_count frame"]
    s1 -- no --> s2{"origin_client_id null?"}
    s2 -- yes --> deliver
    s2 -- no --> s3{"connection client_id not null<br/>and equals origin_client_id?"}
    s3 -- yes --> deliver
    s3 -- no --> skip["filter the notification frame,<br/>still deliver unread_count"]
```

A promoted re-publish arrives with `scope = 'all_devices'`; the origin device, if it already rendered the targeted frame, receives it again and de-duplicates by `id`.

**Promotion.** `promote_undelivered_notifications` runs on Celery Beat every 15 s rather than as a per-notification countdown task, which would be a task storm and a scheduling dependency for a cosmetic outcome:

```sql
UPDATE notifications SET promoted_at = now()
 WHERE delivery_scope = 'origin_first'
   AND delivered_at IS NULL AND promoted_at IS NULL
   AND created_at < now() - interval '45 seconds'
   AND created_at > now() - interval '1 hour'     -- bounded scan
RETURNING …            → re-PUBLISH each with scope = 'all_devices'
```

Idempotent by `promoted_at IS NULL`, bounded by the one-hour floor, safe on two workers. **The worst-case toast delay is arithmetic, not a guess:** heartbeat 20 s + presence TTL 45 s + promotion grace 45 s + sweep interval 15 s ≈ **75 seconds** for a laptop whose lid closes immediately after the event; sub-second in the common case. If that is judged too slow for `order_failed`, the lever is a 30 s presence TTL with a 12 s heartbeat, not a redesign.

```mermaid
sequenceDiagram
    autonumber
    participant Beat as Celery Beat
    participant W as celery-worker
    participant DB as notification_db
    participant R as Redis
    participant N as notification replicas
    participant D as Other devices
    Beat->>W: promote_undelivered_notifications (every 15 s)
    W->>DB: UPDATE promoted_at WHERE origin_first, undelivered, unpromoted, 45 s to 1 h old RETURNING
    loop each promoted row
        W->>DB: count unread for the user
        W->>R: PUBLISH nt:user:{id} scope all_devices, unread_count
        R-->>N: message
        N-->>D: notification frame (origin device de-duplicates by id)
    end
```

**The `seq` visibility trap.** ⚠️ `bigserial` values are allocated before commit, so a transaction holding `seq` 84214 can commit *after* one holding 84215. A client that catches up to 84215 would then skip 84214 permanently — a silent, unreproducible loss. The catch-up query therefore reads a small overlap window, `seq > :cursor OR created_at > now() - interval '5 seconds'`, and the client de-duplicates by `id`. Cheap, and it removes the one genuinely nasty bug in this design.

The ack endpoint must not defeat it. A client that acks `seq` 84215 while 84214 is still uncommitted would, with a naive `cursor_seq = 84215`, skip 84214 permanently once the overlap window has passed — turning a transient trap into a permanent one. So `cursor_seq` is clamped to the newest row that is definitely settled:

```sql
cursor_seq := greatest(cursor_seq,
                       least(:acked_up_to_seq,
                             (SELECT coalesce(max(seq), 0) FROM notifications
                               WHERE user_id = :uid
                                 AND created_at < now() - interval '5 seconds')))
```

Frames above the clamp were still *delivered* — `delivered_at` is stamped by explicit `id`, never by range, so a row a device never saw is never marked as though it had been — they are simply re-offered on the next connect and de-duplicated client-side by `id`, which the client already does.

**Where `client_id` comes from, and the gap it has to cross.** `order.confirmed` and `order.failed` are not emitted by the HTTP handler — checkout returns `202` and the Temporal workflow emits them later, on a worker, with no request context. The `client_id` therefore has to be *persisted* to survive the gap: `order_db.orders.origin_client_id` is written at order creation and read by every activity that writes an outbox row ([RFC 0005](0005-order-service.md#orders)). Without that column, the two notification types that most need origin targeting are the two that cannot have it. `identity.user.registered` carries the registration request's `X-Client-Id` directly; carrier-webhook, scheduler and sweeper events carry `null`.

**Connection hygiene.**

| Concern | Handling |
| --- | --- |
| Idle proxies closing connections | `: keepalive` comment every 20 s |
| Slow or dead clients | Bounded per-connection queue (100 messages); on overflow the connection is closed rather than buffered. The client reconnects and catches up by `seq`, so closing is lossless |
| Reconnect storms after a deploy | `retry:` with jitter, plus a cap of 3 concurrent streams per user |
| A fresh device with no cursor | Cold start (C22): zero replayed frames, a `hello` frame with the unread count, and live frames from that point. The alternative — replaying a lifetime of notifications as live frames — is the failure this design exists to remove |
| A client that never acks | Supported. Its cursor never advances, so every reload is a cold start and every `origin_first` notification promotes to broadcast after the grace period. That is precisely the pre-ack behaviour, which is why `delivered_at` gates nothing (C23) |
| Graceful shutdown | Streams closed with a `retry:` hint before the pod exits, spreading reconnects |
| Capacity | An idle SSE connection is an asyncio task and a socket. ~10k per replica with `uvloop` and a raised `ulimit -n`; [Part A](../part-a.md) §4's "tens of thousands of concurrent users" is 2–4 replicas |

**The offline half.** SSE covers "the app is open". It is not the whole answer, and the RFC does not pretend otherwise:

| Client state | Mechanism | Status |
| --- | --- | --- |
| Connected | SSE frame, sub-second | **In scope** |
| Backgrounded or closed | Web Push / FCM via the `dispatch_notification` Celery task and the `push` adapter | Out of scope (§2.2); the `push` channel value and `push_subscriptions` are specified so it is an adapter swap |
| Returning later | `GET /api/v1/notifications` — the durable list | In scope |
| Cannot hold a stream (SSE blocked) | `GET /api/v1/notifications/unread-count` on a timer | The client's fallback, not the design (archive/0003 §5, Alternative 9) |

###### Channel adapters

```python
class NotificationAdapter(Protocol):
    channel: NotificationChannel
    async def send(self, notification: NotificationView) -> AdapterResult: ...
    # AdapterResult = Sent(provider_ref: str | None) | Failed(error: str, retryable: bool)
```

| Adapter | Channel | Registered | Behaviour |
| --- | --- | --- | --- |
| *(none — SSE)* | `in_app` | n/a | Delivered by the stream; no Celery task, no `delivery_attempts` (NTF-4) |
| `EmailLogAdapter` | `email` | yes | Renders a text email (`to` = the user id, since `notification` holds no addresses; `subject` = `title`; `body`) and writes it as one structured log line at `INFO` with `notification_id`, `trace_id`. Returns `Sent`. Can be told to fail by `NOTIFICATION_FAKE_EMAIL_FAILURE_RATE` for the retry tests, mirroring A9's fake providers |
| `SmsLogAdapter` | `sms` | yes | As above, `body` truncated to 160 characters |
| `WebPushAdapter` | `push` | **no** | Specified: for each `push_subscriptions` row of the user, send an encrypted Web Push message (`pywebpush`, VAPID keys from settings) carrying `{id, type, title}` only — the body is fetched by the client from the list, so no content sits in a third-party push service. `404`/`410` from the push service deletes the subscription; other errors are `Failed(retryable=True)`. Registering it is one line in the adapter registry plus the VAPID settings |

A real email provider needs a user email address, which `identity` owns: it would arrive either as a snapshot in `identity.user.registered` (a projection `notification` would keep) or as a claim, and that choice belongs to the RFC that introduces the provider.

###### Celery tasks

| Task | Queue | Trigger | Body |
| --- | --- | --- | --- |
| `dispatch_notification(notification_id)` | `notifications` | Consumer, after commit; NTF-6 sweep | Load the row; if a `sent` attempt exists → return (idempotent). Insert attempt *n + 1* `pending`; call the channel's adapter; update to `sent` or `failed` + `error`. On `Failed(retryable)` → `self.retry(countdown=backoff(n))`, max 5 (NTF-5). `acks_late=True`, soft time limit 30 s, hard 60 s |
| `promote_undelivered_notifications` | `notifications` | Beat, every 15 s | The promotion `UPDATE … RETURNING`, then re-`PUBLISH` each row with `scope = 'all_devices'` and a fresh `unread_count` (C21) |
| `sweep_stale_stream_cursors` | `maintenance` | Beat, daily | Delete `stream_cursors` rows with `last_seen_at` older than 90 days; then, per user with more than 20 rows, delete all but the 20 newest by `last_seen_at` |
| `redispatch_undispatched_notifications` (NTF-6) | `maintenance` | Beat, every 5 min | For rows with `channel <> 'in_app'`, no `delivery_attempts` row, created between 2 minutes and 24 hours ago → `dispatch_notification.delay(id)`. Also re-enqueues rows whose newest attempt is `pending` and older than the hard time limit, while fewer than 5 attempts exist |

Task bodies live with the entity whose rows they write: the first two and the redispatch sweep in `notifications/tasks/`, the cursor sweep in `stream/tasks/` (NTF-D25).

Separate queues so a burst of media processing cannot starve notification delivery ([RFC 0001](0001-platform-and-shared-infrastructure.md) owns routing). The `processed_events` retention sweep is shared infrastructure ([RFC 0000](0000-architecture.md#3132-shared-tables--outbox-and-processed_events)).

**Dispatch, with its two triggers** (NTF-5, NTF-6):

```mermaid
sequenceDiagram
    autonumber
    participant Src as Consumer or redispatch sweep
    participant MQ as RabbitMQ notifications queue
    participant W as celery-worker
    participant DB as notification_db
    participant Ad as Channel adapter
    Src->>MQ: dispatch_notification(notification_id)
    MQ->>W: deliver task (acks_late)
    W->>DB: load row, any sent attempt?
    alt a sent attempt exists
        W-->>MQ: ack, return (idempotent)
    else none sent
        W->>DB: INSERT delivery_attempts attempt n + 1, pending
        W->>Ad: send(notification)
        alt Sent
            Ad-->>W: Sent(provider_ref)
            W->>DB: UPDATE attempt to sent
        else Failed retryable
            Ad-->>W: Failed(error, retryable)
            W->>DB: UPDATE attempt to failed, error
            W->>MQ: self.retry(countdown = backoff(n)), max 5
        end
    end
```

##### 3.1.1.5. Dependencies and degradation

**Synchronous calls in.** Only the gateway, on `/api/v1/notifications/*`. **Synchronous calls out.** None (R4, R5, R7).

| Dependency | Used for | When it is down |
| --- | --- | --- |
| `notification_db` (PostgreSQL 16) | Everything durable | Read endpoints and ack return `503`; stream opens fail with `503`; open streams stay open on heartbeats but deliver nothing new; the consumer pauses and lags (NTF-14). No event is lost — Kafka retains it |
| Kafka + Schema Registry | Consuming facts | Nothing new is recorded; everything already recorded is served. Producers' outbox rows accumulate and publish on recovery. Checkout is unaffected |
| Redis | Tickets, fan-out, presence, connection cap, ack rate limit | `stream-ticket` returns `503`, so no new streams open; clients fall back to polling `unread-count` and the list, which do not touch Redis. Existing streams lose live frames and recover by catch-up on reconnect. `PUBLISH` fails and is counted. The presence check fails *absent*, so `origin_first` notifications broadcast immediately (the safe direction). The ack rate limiter fails open |
| RabbitMQ / Celery worker | Dispatch, promotion, sweeps | Email/SMS dispatch waits; NTF-6 recovers rows the consumer failed to enqueue. Promotion stops, so an `origin_first` notification whose origin vanished stays targeted until Beat returns — it is still in every device's list and count. Cursor retention stops; harmless for days |
| Gateway | Edge routing, SSE pass-through | The service is unreachable from clients; consumption continues |
| `identity` public key | JWT verification | Held locally (A4); `identity` being down does not affect `notification` |
| Producers (`order`, `inventory`, `identity`) | Nothing at runtime | Irrelevant to availability; `notification` never calls them |

**Blast radius.** `notification` being down means events accumulate in Kafka and are processed on recovery — checkout is unaffected, because nothing synchronous is downstream of the order path.

##### 3.1.1.6. Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `NOTIFICATION_PORT` | `8005` | HTTP port |
| `NOTIFICATION_DATABASE_URL` | `postgresql+asyncpg://notification:…@postgres:5432/notification_db` | Own database and role only (R1) |
| `REDIS_URL` | `redis://redis:6379/0` | Tickets, fan-out, presence, caps, rate limits |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Consumer |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` | Avro deserialisation |
| `KAFKA_CONSUMER_GROUP` | `notification` | Also the `processed_events.consumer_group` value |
| `CONSUMER_MAX_ATTEMPTS` | `3` | In-process retries before DLQ (C6) |
| `CELERY_BROKER_URL` | `amqp://guest:guest@rabbitmq:5672//` | Enqueue only; the worker is hosted by the platform |
| `JWT_PUBLIC_KEY_PATH` | `/run/secrets/jwt_public.pem` | A4 |
| `STREAM_TICKET_TTL_SECONDS` | `60` | C19 |
| `STREAM_MAX_PER_USER` | `3` | Concurrent stream cap |
| `STREAM_CONN_COUNT_TTL_SECONDS` | `60` | `nt:conns:*` TTL, refreshed by heartbeat |
| `STREAM_HEARTBEAT_SECONDS` | `20` | Keepalive comment and `ping` frame |
| `STREAM_PRESENCE_TTL_SECONDS` | `45` | `nt:client:*` TTL (C21) |
| `STREAM_QUEUE_MAX` | `100` | Per-connection bounded queue; overflow closes the connection |
| `STREAM_RETRY_BASE_MS` / `STREAM_RETRY_JITTER_MS` | `3000` / `2000` | `retry:` hint sent in `hello` and on shutdown |
| `STREAM_SHUTDOWN_GRACE_SECONDS` | `25` | Drain window on `SIGTERM` |
| `CATCHUP_MAX_FRAMES` | `50` | C22 |
| `CATCHUP_MAX_AGE_HOURS` | `24` | C22 |
| `SEQ_VISIBILITY_WINDOW_SECONDS` | `5` | Overlap window and ack clamp |
| `PROMOTION_GRACE_SECONDS` | `45` | C21 |
| `PROMOTION_SCAN_WINDOW_SECONDS` | `3600` | Bounded promoter scan |
| `PROMOTION_INTERVAL_SECONDS` | `15` | Beat schedule |
| `ACK_RATE_LIMIT_PER_MINUTE` | `12` | Per `(user, client)` (NTF-15) |
| `ACK_MAX_IDS` | `200` | Per request |
| `STREAM_CURSOR_RETENTION_DAYS` | `90` | Sweep |
| `STREAM_CURSOR_MAX_PER_USER` | `20` | Sweep cap |
| `DISPATCH_MAX_RETRIES` | `5` | NTF-5 |
| `DISPATCH_BACKOFF_BASE_SECONDS` / `DISPATCH_BACKOFF_MAX_SECONDS` | `2` / `300` | NTF-5 |
| `REDISPATCH_MIN_AGE_SECONDS` / `REDISPATCH_MAX_AGE_HOURS` | `120` / `24` | NTF-6 |
| `NOTIFICATION_FAKE_EMAIL_FAILURE_RATE` | `0.0` | Test hook for the logging adapter |
| `WEB_PUSH_VAPID_PUBLIC_KEY` / `WEB_PUSH_VAPID_PRIVATE_KEY` / `WEB_PUSH_CONTACT` | unset | Only read if the `push` adapter is registered |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4317` | [RFC 0001](0001-platform-and-shared-infrastructure.md) |

##### 3.1.1.7. Observability

**Metrics.** RED on every endpoint via `common` (`http_requests_total`, `http_request_duration_seconds` by route and status). Service-specific, with the names from archive/0003 T10 kept verbatim and the prose-only instruments named here (NTF-16 is the naming of those, not new instruments):

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `notification_consumed_total` | counter | `topic`, `outcome=recorded\|duplicate\|ignored\|dlq` | Consumer throughput and dedupe hits |
| `kafka_consumer_lag` | gauge | `topic`, `partition` | From the base class |
| `notification_dlq_total` | counter | `topic` | Poison messages |
| `notification_recorded_total` | counter | `type`, `channel`, `scope` | Rows inserted |
| `notification_sse_connections_active` | gauge | — | Active SSE connections per replica |
| `notification_fanout_latency_seconds` | histogram | — | Consumer `PUBLISH` → frame written, from a timestamp in the message |
| `notification_fanout_publish_errors_total` | counter | — | Post-commit `PUBLISH` failures |
| `notification_sse_reconnects_total` | counter | `mode=cold\|resume\|ephemeral` | Reconnect rate (reconnects/min on the dashboard) |
| `notification_sse_slow_consumer_drops_total` | counter | — | Connections closed on queue overflow |
| `notification_sse_rejected_total` | counter | `reason=ticket\|cap` | Refused stream opens |
| `notification_frames_total` | counter | `scope`, `outcome=delivered\|filtered` | Per-connection frame decisions |
| `notification_ack_lag_seconds` | histogram | — | `created_at` → first ack |
| `notification_catchup_rows` | histogram | — | Catch-up rows per reconnect |
| `notification_catchup_truncated_total` | counter | — | Bound hits |
| `notification_cold_start_total` | counter | — | Cold starts |
| `notification_promoted_total` | counter | `reason=presence_absent\|unacked` | Promotions (NTF-9 and the sweeper) |
| `notification_undelivered_age_seconds` | gauge (p99) | `type` | Age of in-app rows with `delivered_at IS NULL`. **The only instrument that detects "the scope rule is wrong and nobody is being toasted"**, which is otherwise a silent failure |
| `notification_dispatch_total` | counter | `channel`, `status=sent\|failed` | Adapter outcomes per attempt |
| `notification_dispatch_exhausted_total` | counter | `channel` | Notifications whose fifth attempt failed |
| `notification_redispatched_total` | counter | — | NTF-6 recoveries |

**Spans.** `notification.consume {event_type}` (child of the producer's span via the envelope's `traceparent`, C11) with children for the insert and the `PUBLISH`; `celery.task dispatch_notification` continuing the same trace through Celery headers; `notification.stream.open` covering ticket redemption, cursor read and catch-up — **not** the whole connection, because a span that lives for hours is useless and must not close at first byte either (archive/0003 §8). Each delivered frame is an event on a short `notification.stream.frame` span linked to the consume span. Request and response bodies are never span attributes.

**Log fields.** Every line carries `trace_id` (= `X-Request-ID`, C12), `service`, and where applicable `notification_id`, `user_id`, `event_id`, `seq`, `client_id`, `channel`, `attempt`. Tickets are never logged.

**Dashboards.** The event-pipeline dashboard carries a `notification` row: consumer lag, DLQ depth, recorded per type, dispatch outcomes. A delivery row: active connections, fan-out latency p50/p99, reconnects/min by mode, catch-up rows, truncations, cold starts, frames delivered vs filtered, promotions by reason, and undelivered age p99. Provisioned as code ([RFC 0001](0001-platform-and-shared-infrastructure.md)).

**Alerts.** None — there is nothing to alert into (archive/0003 Q7). `notification_undelivered_age_seconds` is the first candidate if alerting is ever added.

**Diagnosis walkthroughs** (G10):

| Symptom | Path to the cause |
| --- | --- |
| "A customer got no notification" | `orders.notified_at` → the `order.confirmed` event id → consumer dedupe table → DLQ → delivery attempts |
| "A customer got it late, or only on refresh" | Active-connection metric (was the client even connected?) → fan-out publish→frame latency → reconnect rate → catch-up rows per reconnect. Distinguishes "never pushed" from "pushed to nobody" from "delivered on reconnect" |

#### 3.1.2. API changes

##### 3.1.2.1. Endpoints

All paths are under `/api/v1`, reached through the gateway at `:8000` ([RFC 0000 §3.1.2.2](0000-architecture.md#3122-api-conventions): RFC 9457 errors, `{items,total,limit,offset}` pagination with a 100 ceiling). No endpoint here creates business state from a client request, so none takes `Idempotency-Key`; mark-read, read-all and ack are idempotent by construction. None returns `202`.

| Method | Path | Auth | Purpose |
| --- | --- | --- | --- |
| `GET` | `/notifications` | bearer | The caller's own in-app notifications. Filters: `read`, `type`, `unread_only`, `since_seq` (forward paging for a truncated client). Paginated. Items carry `delivered_at`. **Never scope-filtered** (C21) |
| `POST` | `/notifications/{id}/read` | bearer | Mark read |
| `POST` | `/notifications/read-all` | bearer | Mark all read |
| `GET` | `/notifications/unread-count` | bearer | Badge count for clients that cannot hold a stream open. **Never scope-filtered** (C21) |
| `POST` | `/notifications/ack` | bearer | **Delivery report.** Batched, idempotent, non-blocking. Stamps `delivered_at` and advances the caller's per-device stream cursor (C23) |
| `GET` | `/notifications/admin` | bearer | All notifications with delivery attempts and failures — the operational view, including `delivery_scope`, `origin_client_id`, `delivered_at`, `promoted_at`, and a `delivered_at IS NULL` filter, so "what did we record but never deliver?" is answerable |
| `POST` | `/notifications/stream-ticket` | bearer | Exchanges the JWT for a single-use 60-second stream ticket, **binding `X-Client-Id` into it** (C19, C21) |
| `GET` | `/notifications/stream` | ticket | **SSE stream.** `?ticket=…`, `Last-Event-ID` as a resume hint. `client_id` comes from the redeemed ticket, never a query parameter (C15, C18, C22) |
| `GET` | `/health`, `/health/ready`, `/metrics` | none | Liveness; readiness reports `notification_db`, Redis and broker connectivity and consumer lag; Prometheus exposition |

###### `GET /notifications`

| Query | Type | Default | Notes |
| --- | --- | --- | --- |
| `read` | bool | — | `true` → `read_at IS NOT NULL`; `false` → `IS NULL` |
| `unread_only` | bool | `false` | Shorthand for `read=false`; if both are given and disagree → `400` |
| `type` | `NotificationType` | — | Unknown value → `422` (request schemas are strict, S4) |
| `since_seq` | int ≥ 0 | — | `seq > since_seq`, ordered `seq ASC` (NTF-16). Without it, ordered `created_at DESC` |
| `limit`, `offset` | int | `20`, `0` | `limit` ≤ 100 |

`200`:

```json
{ "items": [ { "id": "uuid", "seq": 84214, "type": "order_confirmed", "title": "…", "body": "…",
               "created_at": "…", "read_at": null, "delivered_at": "…" } ],
  "total": 37, "limit": 20, "offset": 0 }
```

Only `channel = 'in_app'` rows (NTF-2); `user_id` from the token. `payload`, `origin_client_id` and scope are not exposed to the owner — nothing client-side needs them, and the list is deliberately identical on every device.

###### `POST /notifications/{id}/read`

No body. `200 {"id", "read_at"}`. Idempotent: a second call returns the original `read_at`. `404` if the id does not exist **or belongs to another user** — the two are indistinguishable by design. After a change, publishes an `unread_count` message on `nt:user:{user_id}` (NTF-11).

###### `POST /notifications/read-all`

No body. `200 {"updated_count": n}` — `n` is the number of rows this call changed (`0` on a repeat). Affects only the caller's `in_app` rows with `read_at IS NULL`. Publishes an `unread_count` message.

###### `GET /notifications/unread-count`

`200 {"count": n}` — `in_app`, `read_at IS NULL`, for the caller. Never scope- or cursor-filtered.

###### `POST /notifications/ack`

```http
POST /api/v1/notifications/ack          Authorization: Bearer <jwt>
{
  "client_id":  "b3f1…",                  // required; validated for shape only
  "up_to_seq":  84231,                    // advances stream_cursors.cursor_seq
  "ids":        ["…", "…"]                // optional, ≤200; stamps delivered_at
}
→ 204 No Content                          always, including unknown ids and stale seqs
```

Both fields are needed and neither replaces the other. `up_to_seq` is a high-water mark, which is the only shape that survives out-of-order acks and makes the endpoint idempotent by `greatest()`. `delivered_at` has to be per-row and explicit, because scope filtering means a client legitimately never sees some rows inside its range — stamping by range would report a delivery that never happened.

Idempotent by construction: `SET delivered_at = coalesce(delivered_at, now()), delivered_client_id = :client_id` guarded by `WHERE id = ANY(:ids) AND user_id = :uid AND delivered_at IS NULL`, and `INSERT … ON CONFLICT (user_id, client_id) DO UPDATE SET cursor_seq = greatest(…), last_seen_at = now()` with the 5-second clamp (§3.1.1.4). Replaying the same body is a no-op. Never `4xx` except `400` on a malformed body (missing or mis-shaped `client_id` against `^[A-Za-z0-9_-]{8,64}$`, more than 200 ids, negative `up_to_seq`) — an unknown `id` is ignored rather than reported, because it may belong to another user and the response must not say which. Rate-limited at 12/min per `(user, client)`; over the limit is `429` and the client simply drops it.

Client policy, which is what keeps C23 honest: ack on frame render, 2-second debounce, coalesced into one call, `fetch(…, {keepalive: true})`, no retry, no user-visible failure.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant G as Gateway
    participant N as notification
    participant R as Redis
    participant DB as notification_db
    C->>G: POST /api/v1/notifications/ack (client_id, up_to_seq, ids)
    G->>N: forward with Bearer
    N->>N: validate body shape, else 400
    N->>R: INCR rl:notif-ack:{user_id}:{client_id} (NTF-15)
    alt over 12 per minute
        N-->>C: 429, client drops it
    end
    N->>DB: UPDATE notifications SET delivered_at, delivered_client_id WHERE id in ids, user_id, delivered_at IS NULL
    N->>DB: UPSERT stream_cursors cursor_seq = greatest(cursor_seq, least(up_to_seq, settled max seq)), last_seen_at
    N-->>C: 204 No Content
```

###### `GET /notifications/admin`

| Query | Notes |
| --- | --- |
| `user_id`, `type`, `channel`, `delivery_scope` | Exact filters |
| `undelivered` | `true` → `delivered_at IS NULL` (in-app) |
| `dispatch_status` | `failed` → newest attempt failed; `none` → dispatched channel with no attempt |
| `promoted` | `true` → `promoted_at IS NOT NULL` |
| `correlation_id` | The trace id — the link from a Jaeger trace |
| `from`, `to` | `created_at` range |
| `limit`, `offset` | Pagination |

`200` — items carry every `notifications` column except `payload` (available per item as `?include=payload`), plus `attempts: [{attempt, channel, status, error, created_at}]`. All channels are included. `403` for non-admins.

###### `POST /notifications/stream-ticket`

Headers: `Authorization: Bearer <jwt>`, optional `X-Client-Id` (already shape-checked, or stripped, by the gateway; re-checked here). No body. `200 {"ticket": "…", "expires_in": 60}`. The ticket is 32 bytes from a CSPRNG, base64url; Redis stores `nt:ticket:{ticket}` = `{user_id, client_id, jwt_exp}` with `EX 60`. `client_id` is null when the header is absent (NTF-12). `401` on an invalid token; `503` if Redis is unavailable.

###### `GET /notifications/stream`

Query `ticket` (required). Header `Last-Event-ID` (optional, integer; a non-integer is ignored). Before the stream opens, errors are problem details: `401` — ticket missing, unknown, expired, or already redeemed; `429` — a fourth concurrent stream for the user; `503` — Redis or `notification_db` unavailable. On success, `200` with `Content-Type: text/event-stream`, `Cache-Control: no-cache`, `X-Accel-Buffering: no`, and the frame contract in §3.1.1.4. The stream closes with an `error` frame at the token's expiry, on queue overflow (`…/stream-slow-consumer`), and on shutdown (a `retry:` hint, no error).

**Gateway requirements** (owned by [RFC 0002](0002-gateway-service.md), listed because this service depends on them): no buffering of `text/event-stream`, no response compression, an extended read timeout, the per-IP rate limit exempted for `/notifications/stream` (a long-lived connection is one request, and counting it would throttle reconnect storms into a failure), and `X-Client-Id` passed through on all mutating routes and `/notifications/stream-ticket` — validated for shape only (`^[A-Za-z0-9_-]{8,64}$`) and **stripped** if malformed rather than rejecting the request, because a cosmetic header must never fail a checkout.

##### 3.1.2.2. Role matrix

`customer`, `merchant` and `admin` are the `role` claim; *anonymous* is no token; *internal* is a service-to-service caller (there are none).

| Endpoint | anonymous | customer | merchant | admin | internal | Scoping rule |
| --- | --- | --- | --- | --- | --- | --- |
| `GET /notifications` | `401` | ✓ | ✓ | ✓ | — | Own rows only: `user_id = token.sub` |
| `POST /notifications/{id}/read` | `401` | ✓ owner | ✓ owner | ✓ owner | — | Not owner → `404`, never `403` |
| `POST /notifications/read-all` | `401` | ✓ | ✓ | ✓ | — | Own rows only |
| `GET /notifications/unread-count` | `401` | ✓ | ✓ | ✓ | — | Own rows only |
| `POST /notifications/ack` | `401` | ✓ owner | ✓ owner | ✓ owner | — | Foreign ids silently ignored; cursor keyed `(token.sub, client_id)` |
| `GET /notifications/admin` | `401` | `403` | `403` | ✓ | — | All users |
| `POST /notifications/stream-ticket` | `401` | ✓ | ✓ | ✓ | — | Ticket bound to `token.sub` |
| `GET /notifications/stream` | `401` (no/invalid ticket) | ✓ | ✓ | ✓ | — | Channel `nt:user:{user_id}` from the redeemed ticket, never from a parameter |
| `/health`, `/health/ready`, `/metrics` | ✓ | ✓ | ✓ | ✓ | ✓ | Not routed publicly for `/metrics` ([RFC 0002](0002-gateway-service.md)) |

An admin reading `GET /notifications` sees their own notifications, not everyone's — the operational view is a separate endpoint so the two can never be confused. A merchant's `stockout` notifications are their own rows because the recipient was resolved from the event at consume time.

##### 3.1.2.3. Events produced

**None.** `notification` has no `outbox` ([RFC 0000 §3.1.3.2](0000-architecture.md#3132-shared-tables--outbox-and-processed_events)) and no topic in the [event catalogue](0000-architecture.md#3123-event-catalogue). It is a terminal consumer: nothing in the platform reacts to a notification having been sent, and adding a producer would make a failure-tolerant service a dependency of something. See NTF-Q1 on `orders.notified_at`.

##### 3.1.2.4. Events consumed

Consumer group `notification`; dedupe by `processed_events (consumer_group, event_id)` in the same transaction as the insert (S13, C4), plus the NTF-1 unique index. Topics are `smartretail.<domain>.<event>`, Avro, per the [event catalogue](0000-architecture.md#3123-event-catalogue). The envelope's `client_id` is read on every topic ([RFC 0000 §3.1.2.4](0000-architecture.md#3124-event-envelope)).

| Topic | Key | Producer | Effect | Payload fields `notification` requires (NTF-8) |
| --- | --- | --- | --- | --- |
| `identity.user.registered` | `user_id` | `identity` ([RFC 0003](0003-identity-service.md#3123-events-produced)) | `welcome` | `user_id`, `role`, `full_name` |
| `order.confirmed` | `order_id` | `order` ([RFC 0005](0005-order-service.md#3123-events-produced)) | `order_confirmed` | `customer_id`, `order_number`, `total`, `currency`; `tracking_number` (nullable) |
| `order.failed` | `order_id` | `order` | `order_failed` | `customer_id`, `order_number`, `failure_reason` (nullable `OrderFailureReason` code), `payment_status` |
| `order.cancelled` | `order_id` | `order` | `order_cancelled` | `customer_id`, `order_number`, `cancelled_by` (nullable: `customer` \| `merchant` \| `admin` \| `system`), `payment_status` |
| `payment.succeeded` / `payment.failed` | `order_id` | `order` | none — dedupe only (NTF-7, NTF-D36) | — |
| `shipment.status_changed` | `order_id` | `order` | `shipment_update` for `shipped`, `in_transit`, `delivered`; none for `created` | `customer_id`, `order_number`, `to_status`, `tracking_number` (nullable) |
| `inventory.stockout` | `variant_id` | `inventory` ([RFC 0006](0006-inventory-service.md#3123-events-produced)) | `stockout` to the merchant | `merchant_id`, `sku`, `trigger`, `available`, `low_stock_threshold`, `requested` (nullable) |

*Amended Oct 15, 2026 (NTF-D33):* the column above was written before the producers' Avro schemas existed. It now lists the fields of the registered schemas (`common/smartretail_common/events/schemas/`) that the templates actually read: orders carry `order_number`; `order.failed` carries `payment_status` rather than a `payment_captured` flag; `shipment.status_changed` carries `to_status` and `tracking_number`; `inventory.stockout` carries `sku` and `trigger` and no product name.

Missing a required field is a poison message (DLQ), because rendering a notification to the wrong person or with no content is worse than not rendering it. Unknown extra fields are ignored (Avro `BACKWARD`). Retry and DLQ per C6; outage handling per NTF-14. Replay (G3) is safe: every effect is behind both dedupe mechanisms, and `PUBLISH`/enqueue happen only for rows actually inserted.

#### 3.1.3. Data/schema changes

**Store overview.**

###### `notification_db`

Owned by `notification` · PostgreSQL 16 · its own logical database and role on the shared instance (A2, R1). Bursty, fully async, and failure-tolerant — a pure consumer that never sits on a checkout path.

| Table | Purpose |
| --- | --- |
| [`notifications`](#notifications) | The rendered notification and its read state |
| [`delivery_attempts`](#delivery_attempts) | Per-channel send attempts |
| [`push_subscriptions`](#push_subscriptions) | Specified, unused |
| [`stream_cursors`](#stream_cursors) | The server-side SSE replay floor, one row per `(user, device)` |
| [`processed_events`](0000-architecture.md#3132-shared-tables--outbox-and-processed_events) | S13 — carried, not defined here |

| Table | Key | Columns | Purpose |
| --- | --- | --- | --- |
| `notifications` | `uuid` + `seq` | 16 | Rendered notification, read state, stream cursor, origin client and delivery scope, delivery report |
| `delivery_attempts` | `uuid` `‡` | 7 | Per-channel send attempts and errors |
| `push_subscriptions` | `uuid` `‡` | 8 | Specified, unused — makes the offline channel an adapter |
| `stream_cursors` | `uuid`, unique `(user_id, client_id)` | 8 | Per-device SSE replay floor — what stops a fresh session replaying everything |
| `processed_events` | `(group, event_id)` | 4 | S13 |

The only foreign key is `delivery_attempts.notification_id` (same database, R2). `user_id` columns carry no FK (identity owns users); `stream_cursors` relates to `notifications` only logically, by `user_id` and by `cursor_seq` mirroring `seq`. `processed_events` is the shared S13 table, drawn with the columns the consumer writes.

```mermaid
erDiagram
    notifications ||--o{ delivery_attempts : "dispatched via"
    notifications {
        uuid id PK
        bigint seq "bigserial, S1 exception, indexed with user_id"
        uuid user_id "no FK"
        text type "NotificationType"
        text channel "NotificationChannel"
        text title
        text body
        jsonb payload "source envelope, unique with user_id and channel on payload event_id (NTF-1)"
        timestamptz read_at "nullable"
        text correlation_id "indexed"
        text origin_client_id "nullable, opaque client token"
        text delivery_scope "NotificationDeliveryScope, default all_devices"
        timestamptz delivered_at "nullable, first writer wins"
        text delivered_client_id "nullable, null iff delivered_at null"
        timestamptz promoted_at "nullable"
        timestamptz created_at
    }
    delivery_attempts {
        uuid id PK
        uuid notification_id FK "ON DELETE CASCADE"
        text channel "NotificationChannel"
        text status "DeliveryStatus"
        text error "nullable"
        int attempt "1-based"
        timestamptz created_at "indexed"
    }
    push_subscriptions {
        uuid id PK
        uuid user_id "no FK, unique with endpoint"
        text endpoint
        text p256dh
        text auth
        text user_agent "nullable"
        timestamptz created_at
        timestamptz last_seen_at
    }
    stream_cursors {
        uuid id PK
        uuid user_id "no FK, unique with client_id"
        text client_id "max 64 chars"
        bigint cursor_seq "default 0, monotonic"
        timestamptz last_seen_at "indexed"
        text user_agent "nullable"
        timestamptz created_at
        timestamptz updated_at
    }
    processed_events {
        text consumer_group PK
        uuid event_id PK
        text event_type
        timestamptz processed_at
    }
```

**Markers**, as in the archived catalogue:

| Marker | Meaning |
| --- | --- |
| `†` | The column **is named** in an archived RFC, but its **type was not stated** there. The type shown is inferred from the conventions in [S1–S15](0000-architecture.md#3131-schema-conventions-s1s15) |
| `‡` | The column or table is **not named in any RFC**. It exists because a convention requires it — S1 a primary key, S3 timestamps, S12 an outbox for a producer, S13 a dedupe table for a consumer |

In this database: `delivery_attempts` carries 5 `†` rows and 1 `‡` row (the source was a prose list; only `status` was typed); `push_subscriptions` carries 6 `†` rows and 1 `‡` row (a prose list with no types at all). `stream_cursors` carries none: it arrived fully typed, with its key, constraint, index, retention and cap already decided. The `†` types are ratified as a block at the Part A review (archive/0006 Q6).

###### `notifications`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | |
| `seq` | `bigserial` | The stream cursor. Indexed `(user_id, seq)`. Global rather than per-user because a per-user sequence needs a lock or a second table for no benefit — the catch-up query filters by `user_id` anyway. S1 exception |
| `user_id` | `uuid` | no FK; indexed with `created_at desc` |
| `type` | `text` | `order_confirmed` \| `order_failed` \| `order_cancelled` \| `shipment_update` \| `stockout` \| `welcome`. `NotificationType` (S4) — the vocabulary most likely to grow in this platform |
| `channel` | `text` | `in_app` \| `email` \| `sms` — only `in_app` is delivered; the column exists so preferences are additive. `NotificationChannel` (S4) |
| `title`, `body` | `text` | rendered at consume time from a template |
| `payload` | `jsonb` | the source event, for rendering and for support (S8) |
| `read_at` | `timestamptz` | nullable |
| `correlation_id` | `text` | indexed — the link back to the trace |
| `origin_client_id` | `text` | nullable — the `X-Client-Id` of the request that caused the event, or null when there was no originating client (a carrier webhook, a scheduler, a workflow with no captured client), in which case `delivery_scope` is coerced to `all_devices`. An opaque client-minted token of at most 64 characters, validated for shape only, never an identity (C21 and §6.2). Deliberately not `uuid`: the server does not get to assume the client mints one |
| `delivery_scope` | `text` | `origin_only` \| `origin_first` \| `all_devices`. `NotificationDeliveryScope` (S4), derived at consume time from `type` and the presence of `origin_client_id`. `NOT NULL`, default `all_devices` — the safe value, because scope narrows delivery and the default must not |
| `delivered_at` | `timestamptz` | nullable — null means no client has reported receiving a frame. Stamped once by the ack endpoint, first writer wins. A delivery *report*, never a guarantee (C23) |
| `delivered_client_id` | `text` | nullable — the client that reported first; null iff `delivered_at` is null |
| `promoted_at` | `timestamptz` | nullable — set when an `origin_first` notification was widened to all devices because it went unacknowledged (C21) |
| `created_at` | `timestamptz` | |

Index: partial on `(created_at)` where `delivered_at IS NULL AND promoted_at IS NULL AND delivery_scope = 'origin_first'` — the promoter's only query, over a set that is normally near-empty, in the same spirit as the relay's partial index on `outbox`.

Constraint: at-most-one per `(event_id, user_id, channel)` — the invariant that makes `notification` a service (§3.1.1.1). Implemented as a unique index on `((payload->>'event_id'), user_id, channel)` (NTF-1).

###### `delivery_attempts`

Named in archive/0003 §3.1.3 as a prose list; only `status` carried a type.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK `‡` | Not named in the source; S1 requires a key |
| `notification_id` | `uuid` FK → `notifications.id` † | cascade, indexed. Same database, so a real FK (R2 permits it) |
| `channel` | `text` † | `NotificationChannel` (S4), as on `notifications` |
| `status` | `text` | `pending` \| `sent` \| `failed`. `DeliveryStatus` (S4) |
| `error` | `text` † | nullable — the provider error on a failed attempt |
| `attempt` | `integer` † | 1-based retry counter |
| `created_at` | `timestamptz` † | indexed (S3) |

###### `push_subscriptions`

Specified but unused, so the offline channel is an adapter rather than a migration. Named in archive/0003 §3.1.3 as a prose list with **no types at all**; every type below is inferred.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK `‡` | Not named in the source; S1 requires a key |
| `user_id` | `uuid` † | indexed; no FK — owned by `identity` |
| `endpoint` | `text` † | the Web Push endpoint URL |
| `p256dh` | `text` † | client public key, base64url |
| `auth` | `text` † | client auth secret, base64url |
| `user_agent` | `text` † | nullable, for subscription listing |
| `created_at`, `last_seen_at` | `timestamptz` † | S3 |

Constraint: unique `(user_id, endpoint)`.

###### `stream_cursors`

The server-side replay floor, one row per `(user, device)` (C22). It doubles as a device registry, but its job is the cursor — naming it `client_devices` would name the side effect. It exists because `EventSource` retains `Last-Event-ID` only across its own automatic reconnects: a page reload or a fresh browser profile sends none, and without a server-side floor the catch-up query replays a lifetime of notifications as live frames.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | S1 |
| `user_id` | `uuid` | no FK — owned by `identity`; part of the unique key |
| `client_id` | `text` | the client-minted token, at most 64 characters, always scoped under `user_id` and never trusted as an identity |
| `cursor_seq` | `bigint` | `NOT NULL` default `0`. A plain `bigint`, not a sequence — it mirrors `notifications.seq` rather than generating one, so it is not an S1 exception. Advanced monotonically with `greatest()` by the ack endpoint, clamped by the 5-second visibility window (§3.1.1.4) |
| `last_seen_at` | `timestamptz` | indexed — the retention sweep's query; refreshed on stream open and on each ack |
| `user_agent` | `text` | nullable — for the admin device list only |
| `created_at`, `updated_at` | `timestamptz` | S3 |

Constraint: unique `(user_id, client_id)` — the whole table is an upsert target. Retention: rows untouched for 90 days are deleted by `sweep_stale_stream_cursors` (Celery Beat, daily), which also enforces a cap of **20 rows per user**, evicting by `last_seen_at`, so a client rotating its id cannot grow the table without bound.

*Why Postgres and not Redis.* A cursor that a Redis flush resets turns every device into a cold start — survivable, but wrong. One small row per device is cheap enough that the durable store is the obvious answer. Redis holds only the 45-second presence key, which is genuinely disposable.

###### Indexes and constraints, collected

| Table | Index / constraint | Serves |
| --- | --- | --- |
| `notifications` | PK `id` | — |
| `notifications` | `(user_id, seq)` | Catch-up; `since_seq` paging; ack clamp |
| `notifications` | `(user_id, created_at DESC)` | Default list ordering; unread count (with `read_at IS NULL` as a filter) |
| `notifications` | `(correlation_id)` | Admin lookup from a trace |
| `notifications` | partial `(created_at)` WHERE `delivered_at IS NULL AND promoted_at IS NULL AND delivery_scope = 'origin_first'` | The promoter |
| `notifications` | unique `((payload->>'event_id'), user_id, channel)` | I1 (NTF-1) |
| `delivery_attempts` | FK `notification_id` → `notifications.id` `ON DELETE CASCADE`; index `(notification_id)` | Attempt history per notification |
| `delivery_attempts` | `(created_at)` | S3; admin time-range queries |
| `push_subscriptions` | `(user_id)`; unique `(user_id, endpoint)` | Adapter lookup; resubscription upsert |
| `stream_cursors` | unique `(user_id, client_id)` | Upsert target |
| `stream_cursors` | `(last_seen_at)` | Retention sweep and the per-user cap |

Every enum-valued column is `text` with a `COMMENT ON COLUMN` pointing at `app/v1/notifications/enums.py` (S4). No `CHECK` constraints on vocabularies.

###### Shared tables carried

`processed_events` (S13) — consumes `order.*`, `payment.*`, `shipment.status_changed`, `inventory.stockout`, `identity.user.registered`. Its DDL is defined once in [RFC 0000 §3.1.3.2](0000-architecture.md#3132-shared-tables--outbox-and-processed_events). `notification_db` carries **no** `outbox`: `notification` produces nothing.

###### Non-database state (Redis, owned by `notification`)

Hosted by the platform Redis ([RFC 0001](0001-platform-and-shared-infrastructure.md)); the keys and their semantics are this service's.

| Pattern | TTL | Use |
| --- | --- | --- |
| `nt:user:{user_id}` | — | Pub/Sub channel for notification fan-out (C17). Not a stored key. The message body carries `origin_client_id` and `scope`, and the delivering replica filters per connection |
| `nt:ticket:{ticket}` | 60s | Single-use SSE stream ticket, redeemed with `GETDEL` (C19). The value is `{user_id, client_id, jwt_exp}` as JSON, so the stream's client identity comes from the redeemed ticket and never from the URL |
| `nt:conns:{user_id}` | 60s | Per-user connection count, for the 3-stream cap; refreshed by the heartbeat so a crashed replica's slots expire on their own |
| `nt:client:{user_id}:{client_id}` | 45s | Presence hint (C21). Set on stream open, refreshed by the 20s heartbeat, deleted on clean close. The value is `1` — deliberately *not* a replica id |
| `rl:notif-ack:{user_id}:{client_id}` | 60s | Ack rate limit (NTF-15), in the platform's `rl:{scope}:{identifier}` keyspace |

**The C20 retreat, stated rather than buried.** C20 said there is no shared connection registry, and `nt:client:*` is one. What makes it acceptable is what it does *not* contain: no replica address, so no routing decision is ever made from it, no sticky session is implied, and there is no consistency to maintain — it expires on its own if a replica dies. It is consulted at exactly one place, the decision whether an `origin_first` notification can be targeted, and both error directions are bounded: a false negative broadcasts a notification that could have been targeted (cosmetic), and a false positive targets a dead socket, which the promoter fixes within the ~75 s worst case. The cheapest correct alternative — no presence key at all, always publish targeted and always let the promoter widen — is one line away and is the recorded fallback if the key disappoints; it was rejected only because it makes the 45 s delay the *common* case rather than the failure case.

**Rejected keys.** `nt:cursor:{user_id}:{client_id}` as a write-behind cache of `cursor_seq`: acks are batched at ≤1 per 2 s per client, so the Postgres upsert is not a hot path, and a second copy of a cursor is a second thing to be wrong. `nt:conns:*` could likewise be derived by counting `nt:client:{user_id}:*`, but that is a `SCAN` on the connection-open path — two keys, stated reason, no cleverness.

###### Migrations

Per-service Alembic history under `services/notification/alembic/`, every revision with a working `downgrade` (S14), all additive.

| Revision | Creates | Downgrade |
| --- | --- | --- |
| `0001_initial` | `notifications` (all 16 columns, including `seq`, `origin_client_id`, `delivery_scope` default `all_devices`, `delivered_at`, `delivered_client_id`, `promoted_at`), its indexes, the partial promoter index and the NTF-1 unique index; `delivery_attempts`; `push_subscriptions`; `processed_events` | Drop in reverse |
| `0002_stream_cursors` | `stream_cursors`, unique `(user_id, client_id)`, index `(last_seen_at)` | Drop |
| `0003_enum_comments` | `COMMENT ON COLUMN` for `type`, `channel`, `delivery_scope`, `status` | Remove comments |

The `alembic/env.py` `render_item` hook for `DomainEnumText` is mandatory ([RFC 0000 §3.1.3.1](0000-architecture.md#3131-schema-conventions-s1s15)). Should a database ever pre-date `seq`, adding it as a `bigserial` backfills existing rows in insertion order; `origin_client_id`, `delivered_at`, `delivered_client_id` and `promoted_at` are nullable; and `delivery_scope` defaults to `all_devices`, which is the pre-scoping behaviour, so existing rows need no backfill at all.

#### 3.1.4. Breaking changes

| Change | Impact | Handling |
| --- | --- | --- |
| `notification` holds long-lived connections | It is not a plain request/response service: deploys must drain connections, the gateway must not buffer, and capacity is measured in sockets as well as RPS | §6.3. No sticky sessions are required (C20), so the load balancer configuration is unchanged; the cost is real but contained to deployment |
| A stream does not replay unbounded history on connect (C22) | A client that relied on the stream to populate its list must read `GET /notifications`. The `hello` frame says which case it is in | Nothing has shipped against an unbounded version. The `cold_start` flag and `catchup_truncated` exist so the client never has to guess |
| Frames are filtered by delivery scope (C21) | A second device does not receive *toasts* for another device's checkout | Counts and lists are never filtered, so nothing disappears — only the toast moves. G5d asserts it |
| Before this service, the order workflow's `notify` step wrote only a log line and an outbox row | Customers now receive notifications; `order` must persist `origin_client_id` and every producer must carry `client_id` in the envelope | Additive in every producer ([RFC 0005](0005-order-service.md), [RFC 0000](0000-architecture.md#3124-event-envelope)); `client_id` is `["null","string"]` with a `null` default, a `FULL`-compatible change |

No migration destroys or rewrites data.

#### 3.1.5. Action Plan

Ordered so that a notification is *recorded* correctly before it is *pushed*, and single-replica push with bounded catch-up exists before fan-out and scoping are layered on.

| # | Task | Output | Depends on |
| --- | --- | --- | --- |
| T14 | `notification` service: skeleton, `notification_db`, migrations `0001`, the four vocabularies, consumer for the six notification types (and dedupe-only for `payment.*`), templates, recipient resolution, scope derivation | Notifications persisted per event type (G5); replay changes nothing (G3) | Consumer base class, Avro schemas, relay ([RFC 0001](0001-platform-and-shared-infrastructure.md)); Celery (T12) |
| T15 | Notification dispatch as a Celery task with attempt recording and the logging adapters; NTF-5 retry policy; NTF-6 redispatch sweep | Attempts recorded; a failed dispatch is retried and visible | T14 |
| T16 | Notification read APIs — list, mark read, read-all, unread count — and the admin view | Tests green, isolation enforced | T14 |
| T16a | `notifications.seq`, the stream-ticket endpoint, and the SSE handler with catch-up and heartbeat (C18, C19). The catch-up is bounded from the first commit — the unbounded version is not shipped even as an intermediate step | A single replica pushes to a connected client; `Last-Event-ID` replays a gap (G5b) | T16 |
| T16b | Redis Pub/Sub fan-out: `PUBLISH` after commit in the consumer, dynamic `SUBSCRIBE`/`UNSUBSCRIBE` per connection, subscribe-before-catch-up (C17, NTF-10), `unread_count` in the message (NTF-11) | **Two replicas: consume on one, deliver on the other** (G5a) | T16a, Redis (T20) |
| T16c | Gateway SSE pass-through — no buffering, no compression, extended read timeout, rate-limit exemption on the stream route ([RFC 0002](0002-gateway-service.md)) | Measured end-to-end push latency through the gateway (G5a) | T16b |
| T16d | Connection hygiene: bounded queues, the 3-stream cap, jittered `retry:`, drain on shutdown, and the connection/latency metrics | A slow client is dropped and recovers; a rolling restart does not stampede | T16c, metrics (T10) |
| T16e | `X-Client-Id` end to end: gateway pass-through and shape validation, request context, `outbox.client_id`, the envelope field and its Avro defaults, and `orders.origin_client_id` so workflow-emitted events keep their origin (C21). Owned across [RFC 0002](0002-gateway-service.md), [RFC 0001](0001-platform-and-shared-infrastructure.md) and [RFC 0005](0005-order-service.md); listed here because `notification` is its only reader | A checkout from client A produces an `order.confirmed` carrying A's id; a carrier webhook produces one carrying null | Envelope, relay |
| T16f | `stream_cursors`, the bounded catch-up (50 / 24h), the `hello` and `catchup_truncated` frames, cold start, and ephemeral streams (C22, NTF-12) | A never-seen `client_id` against 500 notifications yields zero frames and a correct badge (G5c) | T16a |
| T16g | `POST /notifications/ack`: batched, idempotent, rate-limited; `delivered_at`; cursor advance with the 5-second safe-seq clamp (C23) | Acks stamp and advance; a double ack is a no-op; a high ack cannot strand a lower `seq` (G5e) | T16f |
| T16h | Scoped delivery: `NotificationDeliveryScope` per type, the client-bound stream ticket, the `nt:client:*` presence key, NTF-9, per-connection frame filtering, `promote_undelivered_notifications` on Beat, and `sweep_stale_stream_cursors` (C21) | Two devices, one user: an origin-scoped frame reaches one and both badges move; with the origin offline the notification is promoted within the stated bound (G5d) | T16b, T16e, T16g |
| T10 (part) | The `notification` metrics in §3.1.1.7, including `notification_undelivered_age_seconds` | `/metrics` exposes every instrument; dashboards load | Observability stack ([RFC 0001](0001-platform-and-shared-infrastructure.md)) |
| T25 (part) | End-to-end: checkout producing one trace ending in the notification consumer, a notification recorded — and the frame landing on the originating device only | `make test` green cold | T16h |
| — | `push` adapter: register `WebPushAdapter`, VAPID settings, a subscription endpoint | Not scheduled — requires a client with a service worker | A client project |

If the schedule slips, the reducible scope is fixed in advance (Q13): T16a + T16f — single-replica SSE with *bounded* catch-up — deliver the user-visible behaviour; T16b's fan-out and T16h's scoping follow. The bounded catch-up is not part of the reducible scope.

**Delivery phasing**

| Component | Milestone |
| --- | --- |
| `notification_db`, `notifications`, `delivery_attempts`, `push_subscriptions` (created, unused), `processed_events`; consumer, templates, vocabularies (T14) | M3 |
| `dispatch_notification`, logging email/SMS adapters, attempt recording, redispatch sweep (T15) | M3 |
| Read APIs and admin view (T16) | M3 |
| Stream ticket, SSE handler, `seq`, bounded catch-up, heartbeat (T16a) | M3 |
| `stream_cursors`, cold start, `hello` / `catchup_truncated` (T16f) | M3 |
| Redis Pub/Sub fan-out across replicas (T16b) | M3 (M4 under the Q13 fallback) |
| Gateway SSE pass-through (T16c) and connection hygiene (T16d) | M3 |
| `X-Client-Id` end to end, `orders.origin_client_id`, envelope `client_id` (T16e) | M3 |
| Ack endpoint and delivery report (T16g) | M3 |
| Scoped delivery, presence key, promoter, cursor sweep (T16h) | M3 (M4 under the Q13 fallback) |
| Notification metrics and dashboard rows (T10 part) and end-to-end test (T25 part) | M3 |
| Ratification of `†` types (archive/0006 Q6), opaque client token as a reference kind (archive/0006 Q7), Q12, Q16 | M4 (Part A review) |
| `push` adapter registration | Not scheduled |

### 3.2. Rationale

#### 3.2.1. Decision register

This RFC already numbers its decisions (C4–C23 from the archive, NTF-1–NTF-16 new here). The register gives one row per significant decision and does not renumber them: a row that restates an existing ID says so in the Decision cell (“= C16”), and rows without one (NTF-D1, NTF-D2, NTF-D6, NTF-D20, NTF-D22, NTF-D23, NTF-D24) are decisions that until now were stated only in prose. The full reasoning stays in the linked sections. The register itself was added after [mentor review 2026-10-06 §2](../reviews/mentor-reviews.md#2-a-reason-for-every-decision). Rows a review changed say so at the end of *Why* ([ARC-D36](0000-architecture.md#321-decision-register)).

| ID | Decision | Why | Alternatives rejected (and why) | Revisit if |
| --- | --- | --- | --- | --- |
| NTF-D1 | `notification` is its own service, owning `notification_db` | Passes all four parts of the ownership test, and its profile (bursty, async, failure-tolerant, connection-holding) is unlike any neighbour's ([§3.2](#32-rationale), [§3.1.1.1](#3111-responsibility-and-boundaries)) | Fold into `order`: puts SSE connections and email retries inside the service with the strongest consistency need, and makes a notification outage a checkout risk | Notifications have to join the checkout transaction, or the long-lived connections move to a dedicated edge service |
| NTF-D2 | A terminal consumer: no outbox, no events produced, no synchronous call in or out except the gateway | Nothing reacts to a notification having been sent; a producer would make a failure-tolerant service a dependency of something (R4, R5, R7; [§3.1.2.3](#3123-events-produced)) | A `notification.delivered` event for `orders.notified_at`: a new producer for a cosmetic column (NTF-Q1, option b) | NTF-Q1 is decided for option (b), or another service needs to react to delivery |
| NTF-D3 | = C4 + NTF-1. At-least-once consumption made idempotent twice: `processed_events` and a unique `((payload->>'event_id'), user_id, channel)` index | The second guard keeps the outcome idempotent even when a replay tool or backfill bypasses the dedupe table; the expression index keeps the catalogued columns exact ([§3.2](#32-rationale), [§3.1.1](#311-architectural-changes)) | Kafka transactions: exactly-once does not extend to a Postgres write or an email. An explicit `event_id` column: a second copy of the id (NTF-Q6) | Review prefers an explicit column (NTF-Q6): one additive migration |
| NTF-D4 | = NTF-2. One row per channel; only `in_app` rows are listed, counted or streamed | The invariant is per channel, so the row must be ([§3.3](#33-tradeoffs)) | One row with a channel array: at-most-one is then not enforceable by a constraint | Channels need a shared read state across email and in-app |
| NTF-D5 | = NTF-3 + NTF-7. The channel set is a per-type constant in code; `payment.*` and `shipment created` produce nothing | Preferences are deferred, so a per-type table is the honest shape; notifying both `payment.failed` and `order.failed` would be the duplicate the service exists to prevent ([§3.1.1.2](#3112-domain-model)) | Per-user preferences now (deferred, §2.2); a payment notification type (duplicates the order outcome) | Preferences or unsubscribe come into scope ([§2.2](#22-non-goals)) |
| NTF-D6 | Render `title`/`body` at consume time with plain per-type Python format functions; keep the envelope in `payload` | The rendered text is a record of what the user was told; re-rendering stays possible, but only deliberately ([§3.2](#32-rationale), [§3.1.1.2](#3112-domain-model)) | Render on read: rewrites history when a template changes and puts templates on the read path. A template engine: there is no HTML | Localisation or HTML email arrives with a real provider |
| NTF-D7 | = C6 + NTF-14. Poison messages: 3 in-process attempts, then DLQ. Infrastructure outages: pause the partition and retry, never DLQ | A poison message must not stop a partition, but an outage is not the event's fault and would otherwise DLQ every event ([§3.1.1](#311-architectural-changes)) | DLQ everything after 3 attempts: empties an outage into the DLQ | The `common` consumer base class cannot tell the two apart ([RFC 0001](0001-platform-and-shared-infrastructure.md)) |
| NTF-D8 | = C7 + NTF-5. Email/SMS dispatch is a Celery task with 5 retries, exponential backoff with jitter, `acks_late`, one attempt row per try | "One job soon, with per-task retry" is the Celery shape; inline sending would hold a Kafka partition hostage to an SMTP timeout ([§3.2](#32-rationale), [Celery tasks](#celery-tasks)) | Send inside the consumer; a Temporal workflow (nothing here is a process) | A real provider needs rate shaping or batching beyond per-task retry |
| NTF-D9 | = NTF-6. A Beat sweep re-enqueues dispatched-channel rows with no attempt, or a stale `pending` one | A crash or RabbitMQ outage between commit and enqueue would otherwise leave a row undispatched forever; the task is idempotent ([§3.1.1](#311-architectural-changes)) | An outbox for the enqueue: a second relay for a recoverable gap | Enqueue loss is frequent enough that 5 minutes of delay matters |
| NTF-D10 | = C15. Push to connected clients over SSE | Checkout returns `202`, so the notification is the customer's only window into the outcome; SSE is plain HTTP, so gateway auth, rate limits, tracing and problem details apply unchanged ([§3.2](#32-rationale)) | Polling: latency bound by the interval and load scaling with idle users ([§5](#5-alternate-solutions) Alt 9). WebSockets: bidirectionality nobody needs, at the cost of a stateful gateway path (Alt 10) | The product needs client→server streaming or typing indicators |
| NTF-D11 | = C16. The push path is an accelerant over the durable `notifications` row, never the source of truth | Lets fan-out be lossy, replicas die mid-frame and every failure degrade to latency, never loss ([§3.2](#32-rationale), [§11](#11-notes)) | Treat the open connection as the delivery guarantee: what makes real-time systems fragile | Never, without redesigning the stream |
| NTF-D12 | = C17. Fan-out between replicas over Redis Pub/Sub, one channel per user, filtered per connection | The consuming replica is almost never the holding replica; at-most-once is enough under C16 and Redis is already in the stack ([§3.1.1](#311-architectural-changes)) | Per-replica Kafka groups (Alt 11: offset garbage, persistence and delivery entangled); Redis Streams or a second topic (Alt 12: protects data not at risk); per-device or sharded channels (Q11) | `SUBSCRIBE` churn shows up in Redis latency (Q11) |
| NTF-D13 | = C18 + NTF-10. A global `bigserial` `seq` with `Last-Event-ID` resume; subscribe before catch-up; a 5-second overlap window with client de-duplication by `id` | One catch-up query recovers blips, restarts and dropped messages alike; subscribing first closes the gap between query and subscription; the window fixes the `seq` visibility trap ([The SSE stream](#the-sse-stream)) | A per-user sequence: needs a lock or a second table. Catch-up before subscribe: a notification can be neither replayed nor received live | Out-of-order commits take longer than 5 s under load |
| NTF-D14 | = C19. Streams authenticate with a 60-second single-use Redis ticket redeemed by `GETDEL` | `EventSource` cannot set `Authorization`, and a JWT in the URL leaks into logs and `Referer` ([§6.2](#62-security-considerations)) | JWT in the query string; WebSockets to avoid the ticket (Alt 10) | Clients move to `fetch`-based streaming that can send headers |
| NTF-D15 | = C20. No replica-affine state; only shared, advisory, fail-safe state (Postgres cursor, Redis presence key) | Any replica serves any reconnect with no sticky sessions, so scaling stays ordinary ([§3.1.3](#non-database-state-redis-owned-by-notification), "the C20 retreat") | A connection registry mapping clients to replicas, or sticky sessions: routing becomes sticky | A feature needs to address one specific connection |
| NTF-D16 | = C21 + NTF-9. Frames are scoped by the originating `client_id` per type; `origin_first` widens immediately when the origin's presence key is absent, else after 45 s by a 15 s Beat promoter | A toast on the wrong device is noise, but scope narrows only frames, never the count or list, so a wrong rule only misplaces a toast ([§3.2](#32-rationale), [§3.1.1.2](#3112-domain-model)) | No presence key: makes the 45 s delay the common case. Never promote: an unseen payment failure. A per-notification countdown task: a task storm | The presence key disappoints (recorded fallback), or the ~75 s worst case is too slow for `order_failed` |
| NTF-D17 | = C22. A server-side replay floor in `stream_cursors`; catch-up bounded at 50 frames and 24 h; a first-seen `client_id` cold-starts with zero frames | `Last-Event-ID` survives only `EventSource`'s own reconnects; an unbounded replay is a self-inflicted denial of service ([§3.2](#32-rationale), [`stream_cursors`](#stream_cursors)) | Unbounded `seq > cursor`; cursor in Redis (a flush turns every device cold); newest 50 on truncation (a hole below); courtesy frames on cold start (Q14) | UX feedback on cold start (Q14) |
| NTF-D18 | = C23 + NTF-15. The ack is a batched, idempotent, rate-limited report that always returns `204`; `delivered_at` is stamped by explicit id, the cursor by a clamped high-water mark | Records what happened without anything depending on it; stamping by id avoids reporting deliveries that scope filtering prevented; the clamp stops a high ack stranding a lower `seq` ([`POST /notifications/ack`](#post-notificationsack)) | Ack as an at-least-once protocol (rebuilds TCP badly); stamping by range; gateway rate limiting (cannot see the body's `client_id`) | Offline push needs per-device delivery receipts |
| NTF-D19 | = NTF-13. The notification lifecycle is derived from `read_at`, `delivered_at`, `promoted_at`; no status column | Three facts with three writers and no order between them; a status would be a fourth thing to keep consistent ([§3.1.1.3](#3113-state-machines)) | A status enum on `notifications` | A reader needs to filter on a combined state at scale |
| NTF-D20 | Delivery attempts are append-only: a retry inserts attempt *n + 1*; `in_app` has no attempt rows (NTF-4) | Keeps the full history for the admin view; `in_app`'s delivery record is `delivered_at` ([§3.1.1.3](#3113-state-machines)) | Reopening a failed row (loses history); attempt rows for SSE frames (the ack already records it) | `push` is registered and needs per-subscription attempts |
| NTF-D21 | = NTF-12. A ticket without `client_id` opens an ephemeral stream: no cursor row, never an origin, never receives `origin_only` frames | A missing or stripped `X-Client-Id` must never fail a request (C21) ([§3.1.1](#311-architectural-changes)) | Reject the stream; mint a server-side id (the server does not get to assume one) | Most clients turn out to be ephemeral |
| NTF-D22 | `push_subscriptions` and the `push` channel value are specified now; the adapter is not registered | Makes the offline half of delivery an adapter swap rather than a redesign, without owning VAPID keys or a service worker ([§2.2](#22-non-goals)) | Leave push out of the schema entirely: a later migration and redesign | A client with a service worker exists |
| NTF-D23 | Connection hygiene: bounded 100-message queue that closes on overflow, 3 streams per user, 20 s heartbeat, jittered `retry:`, drain on shutdown | Closing is lossless because reconnect catches up by `seq`; unbounded buffering is how push systems die under load ([§6.1](#61-scalability-and-fault-tolerance)) | Unbounded per-connection buffering; per-connection count keys (a `SCAN` on open, NTF-Q8) | `nt:conns` false `429`s are seen in practice (NTF-Q8) |
| NTF-D24 | Entity-first layout: `notifications/`, `subscriptions/`, `stream/`, each HTTP contract in `<entity>/v1/`; where the consumer and tasks live is NTF-D25 | The binding template in [file-structure §4–§5](../file-structure.md#4-service-layout-every-service): only the HTTP contract is versioned (`<entity>/v1/`), and repos are the one place rules live, called by routes, consumers and tasks alike ([§3.1.1.1](#3111-responsibility-and-boundaries)). *Changed after [mentor review 2026-10-06 §3](../reviews/mentor-reviews.md#3-entity-first-service-layout-with-versions) — previously layer-first: `app/api/v1/`, `models/`, `schemas/`, `services/`, with templates in `app/templates/`.* | Layer-first `api/`, `services/`, `models/`: scatters one entity across folders and cannot version the contract alone | The template changes in [RFC 0000 §3.1.1.7](0000-architecture.md#3117-module-structure-inside-a-service) |
| NTF-D25 | Under [ARC-D27](0000-architecture.md#321-decision-register), the Kafka consumer is `notifications/consumers/`; Celery tasks sit under the entity whose rows they write — `notifications/tasks/` (dispatch, promotion, redispatch) and `stream/tasks/` (cursor sweep); `core/` keeps only config, db, logger, errors, dependencies and shared schemas | The consumer writes only `notifications` rows through `recording_repo.py`, and each task changes one entity's table, so one folder answers what an entity does and what runs against it. *Changed after [mentor review 2026-10-12 §2](../reviews/mentor-reviews.md#2-every-file-in-its-entity-folder) — previously app-level `consumers/` and `tasks/`.* | App-level `consumers/` and `tasks/` (a second, layer-first tree beside the entities, which ARC-D23 rejected); one `tasks/` under `notifications/` for all four (puts the cursor sweep away from the table it deletes from) | A consumer or task starts writing to two entities' tables — it moves to `core/` and this row records why |
| NTF-D26 | (Milestone 3 implementation) Scoped delivery is deferred: every frame goes to every connected device of the user. `origin_client_id` and `delivery_scope` are still derived (I2) and stored on every row; the `nt:client:*` presence key, NTF-9's promote-at-insert, the per-connection scope filter and `promote_undelivered_notifications` are not built. The `origin` flag on a frame still tells the originating device it was the origin | Scope only ever narrows *toasts* (C21), so broadcasting is the safe direction: nothing is lost, a second device merely toasts too. Storing the scope now means enabling it later is code only, with no migration or backfill, and there is no client yet on which to judge the toast UX. *Changed after [mentor review 2026-10-13](../reviews/mentor-reviews.md#review-of-2026-10-13) — previously T16h in M3.* | Build all of C21 now: a presence key, a Beat promoter every 15 s and a filter whose only observable benefit needs a second real device | A client exists and G5d is a milestone gate, or a second device's toasts are reported as noise |
| NTF-D27 | (Milestone 3 implementation) No `WebPushAdapter` file; the `push` vocabulary value and the `push_subscriptions` table exist (NTF-D22), and the adapter registry has `email` and `sms` only | An adapter with no VAPID keys, no service worker and no subscription endpoint cannot be exercised, so it would be untested code. *Changed after [mentor review 2026-10-13](../reviews/mentor-reviews.md#review-of-2026-10-13) — previously "specified" as `adapters/web_push.py`.* | A stub adapter that raises: registry noise with no behaviour | A client with a service worker exists (§3.1.5, "push adapter") |
| NTF-D28 | (Milestone 3 implementation) The §3.1.1.7 metrics, spans and `/metrics` endpoint are deferred; the service emits structured JSON logs with `event_id`, `notification_id`, `client_id`, `attempt`. `/health/ready` checks `notification_db` and Redis only, not the broker or consumer lag | The platform's OTel and Prometheus wiring is not in `common` yet; instrumenting one service ahead of it would invent conventions RFC 0001 owns. *Changed after [mentor review 2026-10-13](../reviews/mentor-reviews.md#review-of-2026-10-13) — previously T10 (part) in M3.* | Hand-rolled Prometheus counters in this service: a second metrics convention to unwind | RFC 0001's observability task lands; `notification_undelivered_age_seconds` is the first instrument to add |
| NTF-D29 | (Milestone 3 implementation) Reconnect-storm, capacity and through-the-gateway latency tests are deferred. The mechanisms exist — the 3-stream cap, the jittered `retry:` on `hello` and on shutdown, the bounded queue — and are tested functionally against a real uvicorn server | Load figures (~10k sockets per replica) are estimates that only a load rig can confirm; functional tests prove each mechanism behaves. *Changed after [mentor review 2026-10-13](../reviews/mentor-reviews.md#review-of-2026-10-13) — previously part of T16c/T16d.* | A load test in the unit suite: slow, flaky and not a measurement | Before any production use, or the gateway's SSE path changes |
| NTF-D30 | (Milestone 3 implementation) SSE is Starlette's `StreamingResponse` with `text/event-stream`, `Cache-Control: no-cache`, `X-Accel-Buffering: no`, and a small typed frame serialiser (`stream/frames.py`); not `sse-starlette` | The frame contract is six events and a comment; a dependency adds its own ping and disconnect handling that would duplicate the heartbeat, slot refresh and JWT-expiry loop this stream needs. Cleanup runs in a shielded `finally`, so a disconnect always releases the slot and the subscription. *Changed after [mentor review 2026-10-13](../reviews/mentor-reviews.md#review-of-2026-10-13) — previously §4 listed `sse-starlette`.* | `sse-starlette` (§4): a second heartbeat and less control over close semantics | The assistant stream ([RFC 0009](0009-assistant-service.md)) adopts `sse-starlette` and one SSE implementation should serve both |
| NTF-D31 | (Milestone 3 implementation) Redis Pub/Sub fan-out is built and mandatory, not the Q13 fallback: the consumer runs as its own process (`notification-consumer`), so the replica holding a stream is never the one consuming, and every in-app frame travels `PUBLISH` → `SUBSCRIBE` | With the consumer split from the API, a single-replica in-process push would deliver nothing; making fan-out the only path means the cross-replica test (G5a) exercises the same code as production. *Changed after [mentor review 2026-10-13](../reviews/mentor-reviews.md#review-of-2026-10-13) — previously fan-out was reducible to M4 under Q13.* | Consumer inside the API process with in-process delivery: couples consumption to HTTP replicas and still needs fan-out for any second replica | Never, while the consumer is a separate process |
| NTF-D32 | (Milestone 3 implementation) One Celery queue, `notification.notifications`, for dispatch and every sweep; Beat embedded in the worker (`-B`) | Queues separate workloads that can starve each other; here dispatch and three small sweeps share one worker and the sweeps run daily or every 5 minutes. One queue per service matches the platform's `<service>.<queue>` rule (PLT-D36). *Changed after [mentor review 2026-10-13](../reviews/mentor-reviews.md#review-of-2026-10-13) — previously `notifications` and `maintenance` queues.* | A second `notification.maintenance` queue and worker: a container for a few rows a day | A real provider makes dispatch slow enough that a sweep waits behind it |
| NTF-D33 | (Milestone 3 implementation) NTF-8 and the templates are aligned to the registered Avro schemas: `order_number` rather than an `order_ref` cut from the id; `order.failed`'s `payment_status` rather than `payment_captured`; `shipment.status_changed`'s `to_status`/`tracking_number`; `inventory.stockout`'s `sku`/`trigger` (no product name). A missing required field raises `KeyError` and the event is dead-lettered | The schemas, not this RFC, are the contract producers ship; templates that read fields no producer sends would DLQ every event. *Changed after [mentor review 2026-10-13](../reviews/mentor-reviews.md#review-of-2026-10-13) — previously the §3.1.2.4 field list written before the schemas.* | Ask producers to add the RFC's fields: duplicates (`order_ref`), or a projection `inventory` does not hold (product name) | A template needs a field no schema carries — the producer's schema changes first, `BACKWARD`-compatibly |
| NTF-D34 | (Milestone 3 implementation) The after-commit step re-reads the event's rows by `payload->>'event_id'` (the NTF-1 index's leading column), then publishes `in_app` rows and enqueues `email`/`sms` rows; every failure is logged and swallowed | The base calls `after_commit(envelope)` only after `HANDLED`, with no handle on what the handler inserted; the indexed re-read is one query and needs no state across the commit. On an NTF-1 replay (dedupe row gone, rows present) it re-publishes and re-enqueues, both harmless: clients de-duplicate by `id` and dispatch no-ops on a `sent` attempt. *Changed after [mentor review 2026-10-13](../reviews/mentor-reviews.md#review-of-2026-10-13) — previously "for each row inserted".* | Carrying inserted ids from handler to `after_commit` in process memory: state that leaks on a dead-lettered event | Replays become frequent enough that duplicate frames are noticed |
| NTF-D35 | (Milestone 3 implementation) One `FanoutHub` per process holds one Redis `PubSub` connection; the first local stream of a user `SUBSCRIBE`s `nt:user:{id}` and waits for Redis's confirmation (NTF-10), the last one `UNSUBSCRIBE`s; a single reader task routes messages to per-connection bounded queues. If that connection fails, every local stream ends with a `retry:` hint and reconnects through catch-up | One socket per replica instead of one per stream keeps Redis connections flat as streams grow; waiting for the subscribe confirmation is what makes subscribe-before-catch-up exact. *Changed after [mentor review 2026-10-13](../reviews/mentor-reviews.md#review-of-2026-10-13) — previously unspecified per connection.* | A `PubSub` per stream: one Redis connection per open socket | One reader task becomes a bottleneck at measured load (NTF-D29) |
| NTF-D36 | (Milestone 3 implementation) `payment.succeeded` and `payment.failed` have no-op handlers rather than none | The consumer base returns `IGNORED` and records nothing for a type without a handler; a no-op handler makes the base record the event in `processed_events`, so dedupe and replay are uniform across every subscribed type, as NTF-7 requires. *Changed after [mentor review 2026-10-13](../reviews/mentor-reviews.md#review-of-2026-10-13) — previously implicit in NTF-7.* | Leave them unhandled: replay statistics and dedupe differ by type | A payment notification type is introduced (it would then duplicate the order outcome, NTF-7) |

**Why a service at all.** It passes all four parts of the ownership test: it alone writes notifications, delivery attempts and device cursors; it protects at-most-one per `(event, user, channel)`; its requirements (templates, channels, preferences, push) move for reasons unrelated to orders or stock; and its profile — bursty, fully async, failure-tolerant, connection-holding — is unlike any neighbour's. Folding it into `order` would put SSE connections and email retries inside the service with the strongest consistency need, and make a notification outage a checkout risk.

**Why push rather than polling, and why this shape of push.** [Part A](../part-a.md) §1 names unreliable order confirmations and dispatch updates during traffic spikes as a problem to solve, and B9 makes checkout asynchronous — so there is a real gap between "we got your order" and "your order is confirmed" that something has to close. Polling closes it badly: latency is bounded below by the poll interval, and the load is proportional to *idle* users rather than to events, which is exactly backwards for a system whose stated problem is behaviour under spikes. A poll every 10 seconds across 20,000 idle users is 2,000 req/s of mostly-empty responses; the same users on SSE are 20,000 idle sockets and zero requests.

The non-obvious part is not choosing SSE — it is C16, that push must sit *over* a durable store rather than replace it. Most of the complexity in real-time delivery comes from trying to make the connection itself reliable: acknowledgements, per-connection buffers, replay logs, sticky routing. None of that is needed here — and the acknowledgement this RFC does keep (C23) is a *report* that nothing depends on, never a guarantee — because the notification is already durable in Postgres before anything is pushed. That lets the fan-out bus be at-most-once Redis Pub/Sub instead of a second Kafka topic or a Redis Stream with consumer groups, and it lets a replica die mid-frame without anyone needing to know. The `seq` cursor (C18) then converts every failure mode — dropped frame, killed pod, closed laptop — into the same cheap catch-up query. One mechanism covers all of them, which is why the design is small.

**Why a server-side cursor and a bound.** `Last-Event-ID` survives only `EventSource`'s own reconnects. Without a server floor, a page reload is a full-history replay; without a bound, a device away for a week is shown a week of toasts. Both are UI incidents dressed as reliability. The durable list has always been the mechanism for "returning later"; the stream only has to be right about "just now".

**Why origin scoping, and why it is safe.** A toast is an interruption; delivering it to a device the user is not using is noise, and on a phone in a pocket it is a false alarm about money. But scoping is only safe because it narrows *frames* and nothing else — the count and the list are global, so the worst a wrong scope rule can do is misplace a toast. `notification_undelivered_age_seconds` exists to catch the one way that could go unnoticed.

**Why at-most-one per channel, enforced twice.** `processed_events` makes the consumer idempotent per event; the unique `(event, user, channel)` index makes the *outcome* idempotent even if a future consumer group, a replay tool, or a manual backfill bypasses the dedupe table. The second guard is what turns "we dedupe" into an invariant the database arbitrates (S11).

**Why rendering at consume time.** The rendered text is a record of what the user was told. Rendering on read would change history whenever a template changes and would put template code on the hot read path. Keeping `payload` alongside makes re-rendering possible deliberately, never accidentally.

**Why Celery for dispatch.** Sending an email is "do one job soon, with per-task retry" — the Celery shape (A12, C7). Doing it inside the consumer would hold a Kafka partition hostage to an SMTP timeout.

### 3.3. Tradeoffs

| Trade being made | The cost | Why it is acceptable |
| --- | --- | --- |
| Email and SMS not actually delivered | The demo shows a record for those channels, not a mailbox | `in_app` *is* delivered, over SSE, so the path is demonstrably end to end. No provider account is in scope, and the interesting engineering — consume, dedupe, render, push, retry, record — is all present |
| `notification` becomes a stateful, connection-holding service | Deploys must drain; capacity is sockets as well as RPS; the gateway needs streaming-specific configuration | Real-time order and shipment updates are the product requirement, not a nicety. The cost is bounded by C20: no sticky sessions, no replica-affine state, so scaling stays ordinary |
| A server-side per-device replay cursor | A table, a write path, and a second place a replay position can be wrong | Without it a page reload is a full-history replay, because `EventSource` keeps `Last-Event-ID` only across its own reconnects. One small row per device is the cheapest correct answer, and losing the table degrades to a cold start rather than to loss |
| Bounded catch-up — 50 frames, 24 hours | A client away for a week is told a number instead of being shown frames | The durable list has always been the mechanism for "returning later". Replaying a week of notifications as *live* frames is not a resume, it is a UI incident |
| A short-TTL presence key — a partial retreat from C20 | A shared registry exists where the design said none would | It carries no routing information, so nothing becomes sticky; it expires on its own; and both error directions cost at most a delayed or over-broad toast. The alternative — always broadcast after a delay — is one line away if it disappoints |
| A client-minted device id | Spoofable, unauthenticated, and a device correlator in the database | Always scoped under the authenticated `user_id` and never an authorisation input (§6.2), so the worst a forger achieves is affecting their own toasts. Rows per user are capped at 20 |
| At-most-once fan-out over Redis Pub/Sub | A dropped message means a notification arrives on reconnect rather than instantly | Correct under C16 — the row is already durable. Buying at-least-once here would mean Redis Streams or a second Kafka topic, to protect data that is not at risk |
| A single-use ticket instead of header auth on the stream | One extra round trip before a stream opens, and a second auth path to maintain | 60 ms once per connection, against keeping JWTs out of URLs and logs. The alternative that avoids both is WebSockets, which costs far more (§5, Alternative 10) |
| One row per channel (NTF-2) | An event with two channels writes two rows; list queries filter by channel | The invariant is per channel, so the row must be. The filter is on an indexed path and the alternative — one row with a channel array — makes at-most-one unenforceable by a constraint |
| Derived lifecycle instead of a status column (NTF-13) | "What state is this in?" is a small expression, not a column read | The three facts have three writers and no order between them; a status would be a fourth thing to keep consistent |
| Unique index on a `jsonb` expression (NTF-1) | Slightly unusual; the key lives inside `payload` | Keeps the catalogued schema intact; a dedicated column is one additive migration if review prefers it |

#### 3.3.1. Challenges and Open Questions

IDs from the archive are kept; the source RFC is named because archive/0003 and archive/0006 both number from Q1.

| # | Source | Question | Current position | Needs a decision by |
| --- | --- | --- | --- | --- |
| Q2 | archive/0003 | One consumer process per service, or per topic? | Per service, subscribing to several topics, with the dedupe keyed by consumer group. Fewer containers, and the ordering guarantee is per key anyway | Decided |
| Q11 | archive/0003 | One Pub/Sub channel per connected user, or a fixed set of sharded channels every replica subscribes to? | Per user. Redis handles tens of thousands of channels comfortably, and it means a replica receives only the messages it can deliver. Sharding trades that for fewer subscriptions and is worth revisiting only if `SUBSCRIBE` churn shows up in Redis latency | Revisit under load |
| Q12 | archive/0003 | Should the SSE stream also carry non-notification events — order status, inventory changes? | No. One stream per concern keeps the contract legible; a general-purpose client event bus is a design decision that deserves its own RFC rather than arriving as a side effect of notifications | Part A review |
| Q13 | archive/0003 | Does push fit alongside the rest of the event backbone's delivery? | T16a–T16h are ~2 days on top of the notification tasks because Redis and the gateway already exist. If time slips, T16a + T16f (single-replica SSE with *bounded* catch-up) deliver the user-visible behaviour, and T16b's fan-out with T16h's scoping follow (see Delivery phasing). The bounded catch-up is not part of the reducible scope: the unbounded version is the bug this design was revised to remove | Decided, with the fallback stated |
| Q14 | archive/0003 | Should a cold-start client get *zero* frames, or the newest few unread as a courtesy? | Zero, plus `unread_count` in the `hello` frame. Any non-zero number is an arbitrary constant that will be argued about forever, and the list is one request away | Decided; revisit on UX feedback |
| Q15 | archive/0003 | Is `origin_first` promotion worth a sweeper, or should origin-scoped types simply never promote? | Worth it, for `order_failed` specifically — a payment failure the user never sees is the worst outcome in the set. Promotion is one broadcast, idempotent and bounded | Decided |
| Q16 | archive/0003 | Should `delivery_scope` be a per-user *preference* rather than a per-type constant? | No, not yet. The `channel` column already carries the preferences deferral (§2.2); scope would join it there | Part A review |
| Q17 | archive/0003 | Two tabs in one browser share a `client_id` and therefore a cursor. Per-tab ids instead? | No. Per-tab ids make every new tab a cold start, which defeats C22. The granularity of the report is "this browser profile saw it", and the unread badge is what recovers the lagging tab. The limitation is recorded in §4 | Decided |
| Q18 | archive/0003 | Does `client_id` need to survive logout? | Yes — it is a device correlator, not a session. It is never used for authorisation, and `stream_cursors` rows are keyed `(user_id, client_id)`, so a second user on the same browser gets their own row | Decided |
| Q6 | archive/0006 | Should the `†` inferred types (5 in `delivery_attempts`, 6 in `push_subscriptions`) be ratified, or re-decided? | Ratified as a block, since each follows mechanically from S1–S15. Any row a reviewer disputes becomes a decision here | Part A review |
| Q7 | archive/0006 | Is `origin_client_id` a legitimate fifth reference kind ([RFC 0000 §3.1.3.5](0000-architecture.md#3135-cross-boundary-reference-kinds)), or a snapshot of an untrusted hint? | A fifth kind — the *opaque client token*. Calling it a snapshot implies a source of truth it does not have, and calling it a claim implies verification that never happened. It is a token the client invented, compared only for equality, and always scoped under a verified `user_id` | Part A review |
| NTF-Q1 | new | Who writes `orders.notified_at`, which exists "so a notification failure is visible on the order" and is the first hop of the "no notification" walkthrough? | Unresolved. `notification` cannot write `order_db` (R1) and produces no events. Options: (a) `order`'s `notify` activity stamps it when it writes the outbox row — then it means "notification requested", not "delivered"; (b) `notification` gains an outbox and a `notification.delivered` event `order` consumes — a new producer for a cosmetic column. Position: (a), documented as "requested at", with the delivery truth in the admin view. Owned jointly with [RFC 0005](0005-order-service.md) | Part A review |
| NTF-Q2 | new | [Part A](../part-a.md) §1 names *refund notices*; there is no refund event or `NotificationType` | No refund flow exists in `order` to notify about. When one does, it is a new type and a new topic — additive | When `order` gains refunds |
| NTF-Q3 | new | archive/0002 Q8 promised a merchant "a notification" when a publish fails, but `notification` does not consume `catalog.*` and there is no type for it | Not in scope here; the merchant uses `publish-status` and the failures table ([RFC 0004](0004-catalog-service.md)). Adding it needs a `catalog.product.publish_failed` topic and a `publish_failed` type | Part A review |
| NTF-Q4 | new | Retention of `notifications` and `delivery_attempts` | None specified; rows grow without bound. Candidate: keep 180 days, sweep on Beat. Catch-up only looks back 24 h, so retention affects the list and support, not delivery | Before any production use |
| NTF-Q5 | new | archive/0006 notes `channel` as `in_app \| email \| sms`, while archive/0003 says the channel enum "gains `push`" | `push` is in the `NotificationChannel` vocabulary (S4 makes that a one-line code change, no migration); the column note is copied verbatim from the catalogue and should be amended there at ratification | Part A review |
| NTF-Q6 | new | At-most-one via `payload->>'event_id'` (NTF-1) or an explicit `event_id uuid` column? | Expression index, to keep the catalogued columns exact. An explicit column is one additive migration | Part A review |
| NTF-Q7 | new | `stream_cursors.user_agent` is "for the admin device list only", but no admin device-list endpoint is specified | Candidate: `GET /api/v1/notifications/admin/devices?user_id=` returning `client_id`, `user_agent`, `last_seen_at`, `cursor_seq`. Not added until a need is stated | Part A review |
| NTF-Q8 | new | `nt:conns:{user_id}` is one counter per user refreshed by any heartbeat, so a crashed replica's slots only expire once *all* the user's connections are gone | Accepted: the cap is 3 and the effect is a `429` that clears within 60 s of the user's last live stream closing. A per-connection key set would fix it at the cost of a `SCAN` on open | Revisit if seen |

---

## 4. Dependencies/blockers/known issues

**Dependencies.**

| Dependency | Version | Used for |
| --- | --- | --- |
| PostgreSQL | 16 | `notification_db` |
| `confluent-kafka[avro]` | 2.6.x | Consumer, via the `common` base class |
| Celery | 5.4.x | Dispatch and Beat tasks (hosted by `celery-worker`) |
| Redis + `redis[hiredis]` | 7.4 / 5.x | Tickets, Pub/Sub fan-out, presence, caps, rate limits |
| `sse-starlette` | 2.x | SSE responses in FastAPI — the same dependency the assistant stream takes ([RFC 0009](0009-assistant-service.md)) |
| `uvloop` | current | Connection density |
| OpenTelemetry SDK | 1.28.x | Traces and metrics |
| `pywebpush` | — | Only if the `push` adapter is registered |

**Upstream contracts this service depends on.** `client_id` in the envelope ([RFC 0000](0000-architecture.md#3124-event-envelope)); `orders.origin_client_id` ([RFC 0005](0005-order-service.md#orders)); the payload fields in §3.1.2.4 from `order`, `inventory` and `identity`; the gateway's SSE pass-through and `X-Client-Id` handling ([RFC 0002](0002-gateway-service.md)); NTF-14 in the consumer base class ([RFC 0001](0001-platform-and-shared-infrastructure.md)).

**Blockers.** None.

**Known issues.**

- `in_app` notifications are pushed over SSE; email, SMS, and Web Push are recorded and rendered but not sent.
- A notification for a user with no open connection waits until they reconnect — there is no offline push (§2.2), so "real-time" means "real-time while the app is open".
- Redis is a single node and Pub/Sub is at-most-once: a Redis restart drops in-flight frames, and clients recover on reconnect rather than instantly.
- Two tabs in one browser share a `client_id` and therefore a cursor (Q17), so a tab that never rendered a frame the other tab acked will not be re-shown it. Safe only because the unread count and the durable list are never cursor- or scope-filtered.
- A client that never acks cold-starts on every reload and has every origin-scoped notification promoted to broadcast after the grace period. Supported, and identical to the pre-ack behaviour (C23).
- An origin-scoped toast can be delayed by up to ~75 seconds when the originating device disappears without a clean close — the presence TTL plus the promotion grace plus the sweep interval.
- A promoted notification may reach the origin device twice (targeted, then broadcast); the client de-duplicates by `id`.
- `orders.notified_at` has no specified writer (NTF-Q1).
- No retention on `notifications` or `delivery_attempts` (NTF-Q4).
- The email adapter has no address to send to; a real provider needs an address projection from `identity`.

---

## 5. Alternate solutions

**Alternative 9 — Keep polling: the client calls `GET /notifications` on a timer.** No connections to hold, no fan-out bus, no gateway changes, and `notification` stays a plain request/response service.

*Rejected,* and it is the alternative this design exists to argue against. Two problems, both structural. Latency is bounded below by the poll interval, so "your payment failed" arrives up to ten seconds late by construction. And load scales with *idle users* rather than with events: 20,000 clients polling every 10 s is 2,000 req/s of almost-always-empty responses, each one a JWT verification and an indexed query, and the rate rises precisely when a spike makes users open the app to check on their orders. [Part A](../part-a.md) §1 names spike behaviour as the thing that is broken today, so a delivery mechanism whose cost peaks with anxiety is the wrong shape. Polling survives in one place only — as the client's fallback when SSE cannot connect.

**Alternative 10 — WebSockets instead of SSE.** Bidirectional, one well-known protocol, no ticket dance because auth can happen in the first frame.

*Rejected,* for the reasons E4 rejects it for the assistant. The traffic is server → client only; nothing the client sends mid-stream matters, and marking a notification read is an ordinary `POST`. SSE is plain HTTP, so the existing gateway routing, JWT verification, rate limiting, tracing, and RFC 9457 error handling apply unchanged, and `EventSource` reconnects with `Last-Event-ID` natively — which is the entire recovery mechanism in C18, for free. WebSockets would mean a separate connection lifecycle, a stateful gateway path, a bespoke auth handshake, and a hand-written reconnect-and-resume client, to buy a bidirectionality no requirement asks for. It becomes the right answer the day the product needs typing indicators or client→server streaming; it is not that day.

**Alternative 11 — Every replica joins Kafka with its own consumer group and filters locally.** No Redis Pub/Sub, no second bus: each replica sees every event and delivers to whichever users it happens to hold.

*Rejected,* though it is elegant and genuinely tempting. It removes a moving part, but it makes consumer groups dynamic — created and abandoned per pod — which leaves offset garbage in the broker on every deploy, and it means every replica deserialises every event to discard most of them. Worse, it entangles the *delivery* path with the *persistence* path: the same consumer that must be idempotent and dedupe-backed (C4) would now also be the one fanning out to sockets, so a slow client and a stuck partition become the same problem. Splitting them is the point of C17 — Kafka consumption is partitioned and durable; delivery fan-out is broadcast and disposable.

**Alternative 12 — Redis Streams or a second Kafka topic for fan-out, giving at-least-once push.** Frames survive a Redis restart; delivery could be acknowledged per connection.

*Rejected* as protecting data that is not at risk. Under C16 the notification is already durable in Postgres before anything is published, so the fan-out bus is carrying a hint, not a record. Paying consumer-group bookkeeping, per-connection offsets, and trim policies to make that hint reliable buys a few seconds of latency in a rare failure — and the `seq` catch-up (C18) already covers that case with a query the system has to support anyway, since it also handles first connect, reconnect after sleep, and pod restart. **Sticky sessions**, the third variant of this idea, are rejected on the same ground plus the scaling cost they impose on every deploy.

**Smaller rejected alternatives**, each recorded where it was decided:

| Alternative | Rejected because |
| --- | --- |
| JWT in the stream's query string | Leaks into access logs, proxy logs and `Referer` headers (C19) |
| One Pub/Sub channel per device | The consumer cannot derive device channel names, and `SUBSCRIBE` churn multiplies for a three-line predicate at the edge (C17) |
| Sharded Pub/Sub channels every replica subscribes to | Each replica would receive messages it cannot deliver; revisit only under measured churn (Q11) |
| A connection registry mapping clients to replicas | Makes routing sticky — the line C20 must not cross |
| No presence key; always target, always let the promoter widen | Makes the 45 s delay the common case rather than the failure case. Recorded fallback |
| A per-notification countdown task for promotion | A task storm and a scheduling dependency for a cosmetic outcome |
| Origin-scoped types that never promote | A payment failure the user never sees is the worst outcome in the set (Q15) |
| Unbounded catch-up `WHERE seq > cursor` | Replays a lifetime of notifications as live frames on a fresh device (C22) |
| Sending the newest 50 on truncation | Leaves a hole below the sent range the cursor cannot advance past |
| Courtesy frames on cold start | An arbitrary constant argued about forever (Q14) |
| Per-tab `client_id` | Every new tab becomes a cold start, defeating C22 (Q17) |
| Stamping `delivered_at` by range | Reports deliveries scope filtering prevented |
| Cursor in Redis, or a write-behind `nt:cursor:*` cache | A Redis flush makes every device a cold start; a second copy of a cursor is a second thing to be wrong |
| Deriving `nt:conns:*` by counting `nt:client:*` | A `SCAN` on the connection-open path |
| A per-user `seq` sequence | Needs a lock or a second table for no benefit; the catch-up filters by `user_id` anyway |
| Naming `stream_cursors` `client_devices` | Names the side effect, not the job |
| `origin_client_id` as `uuid` | The server does not get to assume the client mints one |
| Ack as at-least-once delivery protocol | Rebuilds TCP badly over a store that is already durable (C16, C23) |
| Kafka transactions for exactly-once notification | The guarantee does not extend to a Postgres write or an email; consumer idempotency is needed regardless |

---

## 6. Production readiness

### 6.1. Scalability and Fault tolerance

- **Connection scaling is horizontal and unsticky.** Replicas hold sockets but no *replica-affine* state (C20). The two pieces of shared state — the per-device replay cursor in Postgres and the presence key in Redis — belong to no replica and both fail safe, so any replica serves any reconnect, no session affinity is configured, and adding a replica adds capacity with no rebalancing. ~10k idle SSE connections per replica; [Part A](../part-a.md) §4's tens of thousands of concurrent users is 2–4 replicas.
- **Consumption scales to the partition count.** Three partitions per topic locally; replicas beyond three hold connections but idle as consumers.
- **Push failure degrades to latency, never to loss.** The notification is committed to Postgres before it is published (C16), so a dropped frame, a Redis restart, a killed replica, or a client that was offline all resolve identically: the next connection replays from `seq` (C18). There is no failure mode in the delivery path that loses a notification. There are failure modes that **downgrade** a frame to a list entry — a cold-started device (C22), a gap past the resume bound, an origin-scoped notification whose device never returns (C21) — and each of those is visible in the unread count and in the durable list, which are never scope- or cursor-filtered.
- **Slow clients cannot consume the server.** Each connection has a bounded queue; on overflow the connection is closed rather than buffered, which is safe precisely because reconnect is lossless. Unbounded per-connection buffering is the usual way a push system dies under load, and it is designed out rather than tuned.
- **Poison messages cannot stall a partition** (C6); **outages do not empty into the DLQ** (NTF-14).
- **Idempotent everywhere.** The consumer dedupes twice; `dispatch_notification` no-ops once a `sent` attempt exists; the promoter, the cursor sweep and the redispatch sweep are safe to run twice or concurrently; ack, mark-read and read-all are idempotent.
- **Backpressure.** Kafka retains and the consumer lags; the `notifications` RabbitMQ queue buffers dispatch independently of `media`; the stream cap and per-connection queue bound socket-side memory.
- **Blast radius.** `notification` down: events accumulate in Kafka and are processed on recovery — checkout is unaffected.
- **Known ceilings.** One Redis node; Pub/Sub at-most-once; one Postgres instance shared by logical databases. None is a design decision that blocks scaling.

### 6.2. Security considerations

- **Notification isolation.** A user reads only their own; another user's id is a `404`. The admin view requires `admin`. The SSE stream is subscribed to `nt:user:{user_id}` from the *redeemed ticket*, never from a client-supplied parameter, so one user cannot subscribe to another's channel.
- **`client_id` is client-minted and therefore forgeable, and is treated as such.** It is stored and queried only as part of `(user_id, client_id)`, with `user_id` taken from a verified token, so cross-user reach is unrepresentable rather than merely denied. It is never an authorisation input, never resolves a channel — the channel comes from the redeemed ticket — and never appears in a URL. It is a device correlator: not reused for analytics, pruned at 90 days, and capped at 20 rows per user, which is what closes the one real abuse (resource growth by a client that rotates its id, not access).
- **Stream tickets are single-use and short-lived** — 60 seconds, redeemed with an atomic `GETDEL`, so a ticket captured from a log or a browser history is almost always already spent. This is why the JWT itself is never placed in the stream URL (C19). Tickets are never logged by `notification`.
- **Long-lived connections are re-authorised, not authorised once.** A stream carries the token's expiry: when the underlying JWT would have expired, the stream closes with a typed `error` frame and the client re-authenticates. Otherwise a connection opened before a logout or a merchant suspension would outlive the decision indefinitely.
- **The ack endpoint leaks nothing.** `204` for unknown and foreign ids alike; foreign rows are untouched because every update is guarded by `user_id`.
- **Payload minimalism.** Events carry identifiers and the minimum needed to render; `notification` stores the envelope in `payload` for support, so the same minimalism bounds what this database holds. No credentials, no card data, no addresses.
- **Push subscriptions**, if ever used, hold client key material (`p256dh`, `auth`); the push payload carries `{id, type, title}` only.
- **Traces** may contain URLs and user ids; bodies are never span attributes; headers are allow-listed.
- **Accepted gaps.** Redis without auth on the internal network; no TLS between gateway and service. Local-environment choices, listed so they read as choices.

### 6.3. Deployment strategy

- Runs under the `events` Compose profile with Kafka, Redis, RabbitMQ and Celery ([RFC 0001](0001-platform-and-shared-infrastructure.md)); `make migrate` applies its Alembic history. Health-gated `depends_on` on Postgres, Redis and Kafka; the consumer retries broker connection with backoff rather than crash-looping.
- **Connection draining on deploy.** A rolling restart closes streams with a jittered `retry:` hint and a grace period rather than dropping them, so reconnects spread instead of arriving as a thundering herd. No sticky sessions or session affinity are configured, by C20.
- **The gateway needs streaming-specific configuration** — proxy buffering off, no compression on `text/event-stream`, an extended read timeout, and the per-IP rate limit exempted on the stream route. Misconfiguring any of these degrades silently into "notifications feel slow" rather than failing loudly, which is why T16c tests it by measurement.
- **Consumers deploy with the service but never interrupt its API**; a restart resumes from committed offsets.
- **Rollback.** Alembic downgrade per revision; vocabularies read with `passthrough`/`fallback` so a rolled-back build reading a newer value does not 500 (S4). A bad consumer is rolled back and its offsets rewound if needed; the at-most-one index makes a rewind safe.

---

## 7. Test Plan

**Unit**

- Template rendering for each of the six types, including an unknown `failure_reason` and each shipment status.
- The scope-derivation function over all twelve `type` × origin-presence combinations, and the catch-up bound calculator.
- Recipient and channel resolution for every consumed topic, including `payment.*` and `shipment created` producing nothing.
- Per-connection frame filter truth table, including ephemeral (null `client_id`) connections.
- Ack clamp arithmetic; backoff schedule for NTF-5.

**Integration — recording** (real Kafka, Postgres, Redis)

- Each of the six notification types is produced from its event with correct rendering and `user_id` (G5); `in_app` and `email` rows per the channel table.
- **Replay: reset the `notification` group to earliest, re-consume everything, assert rows unchanged** (G3); the same event delivered twice inserts once — with `processed_events` truncated, the NTF-1 index still holds.
- A malformed event, or one missing a required payload field, goes to DLQ; the next event is processed normally (C6).
- `notification_db` down during consumption → the consumer pauses and nothing reaches the DLQ; on recovery every event is recorded (NTF-14).
- Dispatch failure is retried by Celery and recorded as attempts; after five failures the notification shows as exhausted in the admin view; a crash between commit and enqueue is recovered by the redispatch sweep (NTF-6).
- Isolation: another user's notification → `404`; admin view → `403` for customer and merchant.
- List filters, `since_seq` ascending paging, `read-all` count, `unread-count`; mark-read idempotent.

**Integration — delivery (SSE)**

- **Two replicas: a client streams from replica A, the event is consumed by replica B, the frame arrives at the client** (G5a). This is the test that proves C17; a single-replica test would pass without any fan-out at all and is therefore not sufficient.
- **Resume: connect, receive up to `seq` N, kill the connection, publish M more, reconnect with `Last-Event-ID: N` → exactly M frames, in `seq` order, no duplicates, no holes** (G5b).
- **Cold start**: 500 seeded notifications, an unseen `client_id`, no `Last-Event-ID` → **exactly 0** `notification` frames, one `hello` with `cold_start: true` and the exact `unread_count`; a notification published afterwards arrives live (G5c).
- **Warm resume through the server cursor, with no `Last-Event-ID`**: ack to `seq` N, drop the connection, publish M, reconnect *without* the header → exactly M frames. This is the page-reload case, and a design without a server cursor cannot pass it.
- **Bound enforcement**: 200 missed → 50 frames in ascending `seq` plus `catchup_truncated{omitted_count: 150}`; `GET /notifications?since_seq=` returns the rest (G5b).
- **Age bound**: a notification 25 hours old is not replayed even when the cursor sits below it, and is present in the list.
- **Subscribe-before-catch-up** (NTF-10): a notification committed while the catch-up query runs is delivered exactly once.
- **Ack idempotency**: the same payload twice → one `delivered_at`, an unchanged `delivered_client_id`, and an unmoved cursor (G5e).
- **Ack safe-seq clamp**: commit `seq` 84215 before 84214, ack 84215, reconnect after the 5-second window → 84214 is still delivered. The visibility trap, asserted against the ack rather than only against catch-up.
- **Ack of a foreign id**: user B acks user A's notification id → `204`, and A's row is untouched.
- Ack rate limit: the 13th ack in a minute for one `(user, client)` → `429`; a different `client_id` is unaffected.
- **Origin targeting**: clients A and B for one user; `order_confirmed` with `origin_client_id = A` → the frame reaches A only, the `unread_count` frame reaches **both**, and `GET /notifications` on B contains it (G5d).
- **Null origin**: `shipment.status_changed` from the carrier webhook → delivered to both A and B, despite the scope table.
- **Promotion**: `order_failed` with origin A while A is disconnected → `promoted_at` set (immediately, by NTF-9, when A's presence key is absent), frame at B within the stated bound; running the promoter twice promotes once.
- **Presence false positive**: kill A's process without a clean close → the targeted frame is delivered nowhere, the key expires, and the promoter widens it.
- **`welcome` is `origin_only`**: registration from A with B connected → frame at A only; never promoted; in B's list.
- **Spoofing**: user B opens a stream with a ticket minted from B's token but carrying A's `client_id` string → B's own stream, A's notifications never reachable, and a `stream_cursors` row created under B.
- **Client-id cap**: 30 distinct `client_id`s for one user → at most 20 rows survive the sweep, evicted by `last_seen_at`; rows older than 90 days are deleted.
- **Ephemeral stream** (NTF-12): a ticket minted without `X-Client-Id` → no `stream_cursors` row; `origin_only` frames never arrive; `all_devices` frames do.
- **Gateway**: a malformed `X-Client-Id` is stripped and the mutation still succeeds, with `origin_client_id IS NULL`.
- **Avro compatibility**: a v1-schema consumer reads a v2 record (unknown `client_id` ignored); a v2 consumer reads a v1 record (`client_id` resolves to null).
- **The `seq` visibility window**: two notifications committed out of sequence order are both delivered, and the client de-duplicates by `id`.
- A stream ticket is single-use: the second redemption of the same ticket → `401`. An expired ticket → `401`. A ticket for user A cannot stream user B's channel.
- A connection whose token has expired is closed with a typed `error` frame rather than left open.
- The 3-stream-per-user cap rejects the fourth; heartbeat frames keep an idle connection alive past the proxy timeout.
- A slow consumer overflows its queue, is disconnected, and recovers its missed notifications on reconnect — asserting that dropping a connection is lossless.
- Redis down: list and count endpoints still succeed; `stream-ticket` → `503`; `origin_first` notifications recorded meanwhile are promoted at insert.
- Through the gateway: the first frame is not buffered, and push latency from consume to client receipt is measured (G5a, T16c).
- Mark-read on device A → device B receives an `unread_count` frame (NTF-11).

**Observability**

- A checkout produces one trace whose spans include the `notification` consumer and the `dispatch_notification` task — asserted through the Jaeger API, not by eye.
- `/metrics` exposes every §3.1.1.7 instrument non-null after the delivery suite; active SSE connections moves with open streams.
- Every log line for one consumed event carries the same trace id.

**End-to-end** (through the gateway, full stack)

1. Register a customer from client A with a stream open → `welcome` frame on A.
2. Place and confirm an order **with an SSE stream already open for the customer** → a notification row, **the frame received on the open stream within a second**, an `email` row with a `sent` attempt, and one trace spanning gateway, order, the workflow, the relay and the notification consumer.
3. Place an order that fails payment → `order_failed` recorded and pushed; `payment.failed` produces no second notification.
4. Order a product to exhaustion → the merchant receives `stockout`.
5. Advance the shipment through `shipped` and `delivered` via the signed webhook → two `shipment_update` notifications on every device.

**Review walkthroughs** — the two §3.1.1.7 symptoms, each followed from symptom to cause.

**Explicitly not tested** — load beyond functional concurrency (the ~10k connections per replica figure is an estimate, not a measurement); real email, SMS or Web Push providers; alerting.

---

## 8. Impact Analysis

**On other services.**

| Service / RFC | Change required | Why it cannot live here |
| --- | --- | --- |
| [RFC 0000](0000-architecture.md#3124-event-envelope) | The envelope carries `client_id` (`"opaque client token \| null"`); Avro `["null","string"]` with a `null` default — a `FULL`-compatible change | The envelope is defined once, there |
| [RFC 0005 — order](0005-order-service.md#orders) | `orders.origin_client_id`, written at creation and read by every activity that writes an outbox row; `orders.notified_at` (NTF-Q1); payload fields in §3.1.2.4 | `order.confirmed`/`.failed` are emitted by the workflow after the request ended |
| [RFC 0006 — inventory](0006-inventory-service.md#3123-events-produced) | `inventory.stockout` carries `merchant_id`, `product_name`, `sku` | The stockout recipient must be resolvable from the event alone (R4) |
| [RFC 0003 — identity](0003-identity-service.md#3123-events-produced) | `identity.user.registered` carries `role` and the registration request's `client_id` | `welcome` is `origin_only` |
| [RFC 0002 — gateway](0002-gateway-service.md) | SSE pass-through; `X-Client-Id` pass-through with shape validation and stripping; stream route exempt from per-IP rate limit | Edge concerns live in one place |
| [RFC 0001 — platform](0001-platform-and-shared-infrastructure.md) | Celery queues `notifications` and `maintenance`; Beat schedules for the four tasks; NTF-14 in the consumer base class; dashboard rows | Hosting and runtime are the platform's |
| [RFC 0009 — assistant](0009-assistant-service.md) | None; it shares `sse-starlette` and the gateway's streaming path, which land here first | — |

**On effort.** One service, four tables, four Celery tasks, one consumer and eight endpoints. T16a–T16h are ~2 days on top of the recording tasks because Redis and the gateway already exist.

**Risks.**

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| SSE push added to the heaviest part of the schedule | Medium | Medium | T16a–T16h sit on infrastructure that already exists, so the marginal cost is ~2 days. Q13 states the fallback: ship T16a + T16f for single-replica push with bounded catch-up, and move fan-out and scoping after |
| An implementer scope-filters the unread count or the notification list "for consistency" | Medium | **High** | This is the one change that turns origin targeting from a UX decision into silent notification loss. C21 states the invariant in the constraint itself, I6 records it, and G5d asserts that the non-origin device's badge still moves |
| Gateway buffering silently degrading push into delayed polling | Medium | Medium | The failure looks like "notifications feel slow", not like an error, so T16c asserts it by **measuring** first-frame latency through the gateway rather than by inspection |
| Consumer idempotency implemented shallowly and only failing on replay | Medium | High | The replay test is an acceptance criterion; the NTF-1 index backs the dedupe table |
| The consumer base class dead-letters during an outage | Medium | High | NTF-14 stated as a requirement on the platform; tested by the database-down test |
| Producers omit a payload field `notification` needs | Medium | Medium | NTF-8 lists them; a missing field is a DLQ entry, visible on the dashboard, not a wrong notification |

---

## 9. Useful Links/References

**Project documents**

- [RFC 0000 — Architecture](0000-architecture.md): [ownership test](0000-architecture.md#3112-the-ownership-test) · [R1–R8](0000-architecture.md#3113-boundary-rules-r1r8) · [A1–A14](0000-architecture.md#3115-cross-cutting-decisions-a1a14) · [API conventions](0000-architecture.md#3122-api-conventions) · [event catalogue](0000-architecture.md#3123-event-catalogue) · [envelope](0000-architecture.md#3124-event-envelope) · [S1–S15](0000-architecture.md#3131-schema-conventions-s1s15) · [shared tables](0000-architecture.md#3132-shared-tables--outbox-and-processed_events) · [reference kinds](0000-architecture.md#3135-cross-boundary-reference-kinds)
- [RFC 0001 — Platform](0001-platform-and-shared-infrastructure.md) · [RFC 0002 — Gateway](0002-gateway-service.md) · [RFC 0003 — Identity](0003-identity-service.md) · [RFC 0005 — Order](0005-order-service.md) · [RFC 0006 — Inventory](0006-inventory-service.md) · [RFC 0009 — Assistant](0009-assistant-service.md)
- [Part A](../part-a.md) §1, §4, §6 · [Milestones](../milestones.md) · [Execution Guidelines](../execution-guidelines.md)
- Archived sources: [archive/0000](archive/0000-system-architecture.md) · [archive/0003](archive/0003-events-and-observability.md) · [archive/0006](archive/0006-service-decomposition-and-data-model.md)

**Technical references**

- HTML Living Standard, Server-Sent Events (`Last-Event-ID`, `retry:`, reconnection) — https://html.spec.whatwg.org/multipage/server-sent-events.html
- `sse-starlette` — https://github.com/sysid/sse-starlette
- Redis Pub/Sub — https://redis.io/docs/latest/develop/interact/pubsub/
- Why `bigserial` gaps break naive cursors — https://www.postgresql.org/docs/16/functions-sequence.html
- Web Push protocol / VAPID (the deferred offline channel) — https://datatracker.ietf.org/doc/html/rfc8292
- Celery routing and task design — https://docs.celeryq.dev/en/stable/userguide/routing.html
- Transactional outbox — https://microservices.io/patterns/data/transactional-outbox.html

---

## 10. Related tickets/issues

| ID | Title | Maps to |
| --- | --- | --- |
| *(to be created)* | T14, T15, T16, T16a … T16h | §3.1.5, one ticket each |
| *(to be created)* | NTF-Q1 — writer of `orders.notified_at` | With [RFC 0005](0005-order-service.md) |
| — | archive/0002 Q8 | Merchant publish-failure notification — not closed here (NTF-Q3) |
| — | archive/0006 Q6, Q7 | Ratified at the Part A review |

---

## 11. Notes

- **The one sentence to keep.** The row is the notification; the frame is a courtesy. Every design choice above — at-most-once fan-out, bounded catch-up, cold start, scoping, the ack as a report — is licensed by that and would be wrong without it.
- **On the lifecycle.** "Delivered" here always means "a client reported rendering it". A notification with `delivered_at IS NULL` is not failed; it may well be read. The admin view and `notification_undelivered_age_seconds` are how the difference is observed.
- **On reading the archive.** archive/0003's C1–C14 (relay, Avro, Celery split, OTel, Redis jobs, profiles) are platform decisions and live in [RFC 0001](0001-platform-and-shared-infrastructure.md); only C4, C6, C7 and C11 are restated here, as they bind this consumer. The archived comment note referred to decisions C1–C20 and questions Q1–Q13; the current ranges are C1–C23 and Q1–Q18.
- **Amendment log.**

  | Date | Section | Change | Reason |
  | --- | --- | --- | --- |
  | Oct 6, 2026 | All | `libs/common` renamed `common/` in every reference | [Mentor review 2026-10-06 §1](../reviews/mentor-reviews.md#1-shared-package-folder) |
  | Oct 11, 2026 | §3.2.1 | Added the decision register (NTF-D1–NTF-D24); existing C*/NTF-* IDs kept and referenced | [Mentor review 2026-10-06 §2](../reviews/mentor-reviews.md#2-a-reason-for-every-decision) |
  | Oct 11, 2026 | §3.1.1, §3.1.1.3, §3.1.1.4, §3.1.2.1, §3.1.3 | Converted the ASCII architecture diagram to Mermaid; added the state, sequence, decision-flow and `erDiagram` diagrams | [Mentor review 2026-10-06 §4](../reviews/mentor-reviews.md#4-diagrams) |
  | Oct 11, 2026 | §3.1.1.1, §3.1.1.2, §3.1.3 | Layer-first layout (`api/v1/`, `services/`, `models/`) replaced by entity-first `notifications/`, `subscriptions/`, `stream/` with `<entity>/v1/` (NTF-D24) | [Mentor review 2026-10-06 §3](../reviews/mentor-reviews.md#3-entity-first-service-layout-with-versions) |
  | Oct 12, 2026 | §3.1.1, §3.1.1.1, §3.1.1.4, §3.2.1 | NTF-D25: the consumer moved to `notifications/consumers/`, the Celery tasks to `notifications/tasks/` and `stream/tasks/`; no app-level `consumers/` or `tasks/` | [Mentor review 2026-10-12 §2](../reviews/mentor-reviews.md#2-every-file-in-its-entity-folder) |
  | Oct 12, 2026 | §3.1, §3.2.1, §11 | Added the diagram index; review-changed register rows marked with the review and the previous decision; this log | [Mentor review 2026-10-12 §3](../reviews/mentor-reviews.md#3-every-change-traced-to-its-review), [§4](../reviews/mentor-reviews.md#4-diagram-coverage-per-service) |
  | Oct 15, 2026 | §3.2.1 | Milestone 3 implementation: register rows NTF-D26–NTF-D36 — scoped delivery, promotion and the presence key deferred (frames broadcast; scope still stored); no Web Push adapter; metrics/OTel and reconnect-storm tests deferred; fan-out mandatory with the consumer as its own process; one Celery queue; `StreamingResponse` instead of `sse-starlette`; one `PubSub` per replica; the after-commit re-read by `event_id`; no-op `payment.*` handlers | [Mentor review 2026-10-13](../reviews/mentor-reviews.md#review-of-2026-10-13) |
  | Oct 15, 2026 | §3.1.1.2, §3.1.2.4, NTF-8 | Templates and the NTF-8 field list aligned to the registered Avro schemas (`order_number`, `payment_status`, `to_status`, `tracking_number`, `sku`, `trigger`) (NTF-D33) | [Mentor review 2026-10-13](../reviews/mentor-reviews.md#review-of-2026-10-13) |
  | Oct 15, 2026 | §3.1.1.6 | As implemented: the database URL is the platform's `DATABASE_URL`, not `NOTIFICATION_DATABASE_URL`; `DISPATCH_MAX_RETRIES` (5) is the attempt ceiling — the fifth failed attempt is the last row written; adds `NOTIFICATION_FAKE_SMS_FAILURE_RATE`, `DISPATCH_STALE_PENDING_SECONDS` (60) and `STREAM_SUBSCRIBE_TIMEOUT_SECONDS` (2); the `PROMOTION_*` and `STREAM_PRESENCE_TTL_SECONDS` settings are unused until NTF-D26 is lifted; `POST /ack` accepts `up_to_seq` and `ids` as optional | [Mentor review 2026-10-13](../reviews/mentor-reviews.md#review-of-2026-10-13) |

---

## 12. Comments

*Reviewer comments go here. Please raise objections against the numbered decisions (C15–C23, NTF-1–NTF-16), a decision-register row (NTF-D1–NTF-D36), the open questions (Q11–Q18, NTF-Q1–NTF-Q8), or the rejected alternatives in §5, so the discussion attaches to something specific.*

| Reviewer | Section | Comment | Resolution |
| --- | --- | --- | --- |
| | | | |
