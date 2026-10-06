# RFC 0003 — Events & Observability

> **SUPERSEDED — archived Sep 29, 2026.** The RFC set was reorganised from milestone-based to architecture- and service-based. This document is kept for history only and is no longer maintained. Its content now lives in: [0001-platform-and-shared-infrastructure](../0001-platform-and-shared-infrastructure.md), [0007-notification-service](../0007-notification-service.md), [0008-analytics-service](../0008-analytics-service.md), [0002-gateway-service](../0002-gateway-service.md). See [docs/README.md](../../README.md).

| | |
| --- | --- |
| **Owner** | @muhammad.adnan |
| **Collaborators** | SarwanAhmed |
| **Status** | **DRAFT** · IN-REVIEW · DONE |
| **Created** | Sep 22, 2026 |
| **Related Epic or story** | Milestone 3 — Events & Observability ([milestones.md](../../milestones.md)), Week 3 ([execution-guidelines.md](../../execution-guidelines.md)) |
| **Builds on** | [RFC 0000](0000-system-architecture.md) · [RFC 0001](0001-foundation-and-core-services.md) · [RFC 0002](0002-core-business-logic-and-workflows.md) |

---

### Milestone source text (verbatim)

> **Week 3 — Event Driven + Observability**
>
> - Kafka integration for events
> - Background workers (Celery)
> - Basic analytics metrics
> - Observability setup (logs, tracing, metrics)
>
> **Expected Outcome**
> - Event-driven flows working
> - Analytics pipeline initialized
> - Basic monitoring and tracing available

Metrics are specified in [Part A](../../part-a.md) §5. Observability expectations
are in [Part A](../../part-a.md) §6. Domain wording is mapped per
[RFC 0000](0000-system-architecture.md) §11.

---

## Contents

1. [Title](#1-title)
2. [Overview](#2-overview)
   - 2.1. [Goals](#21-goals)
   - 2.2. [Non-goals](#22-non-goals)
3. [Solution Overview](#3-solution-overview)
   - 3.1. [Specification](#31-specification)
     - 3.1.1. [Architectural changes](#311-architectural-changes)
       - 3.1.1a. [The delivery path — how a notification reaches a connected client](#311a-the-delivery-path--how-a-notification-reaches-a-connected-client)
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

**The event backbone and the observability stack — Kafka with Schema Registry, the outbox relay, Celery background workers, the `notification` and `analytics` services, and OpenTelemetry-based tracing, metrics, and logs.**

This milestone turns a set of services that call each other into a system that reacts to itself, and it makes that system legible. It is the largest milestone in Part A by component count and the one most exposed to schedule risk, which is why [RFC 0002](0002-core-business-logic-and-workflows.md) wrote the outbox tables a week early.

---

## 2. Overview

After [RFC 0002](0002-core-business-logic-and-workflows.md), SmartRetail publishes products and processes orders correctly, and every state change already writes an outbox row. Nothing reads those rows. Nothing tells a customer their order was confirmed. There are no numbers, and a request that crosses four services can only be followed by grepping `X-Request-ID` across four log streams.

[Part A](../../part-a.md) §4 lists the work that should not block a customer request — indexing, inventory sync, shipment updates, payment confirmation, notifications, analytics, returns — with four properties: run independently, be traceable and recoverable, handle failure gracefully, avoid double-processing. §5 names nine metrics. §6 requires that failures be diagnosable in publishing, checkout, inventory sync, workers, and notifications.

This RFC delivers all three. It adds Kafka and Schema Registry as the event backbone, an `outbox-relay` that publishes what the services already write, RabbitMQ and Celery for one-shot background jobs, `notification` (:8005) and `analytics` (:8006) as event consumers, MongoDB for analytics, Redis for caching, rate limiting, and fan-out, and the OpenTelemetry → Jaeger / Prometheus / Grafana stack.

It also closes the loop back to the customer. Order creation returns `202` ([RFC 0002](0002-core-business-logic-and-workflows.md) B9), so the customer's browser has no way to learn how the workflow ended — and [Part A](../../part-a.md) §1 names exactly that as broken today: *"order confirmation, dispatch updates, and refund notices are unreliable during traffic spikes."* Recording a notification is not delivering one. So `notification` pushes to connected clients over SSE, fanning out across replicas via Redis Pub/Sub, with resumable streams so nothing is lost when a connection is not. Polling is the client's fallback, not the design (C15–C20, §3.1.1a).

Twenty-one containers. The resource ceiling is reached this week, and Compose profiles are how it is survived.

### 2.1. Goals

| # | Goal | How it is verified |
| --- | --- | --- |
| G1 | Every outbox row reaches Kafka exactly once, in per-aggregate order, with no loss across a crash | Relay killed mid-batch and restarted; every row ends `published_at IS NOT NULL` with no duplicate `event_id` on the topic |
| G2 | Every published event validates against a registered schema, with `BACKWARD` compatibility enforced | Contract test per topic; an incompatible schema registration is rejected by the registry |
| G3 | Consumers are idempotent — replaying the entire topic changes nothing | Replay test: reset consumer offsets to earliest, re-consume, assert every counter and record is unchanged |
| G4 | Failed events are retried with backoff and land in a DLQ rather than blocking the partition | Poison-message test: one bad event goes to DLQ, the partition keeps moving |
| G5 | Notifications are produced for order confirmed, failed, cancelled, shipped, delivered, and stockout | Integration test per event type; delivery records persisted with attempt history |
| G5a | A connected client receives a notification by server push, without polling, within a second of the event being consumed | Two `notification` replicas: a client streams from one, the event is consumed by the other, the frame arrives (§3.1.1, C15–C19) |
| G5b | **A stream that drops loses nothing.** Reconnection replays every notification missed, in `seq` order with no duplicates and no holes, **up to the resume bound of 50 frames and 24 hours**; past the bound the client is told the count and the durable list carries the remainder. The notification is never lost — only its *delivery mode* changes from frame to list entry (C22) | Kill a connection mid-flight, publish N ≤ 50, reconnect → exactly N frames in `seq` order. Repeat with N = 200 → 50 frames plus one `catchup_truncated` with `omitted_count: 150`, and `GET /notifications` returns all 200 |
| G5c | **A device the server has never seen receives a badge, not a backlog.** A first-ever `client_id` replays zero historical frames and learns its unread count from the `hello` frame (C22) | Seed 500 notifications; connect with an unseen `client_id` and no `Last-Event-ID` → 0 `notification` frames, one `hello` with `cold_start: true` and the correct `unread_count`, and live frames from that point |
| G5d | **A notification scoped to the originating device does not toast on the others, and is never lost by them** (C21) | Two clients for one user; place an order from client A → the `order_confirmed` frame arrives only at A, while B's `unread_count` still increments and `GET /notifications` on B shows it |
| G5e | **A delivered notification is distinguishable from a merely recorded one** (C23) | A client receives a frame and acks; `delivered_at` and `delivered_client_id` are set; a second ack of the same ids changes nothing; a never-acked notification keeps `delivered_at IS NULL` and appears in the admin view |
| G6 | All nine [Part A](../../part-a.md) §5 metrics are computed and queryable | One test per metric, asserting the value after a known event sequence |
| G7 | One request is traceable end to end across gateway, service, workflow, event, and consumer | Jaeger shows a single trace spanning all five for one checkout |
| G8 | RED metrics for every service and consumer, on a Grafana dashboard | Dashboard loads with live data; every panel has a query that returns |
| G9 | Background work is off the request path | No handler performs media processing, email rendering, or rollup computation inline |
| G10 | Failure diagnosis is possible for each of [Part A](../../part-a.md) §6's five named flows | A walkthrough per flow at the milestone review, starting from a symptom and reaching a cause |

### 2.2. Non-goals

| Excluded | Why / when |
| --- | --- |
| Semantic search, embeddings, vector storage | Milestone 5. `catalog.product.published` is produced here; `indexer-worker` consumes it there |
| The AI assistant | Milestone 6 |
| Real email or SMS delivery | `notification` records, renders, and **delivers `in_app` over SSE**. The email and SMS adapters log. A provider is an interface swap, and no account is in scope |
| Web Push / FCM / APNs to a backgrounded or closed app | The offline half of delivery. The `channel` enum gains `push` and `push_subscriptions` is specified (§3.1.3) so it is an adapter, not a redesign — but VAPID keys and a service worker are a client project, and there is no client |
| WebSockets | The traffic is server → client only. SSE is plain HTTP, so the gateway, JWT verification, rate limiting, tracing, and problem-detail errors all apply unchanged. See C15 and §5, Alternative 10 |
| Acknowledgement as a **delivery protocol** — blocking sends, server-side retry on a missing ack, at-least-once push | Still out, and still for C16's reason: the durable row is the receipt, and rebuilding TCP badly buys nothing. What *is* in scope is a best-effort delivery **report** (C23) — `delivered_at` plus a per-device cursor — which records what happened without anything depending on it |
| Alerting — Alertmanager, PagerDuty, on-call | No operational context to alert into. Dashboards and traces cover the stated requirement |
| Log aggregation — Loki, ELK | Structured JSON to stdout plus `docker compose logs` is sufficient at this scale. Correlation, which is the actual requirement, comes from the trace ID being in every line |
| Kafka Streams, ksqlDB, Flink | Consumers in Python are adequate for the metric set; a stream processor is a large new concept for no requirement |
| Multi-broker Kafka, replication factor > 1 | One laptop. Topic configuration makes the change a variable |
| Event sourcing as primary persistence | [RFC 0000](0000-system-architecture.md) §5, Alternative 3 |
| Exactly-once Kafka semantics (transactions) | At-least-once plus idempotent consumers is the standard, simpler, and sufficient answer. See §3.2 |
| Customer-facing notification preferences and unsubscribe | Deferred; the schema has a `channel` column so it is additive |
| CDC via Debezium instead of the application-level outbox | §5, Alternative 2 |

---

## 3. Solution Overview

### 3.1. Specification

#### 3.1.1. Architectural changes

**What joins the topology.**

```
   services (M1–M2)                    ┌──────────────┐
   ┌──────────┬─────────┬───────┬──────┤ outbox-relay │ ← new
   │ identity │ catalog │ order │ inv. │  polls each  │
   └────┬─────┴────┬────┴───┬───┴──┬───┘  outbox table│
        │  outbox  │ outbox │outbox│    └──────┬──────┘
        └──────────┴────────┴──────┘           │ produce
                                               ▼
   ┌────────────────────────────────────────────────────────────────┐
   │  Kafka (KRaft, single broker)   +   Schema Registry            │ ← new
   │  smartretail.<domain>.<event>   ·   Avro · BACKWARD compat     │
   └──┬──────────────┬───────────────────┬──────────────┬───────────┘
      │              │                   │              │
      ▼              ▼                   ▼              ▼
 ┌──────────┐  ┌───────────┐      ┌────────────┐  ┌───────────┐
 │notifica- │  │ analytics │      │ inventory  │  │  catalog  │
 │tion :8005│  │  :8006    │      │ consumer   │  │ consumer  │
 │  ← new   │  │  ← new    │      │            │  │           │
 └────┬─────┘  └─────┬─────┘      └────────────┘  └───────────┘
      │              │                                  ▲
      │ enqueue      ▼                                  │ merchant
      ▼        ┌──────────┐                             │ suspended
 ┌──────────┐  │ MongoDB  │ ← new                       │
 │ RabbitMQ │  │analytics │                        (identity events)
 │  ← new   │  │   _db    │
 └────┬─────┘  └──────────┘         ┌─────────┐
      ▼                             │  Redis  │ ← new
 ┌──────────────┐                   │ cache · │
 │ celery-worker│ ← new             │ rate    │
 │ + beat       │                   │ limit · │
 └──────────────┘                   │ pub/sub │
                                    └─────────┘
   `notification` additionally holds open SSE connections and fans out
   over Redis Pub/Sub, so any replica can serve any client — see §3.1.1a.

   every component above and from M1–M2 emits OTLP:
        │
        ▼
   ┌──────────────────┐   traces   ┌─────────┐
   │  OTel Collector  │───────────▶│ Jaeger  │ ← new
   │     ← new        │   metrics  ├─────────┤
   └──────────────────┘───────────▶│Prometheus│──▶ Grafana ← new
                                   └─────────┘
```

**New architectural decisions.**

- **C1 — The outbox relay is a separate process, not a thread in each service.** One component with one job, restartable independently, and the only writer of `published_at`. A per-service background thread would multiply the failure mode by four and couple publish latency to API deployment.
- **C2 — The relay uses `FOR UPDATE SKIP LOCKED`** to claim a batch, so multiple relay instances can run without double-publishing, and a stuck row never blocks the ones behind it.
- **C3 — Ordering is per aggregate, not global.** The Kafka key is `aggregate_id`, so all events for one order land on one partition in order. Global ordering is neither required nor achievable at any useful throughput.
- **C4 — At-least-once delivery with idempotent consumers,** not Kafka transactions. Every consumer records `(consumer_group, event_id)` in its own database before acting, in the same transaction as the effect. Simpler, debuggable, and sufficient — see §3.2.
- **C5 — Avro with Schema Registry, `BACKWARD` compatibility.** Schemas are checked at registration, so an incompatible change fails in CI rather than in a consumer at runtime. JSON Schema was the alternative; Avro is what Part A's "Schema Registry" implies and gives compact payloads with real evolution rules.
- **C6 — Retry in-process with backoff, then DLQ.** Three in-process attempts with exponential backoff, then the event goes to `<topic>.dlq` with its error, and the consumer commits the offset and moves on. A poison message must not stop a partition.
- **C7 — Celery handles one-shot jobs; Kafka handles facts; Temporal handles processes.** The division from [RFC 0000](0000-system-architecture.md) A12, now real. Media processing moves out of the Temporal activity ([RFC 0002](0002-core-business-logic-and-workflows.md) Q7) into Celery; notification dispatch is a Celery task enqueued by the consumer; scheduled rollups run on Celery Beat.
- **C8 — Analytics is consume-and-aggregate, never query-the-source.** It never calls another service and never reads another database. Every number is derived from events it has consumed, which is what makes it safe for it to be slow or down.
- **C9 — Counters are incremented on ingest; time series are bucketed on ingest; ranking is computed on read.** Pre-aggregation where the query shape is known, on-read computation where it is not.
- **C10 — One OTel Collector, three signals.** Services export OTLP to a collector, which fans out to Jaeger (traces) and Prometheus (metrics). Services do not talk to Jaeger or Prometheus directly, so changing a backend is a collector configuration change.
- **C11 — Trace context propagates over HTTP *and* over Kafka.** W3C `traceparent` travels in the event envelope and is restored by the consumer, so a trace does not end at the producer. This is G7, and it is the part most commonly left out.
- **C12 — The trace ID replaces the request ID as the correlation identifier.** `X-Request-ID` is retained as an alias at the gateway for continuity, but the value is now the OTel trace ID, so a log line, a trace, and an event all carry the same string.
- **C13 — Redis arrives with three concrete jobs,** not speculatively: gateway rate limiting, the idempotency-key fast path, and caching of the product browse query. Each is justified by an existing path, which is the condition [RFC 0001](0001-foundation-and-core-services.md) set for introducing it.
- **C14 — The observability stack is a Compose profile, off by default.** Nobody needs Grafana to run a unit test.
- **C15 — Notifications are pushed to connected clients over SSE, not polled for.** Checkout returns `202` ([RFC 0002](0002-core-business-logic-and-workflows.md) B9), so the notification is the customer's only push-based window into a workflow that completes after the request ended. Polling `GET /notifications` makes latency a function of the poll interval and load a function of idle users — both wrong. SSE is chosen over WebSockets for the same reasons [RFC 0005](0005-ai-assistant-and-streaming.md) E4 chooses it for the assistant: the traffic is one-way, SSE is plain HTTP, so the gateway, JWT verification, rate limiting, tracing, and problem-detail errors apply unchanged, and reconnection is native to `EventSource`.
- **C16 — The push path is an accelerant over a durable store, never the source of truth.** The `notifications` row stays authoritative; SSE only makes it arrive sooner. This single constraint licenses everything below — the fan-out bus may be lossy, replicas may restart, and a frame that never arrives costs only latency, because the row is already durable. Frames *are* acknowledged (C23), but the acknowledgement is a **record, not a protocol**: no send blocks on it, no missing ack triggers a resend, and no notification is ever marked failed for want of one. The single decision an ack participates in is widening an origin-scoped notification's audience after a grace period (C21) — which degrades toward broadcast, never toward loss. Inverting C16, and treating the open connection as the delivery guarantee, is still what makes real-time systems fragile.
- **C17 — Fan-out between replicas is Redis Pub/Sub, one channel per connected user.** Kafka partitions by `aggregate_id` (C3), so the replica that *consumes* an event is almost never the replica *holding* that user's connection. On stream open the replica issues `SUBSCRIBE nt:user:{user_id}`; on close, `UNSUBSCRIBE`. The consumer `PUBLISH`es after its transaction commits. At-most-once is sufficient under C16, and Redis is already in the stack for three other jobs (C13), so this adds no new infrastructure. The message body carries `origin_client_id` and `scope`, and the *delivering* replica filters per connection (C21) — fan-out stays one channel per user rather than one per device, because a per-device channel would need the consumer to know channel names it cannot derive and would multiply `SUBSCRIBE` churn for a three-line predicate at the edge. That is Q11's reasoning, and scoping does not reopen it.
- **C18 — Every notification carries a monotonic `seq`, and streams resume with `Last-Event-ID`.** Each SSE frame sets `id: <seq>`. `EventSource` resends the last received id on reconnect, so the handler replays `WHERE user_id = ? AND seq > ?` before subscribing to live frames. Network blips, pod restarts, laptop sleep, and dropped Pub/Sub messages all recover by the same path. This is what makes C16 safe rather than merely optimistic, and it is what actually replaces polling: the client never asks "anything new?", it reconnects and is told. But `Last-Event-ID` is a *hint*, not the only cursor. `EventSource` retains it only across its own automatic reconnects — a page reload, a new tab, or a fresh browser profile sends none — so the resume floor is server-side (C22), and catch-up is bounded in both count and age.
- **C19 — SSE connections authenticate with a short-lived single-use ticket, not a bearer header.** `EventSource` cannot set `Authorization` — the usual reason teams reach for WebSockets, and it does not need to be. `POST /notifications/stream-ticket` exchanges the bearer token for a 60-second single-use ticket held in Redis; the stream is opened with it. Putting the JWT in the query string instead would leak it into access logs, proxy logs, and `Referer` headers.
- **C20 — `notification` replicas hold connections but no *replica-affine* state.** Nothing recoverable is pinned to a replica: any replica serves any reconnect, there are no sticky sessions, and no directory maps a client to a pod. Two pieces of *shared, advisory* state exist and are deliberately not per-replica — the per-`(user_id, client_id)` replay cursor in Postgres (C22) and a short-TTL presence key in Redis (C21). Both are reconstructible and both fail safe: lose the cursor and a client cold-starts, lose the presence key and an origin-scoped notification broadcasts. This is the difference between horizontal scaling that is real and horizontal scaling that is nominal — and the line that must not be crossed is a registry recording *where* a connection lives, because that is the one that would make routing sticky.
- **C21 — Delivery is scoped by the *originating client*, not only by the user.** A browser profile or app install mints a stable `client_id` and sends it as `X-Client-Id` on mutating requests. The producing service records it in the outbox row; it travels in the event envelope; `notification` stores it as `origin_client_id` and derives a `delivery_scope` — `origin_only` \| `origin_first` \| `all_devices` — from the notification type. A checkout begun on a laptop must not toast on a phone in a pocket; a shipment update, which has no originating client at all, must reach every device. **Scope affects frames only: `unread_count` and `GET /notifications` are never scope-filtered, so every device's badge still moves.** That is the property that makes targeting safe rather than lossy, and it is the one an implementer must not quietly optimise away.
- **C22 — Catch-up is bounded, and the replay floor lives on the server.** `stream_cursors(user_id, client_id)` holds `cursor_seq`, advanced by the ack endpoint (C23). The replay floor is `greatest(Last-Event-ID, cursor_seq)`, and catch-up is additionally capped at **50 frames and 24 hours**. A `client_id` seen for the first time replays **zero** frames: it receives a `hello` frame carrying `unread_count` and reads history from `GET /api/v1/notifications`. An unbounded `WHERE seq > cursor` on a fresh device replays a lifetime of notifications as live frames, which is not a resume — it is a denial of service the user performs on themselves.
- **C23 — The ack is a report, not a protocol.** `POST /api/v1/notifications/ack` is batched, idempotent, rate-limited, fire-and-forget from the client, and returns `204` unconditionally. It stamps `delivered_at` (first writer wins) and advances the client's cursor. It is explicitly **not** at-least-once push: the server never resends because an ack is missing, never retries an ack, never queues one, and a stream never waits for one. A client that acks nothing is a supported configuration — it cold-starts on every reload and every origin-scoped notification broadcasts after the grace period, which is exactly the behaviour this RFC had before the ack existed.

**Event flow, end to end.**

```
 order service                  outbox-relay              Kafka            notification
      │                              │                      │                    │
      │ BEGIN                        │                      │                    │
      │  update orders → confirmed   │                      │                    │
      │  insert outbox(order.confirmed, traceparent)        │                    │
      │ COMMIT ──────────────────────┼──────────────────────┼────────────────────┤
      │                              │ SELECT … WHERE       │                    │
      │                              │  published_at IS NULL│                    │
      │                              │  FOR UPDATE SKIP LOCKED                   │
      │                              │ serialise Avro ─────▶│ key=order_id       │
      │                              │ on ack: set published_at                  │
      │                              │                      ├───────────────────▶│
      │                              │                      │  restore trace ctx │
      │                              │                      │  seen(event_id)?   │
      │                              │                      │   yes → commit,skip│
      │                              │                      │   no  → BEGIN      │
      │                              │                      │     insert processed│
      │                              │                      │     insert notif.  │
      │                              │                      │    COMMIT          │
      │                              │                      │   enqueue celery ──┼──▶ dispatch
      │                              │                      │  commit offset     │
```

If the relay crashes after producing but before setting `published_at`, the row is re-published on restart — a duplicate `event_id` that the consumer's dedupe absorbs. That is the at-least-once trade (C4), and it is why C4 and the dedupe table are a single design rather than two.

##### 3.1.1a. The delivery path — how a notification reaches a connected client

Everything above gets a notification *recorded*. This section covers getting it *delivered*, which is C15–C20 made concrete.

**The problem being solved.** Kafka partitions by `aggregate_id`, so consumption and connection land on different replicas. The consumer that writes the row usually cannot see the socket that needs it.

```
  Kafka ──▶ notification-1        notification-2       notification-3
             │ (consumer)                               │ SSE connections:
             │ BEGIN                                    │   user X  (seq 84213)
             │  insert processed_events                 │   user Y
             │  insert notifications  → seq 84214       │
             │ COMMIT                                   │
             │ enqueue celery (email/push adapters)     │
             │ PUBLISH nt:user:X {seq,id,type,title} ─┐ │
             ▼                                        │ ▲
      ┌───────────────────────────────────────────────▼─┴──────┐
      │  Redis Pub/Sub — SUBSCRIBE on stream open,             │
      │                  UNSUBSCRIBE on stream close           │
      └────────────────────────────────────────────────────────┘
                                                        │
                            gateway (no buffering) ◀────┘
                                    │  text/event-stream
                                    ▼
                                  client
```

**Opening a stream.**

```
POST /api/v1/notifications/stream-ticket     Authorization: Bearer <jwt>
                                             X-Client-Id: b3f1…
  → { "ticket": "…", "expires_in": 60 }      single-use; Redis holds
                                             {user_id, client_id, jwt_exp}

GET  /api/v1/notifications/stream?ticket=…   Last-Event-ID: 84213   (often absent)
  1. redeem ticket (atomic GETDEL) → {user_id, client_id}, or 401
  2. SELECT … FROM stream_cursors WHERE user_id = ? AND client_id = ?
     no row → COLD START: insert at the user's current max(seq);
              emit hello {cold_start: true, unread_count}; replay NOTHING
     row    → RESUME:     floor = greatest(cursor_seq, coalesce(Last-Event-ID, 0));
              bounded catch-up (below); emit hello, then catchup_truncated if hit
  3. SUBSCRIBE nt:user:{user_id} → live frames, filtered per connection by scope
  4. SET nt:client:{user_id}:{client_id} EX 45   (presence, refreshed by the heartbeat)
  5. `: keepalive` comment every 20s
```

**The bounded catch-up query (C22).** The 5-second overlap window below is the `seq`
visibility trap's fix and is unchanged; what is new is the age bound, the scope
filter, and the limit.

```sql
SELECT id, seq, type, title, body, created_at, origin_client_id, delivery_scope
  FROM notifications
 WHERE user_id = :user_id
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

When 51 rows come back, the handler sends the **oldest 50** and emits
`catchup_truncated` — never the newest 50, because a hole *below* the sent range is
worse than a delay above it, and sending the oldest lets the client's cursor advance
contiguously into the remainder it pages from `GET /notifications?since_seq=`.

**The frame contract.** Typed events, matching the style [RFC 0005](0005-ai-assistant-and-streaming.md) E5 sets for the assistant stream.

| Event | Payload | Notes |
| --- | --- | --- |
| `hello` | `{client_id, cold_start, unread_count, server_seq, resumed_from}` | Always first. `cold_start: true` tells the client to fetch the list rather than expect a backlog (C22) |
| `notification` | `{id, seq, type, title, body, created_at, origin}` | `id:` is set to `seq` — this is what the browser echoes back. `origin: true` when this client is the originating device, so the UI can show an inline confirmation rather than a generic toast |
| `catchup_truncated` | `{omitted_count, oldest_seq_sent}` | Emitted before the replayed frames when the 50/24h bound was hit |
| `unread_count` | `{count}` | Sent on connect and after each notification, so the badge needs no second request. **Never scope-filtered** (C21) |
| `ping` | — | Heartbeat; also the liveness signal for connection reaping, and what refreshes the presence key |
| `error` | RFC 9457 problem detail | Emitted before close, so a stream never simply stops without explanation |

**Delivery scope per `NotificationType` (C21).** Absent `origin_client_id` coerces to
`all_devices` unconditionally, with no exception for types whose default is narrower.

| Type | Scope | Why | When the origin client is not connected |
| --- | --- | --- | --- |
| `order_confirmed` | `origin_first` | The tab that submitted the checkout is where the user is waiting. But a confirmation the user never sees is a support ticket | Promoted to `all_devices` once |
| `order_failed` | `origin_first` | Same, and it is money. This is the case that justifies the promoter existing | Promoted — highest priority |
| `order_cancelled` | `all_devices` | Frequently *not* user-initiated: the reservation sweeper, an admin, a merchant. Even when it is, the user may have moved devices | n/a |
| `shipment_update` | `all_devices` | Originates from a carrier webhook; there is no origin client to speak of | n/a |
| `stockout` | `all_devices` | Merchant-facing, triggered by somebody else's order entirely | n/a |
| `welcome` | `origin_only` | Only meaningful in the session that just registered. A welcome toast arriving on a second device two days later is noise, and it is already in the list | Nothing. It expires into the list, which is the correct outcome |

**Promotion.** `promote_undelivered_notifications` runs on Celery Beat every 15s rather
than as a per-notification countdown task, which would be a task storm and a scheduling
dependency for a cosmetic outcome:

```sql
UPDATE notifications SET promoted_at = now()
 WHERE delivery_scope = 'origin_first'
   AND delivered_at IS NULL AND promoted_at IS NULL
   AND created_at < now() - interval '45 seconds'
   AND created_at > now() - interval '1 hour'     -- bounded scan
RETURNING …            → re-PUBLISH each with scope = 'all_devices'
```

Idempotent by `promoted_at IS NULL`, bounded by the one-hour floor, safe on two workers.
**The worst-case toast delay is arithmetic, not a guess:** heartbeat 20s + presence TTL
45s + promotion grace 45s + sweep interval 15s ≈ **75 seconds** for a laptop whose lid
closes immediately after the event; sub-second in the common case. If that is judged too
slow for `order_failed`, the lever is a 30s presence TTL with a 12s heartbeat, not a
redesign.

**Where `client_id` comes from, and the gap it has to cross.** `order.confirmed` and
`order.failed` are not emitted by the HTTP handler — checkout returns `202`
([RFC 0002](0002-core-business-logic-and-workflows.md) B9) and the Temporal workflow
emits them later, on a worker, with no request context. The `client_id` therefore has to
be *persisted* to survive the gap: `order_db.orders` gains `origin_client_id`, written at
order creation and read by every activity that writes an outbox row. Without that column,
the two notification types that most need origin targeting are the two that cannot have
it.

**Connection hygiene.**

| Concern | Handling |
| --- | --- |
| Idle proxies closing connections | `: keepalive` comment every 20s |
| Slow or dead clients | Bounded per-connection queue; on overflow the connection is closed rather than buffered. The client reconnects and catches up by `seq`, so closing is lossless |
| Reconnect storms after a deploy | `retry:` with jitter, plus a cap of 3 concurrent streams per user |
| A fresh device with no cursor | Cold start (C22): zero replayed frames, a `hello` frame with the unread count, and live frames from that point. The alternative — replaying a lifetime of notifications as live frames — is the failure this revision exists to remove |
| A client that never acks | Supported. Its cursor never advances, so every reload is a cold start and every `origin_first` notification promotes to broadcast after the grace period. That is precisely the pre-ack behaviour, which is why `delivered_at` gates nothing (C23) |
| Graceful shutdown | Streams closed with a `retry:` hint before the pod exits, spreading reconnects |
| Capacity | An idle SSE connection is an asyncio task and a socket. ~10k per replica with `uvloop` and a raised `ulimit -n`; [Part A](../../part-a.md) §4's "tens of thousands of concurrent users" is 2–4 replicas |

⚠️ **The `seq` visibility trap.** `bigserial` values are allocated before commit, so a transaction holding `seq` 84214 can commit *after* one holding 84215. A client that catches up to 84215 would then skip 84214 permanently — a silent, unreproducible loss. The catch-up query therefore reads a small overlap window, `WHERE user_id = ? AND (seq > :cursor OR created_at > now() - interval '5 seconds')`, and the client de-duplicates by `id`. Cheap, and it removes the one genuinely nasty bug in this design.

The ack endpoint (C23) must not defeat it. A client that acks `seq` 84215 while 84214 is still uncommitted would, with a naive `cursor_seq = 84215`, skip 84214 permanently once the overlap window has passed — turning a transient trap into a permanent one. So `cursor_seq` is clamped to the newest row that is definitely settled:

```sql
cursor_seq := greatest(cursor_seq,
                       least(:acked_up_to_seq,
                             (SELECT coalesce(max(seq), 0) FROM notifications
                               WHERE user_id = :uid
                                 AND created_at < now() - interval '5 seconds')))
```

Frames above the clamp were still *delivered* — `delivered_at` is stamped by explicit `id`, never by range, so a row a device never saw is never marked as though it had been — they are simply re-offered on the next connect and de-duplicated client-side by `id`, which the client already does.

**The offline half.** SSE covers "the app is open". It is not the whole answer, and the RFC does not pretend otherwise:

| Client state | Mechanism | Status |
| --- | --- | --- |
| Connected | SSE frame, sub-second | **In scope** |
| Backgrounded or closed | Web Push / FCM via the `dispatch_notification` Celery task | Out of scope (§2.2); the `channel` enum and `push_subscriptions` are specified so it is an adapter swap |
| Returning later | `GET /api/v1/notifications` — the durable list | In scope, unchanged |

**Celery task inventory.**

| Task | Queue | Trigger | Replaces |
| --- | --- | --- | --- |
| `process_media` | `media` | Publishing workflow activity enqueues and awaits | The inline Temporal activity from M2 ([RFC 0002](0002-core-business-logic-and-workflows.md) Q7) |
| `dispatch_notification` | `notifications` | `notification` consumer, after persisting | — |
| `promote_undelivered_notifications` | `notifications` | Celery Beat, every 15s — widens an unacknowledged `origin_first` notification to all devices (C21) | — |
| `sweep_stale_stream_cursors` | `maintenance` | Celery Beat, daily — 90-day retention on `stream_cursors`, and the 20-rows-per-user cap | — |
| `rebuild_daily_rollups` | `analytics` | Celery Beat, hourly | — |
| `sweep_expired_reservations` | `maintenance` | Celery Beat, every minute | The bespoke loop from M2 |
| `sweep_idempotency_keys` | `maintenance` | Celery Beat, daily | — |
| `sweep_published_outbox` | `maintenance` | Celery Beat, daily | — |
| `reconcile_inventory_ledger` | `maintenance` | Celery Beat, daily | — |

Separate queues so a burst of media processing cannot starve notification delivery.

**The nine metrics, and how each is computed.** [Part A](../../part-a.md) §5, one row each.

| Metric | Source events | Computation |
| --- | --- | --- |
| Total Customers | `identity.user.registered` | Counter, filtered on `role=customer` |
| Total Merchants | `identity.user.registered` | Counter, filtered on `role=merchant` |
| Total Products Published | `catalog.product.published`, `…unpublished` | Counter, incremented and decremented |
| Orders Over Time | `order.created`, `order.confirmed` | Daily buckets, both counts, so conversion is derivable |
| Revenue Trends | `order.confirmed` | Daily bucket sum of `total`, per currency |
| Most Popular Products | `order.confirmed` line items | Per-product quantity and revenue counters; ranked on read |
| Average Order Value | `order.confirmed` | Bucket revenue ÷ bucket confirmed-order count |
| Inventory Stockout Rate | `inventory.stockout`, `order.created` | Stockout events ÷ order attempts in the window |
| Failed Events / Workflow Issues | `order.failed`, DLQ arrivals, workflow failure events | Counter by `failure_reason` and by source |

#### 3.1.2. API changes

**analytics**

| Method | Path | Auth | Role | Purpose |
| --- | --- | --- | --- | --- |
| `GET` | `/api/v1/analytics/overview` | bearer | `admin` | Every counter in one payload — the dashboard's home |
| `GET` | `/api/v1/analytics/orders` | bearer | `admin`, `merchant` | Orders over time. `from`, `to`, `granularity ∈ {day, week, month}`. Merchant scope applied from the token |
| `GET` | `/api/v1/analytics/revenue` | bearer | `admin`, `merchant` | Revenue trends and average order value, same parameters |
| `GET` | `/api/v1/analytics/products/popular` | bearer | `admin`, `merchant` | Ranked by quantity or revenue. `limit`, `from`, `to` |
| `GET` | `/api/v1/analytics/inventory/stockouts` | bearer | `admin`, `merchant` | Stockout rate and the offending variants |
| `GET` | `/api/v1/analytics/failures` | bearer | `admin` | Failed events and workflow issues, grouped by reason |

**notification**

| Method | Path | Auth | Role | Purpose |
| --- | --- | --- | --- | --- |
| `GET` | `/api/v1/notifications` | bearer | any | The caller's own notifications. Filters: `read`, `type`, `unread_only`, `since_seq` (forward paging for a truncated client). Paginated. Items carry `delivered_at`. **Never scope-filtered** (C21) |
| `POST` | `/api/v1/notifications/{id}/read` | bearer | owner | Mark read |
| `POST` | `/api/v1/notifications/read-all` | bearer | any | Mark all read |
| `POST` | `/api/v1/notifications/ack` | bearer | owner | **Delivery report.** Batched, idempotent, non-blocking. Stamps `delivered_at` and advances the caller's per-device stream cursor (C23) |
| `GET` | `/api/v1/notifications/admin` | bearer | `admin` | All notifications with delivery attempts and failures — the operational view. Now also `delivery_scope`, `origin_client_id`, `delivered_at`, `promoted_at`, and a `delivered_at IS NULL` filter, so "what did we record but never deliver?" is answerable |
| `POST` | `/api/v1/notifications/stream-ticket` | bearer | any | Exchanges the JWT for a single-use 60-second stream ticket, **binding `X-Client-Id` into it** (C19, C21) |
| `GET` | `/api/v1/notifications/stream` | ticket | any | **SSE stream.** `?ticket=…`, `Last-Event-ID` as a resume hint. `client_id` comes from the redeemed ticket, never a query parameter. The push channel that replaces polling (C15, C18, C22) |
| `GET` | `/api/v1/notifications/unread-count` | bearer | any | Badge count for clients that cannot hold a stream open. **Never scope-filtered** (C21) |

**The ack endpoint.**

```http
POST /api/v1/notifications/ack          Authorization: Bearer <jwt>
{
  "client_id":  "b3f1…",                  // required; validated for shape only
  "up_to_seq":  84231,                    // advances stream_cursors.cursor_seq
  "ids":        ["…", "…"]                // optional, ≤200; stamps delivered_at
}
→ 204 No Content                          always, including unknown ids and stale seqs
```

Both fields are needed and neither replaces the other. `up_to_seq` is a high-water mark,
which is the only shape that survives out-of-order acks and makes the endpoint idempotent
by `greatest()`. `delivered_at` has to be per-row and explicit, because scope filtering
means a client legitimately never sees some rows inside its range — stamping by range
would report a delivery that never happened.

Idempotent by construction: `SET delivered_at = coalesce(delivered_at, now())` guarded by
`WHERE user_id = :uid AND delivered_at IS NULL`, and
`ON CONFLICT (user_id, client_id) DO UPDATE SET cursor_seq = greatest(…)` with the
5-second clamp from §3.1.1a. Replaying the same body is a no-op. Never `4xx` except `400`
on a malformed body — an unknown `id` is ignored rather than reported, because it may
belong to another user and the response must not say which. Rate-limited at 12/min per
`(user, client)`; over the limit is `429` and the client simply drops it.

Client policy, which is what keeps C23 honest: ack on frame render, 2-second debounce,
coalesced into one call, `fetch(…, {keepalive: true})`, no retry, no user-visible failure.

**Operational, on every service**

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/metrics` | Prometheus exposition. *New this milestone* |
| `GET` | `/health/ready` | Extended: consumers report broker connectivity and consumer lag |

**Changed behaviour on existing endpoints**

| Endpoint | Change |
| --- | --- |
| All | Responses now carry `traceparent`; `X-Request-ID` echoes the trace ID (C12) |
| `POST /api/v1/auth/login` | Rate limited — 5 attempts per minute per IP and per email, `429` with `Retry-After`. Closes the known issue recorded in [RFC 0001](0001-foundation-and-core-services.md) §4 |
| `POST /api/v1/orders` | Rate limited — 30 per minute per user |
| `GET /api/v1/products` | Cached in Redis, 60-second TTL, keyed by the full filter set; invalidated by the `catalog` consumer on `product.updated` |
| `GET /api/v1/inventory/{variant_id}` | Cached, 10-second TTL — short, because stock is the fastest-moving fact in the system |
| `gateway`, notification stream routes | Must not buffer `text/event-stream`: proxy streaming, no response compression, extended read timeout, and the per-IP rate limit exempted for `/notifications/stream` (a long-lived connection is one request, and counting it as one would throttle reconnect storms into a failure) |
| `gateway`, all mutating routes and `/notifications/stream-ticket` | Passes `X-Client-Id` through — it does not generate one. Validates shape only (`^[A-Za-z0-9_-]{8,64}$`) and **strips** a malformed value rather than rejecting the request: a cosmetic header must never fail a checkout (C21) |
| `POST /api/v1/orders/{id}/shipment/status` | Superseded by `POST /api/v1/webhooks/shipment`, an HMAC-signed simulated carrier callback. The admin endpoint remains for manual testing |

#### 3.1.3. Data/schema changes

**Kafka topics.** Fourteen, per the [RFC 0000](0000-system-architecture.md) catalogue, plus one DLQ each. Local configuration: 3 partitions, replication factor 1, 7-day retention; `compact` on `identity.merchant.status_changed` and `catalog.product.published`, where the latest state per key is what a new consumer needs.

**Every column of every table in the platform is specified in
[RFC 0006 §3.1.3](0006-service-decomposition-and-data-model.md#313-dataschema-changes).**
This section names only what this milestone creates or amends; it does not
restate columns, so a type has exactly one definition in the RFC set.

**Databases created:** `notification_db` (PostgreSQL), `analytics_db` (MongoDB).

| Table / collection | Database | Action | Columns |
| --- | --- | --- | --- |
| [`notifications`](0006-service-decomposition-and-data-model.md#notifications) | `notification_db` | Created — rendered notification, read state, `seq` stream cursor, origin client and delivery scope (C21), delivery report (C23) | [0006](0006-service-decomposition-and-data-model.md#notifications) |
| [`stream_cursors`](0006-service-decomposition-and-data-model.md#stream_cursors) | `notification_db` | Created — the server-side replay floor, one row per `(user, device)` (C22) | [0006](0006-service-decomposition-and-data-model.md#stream_cursors) |
| [`delivery_attempts`](0006-service-decomposition-and-data-model.md#delivery_attempts) | `notification_db` | Created — per-channel send attempts | [0006](0006-service-decomposition-and-data-model.md#delivery_attempts) |
| [`push_subscriptions`](0006-service-decomposition-and-data-model.md#push_subscriptions) | `notification_db` | Created — specified but unused, so the offline channel is an adapter rather than a migration | [0006](0006-service-decomposition-and-data-model.md#push_subscriptions) |
| `counters`, `daily_metrics`, `product_stats`, `failure_stats`, `raw_events` | `analytics_db` | Created | [0006](0006-service-decomposition-and-data-model.md#analytics_db) |
| [`processed_events`](0006-service-decomposition-and-data-model.md#processed_events) | every consumer | Created — S13 dedupe, in the same transaction as the effect | [0006](0006-service-decomposition-and-data-model.md#processed_events) |
| [`outbox`](0006-service-decomposition-and-data-model.md#outbox) | every producer | `published_at` now actually written; the relay's partial index added; gains `client_id`, the originating `X-Client-Id` (C21) | [0006](0006-service-decomposition-and-data-model.md#outbox) |

**Amended:** `catalog.products` gains `search_document` (`jsonb`) — the projection
built by the publishing workflow, persisted so Milestone 5 can backfill without
re-running workflows. `order.orders` gains `notified_at`, so a notification
failure is visible on the order, and `origin_client_id`, so an event emitted by the
Temporal workflow after the request has ended still knows which device caused it (C21,
§3.1.1a).

**The event envelope gains one field.** `client_id` (`"opaque client token | null"`)
joins `actor_id` in the envelope defined once in
[RFC 0000 §3.1.2](0000-system-architecture.md#312-api-changes). In Avro this is
`{"name": "client_id", "type": ["null", "string"], "default": null}` — `null` must be
first in the union for the default to be legal, and with the default present the change
is `FULL`, not merely the `BACKWARD` that C5 requires: a v2 reader resolves a v1 record
through the default, and a v1 reader skips the unknown field. T2's compatibility test
asserts both directions.

**Which database carries `outbox` and which carries `processed_events`** is
derived from the event catalogue and tabulated per database in
[RFC 0006 §3.1.3.2](0006-service-decomposition-and-data-model.md#3132-shared-tables--outbox-and-processed_events).
That table supersedes the blanket "all producers / all consumers" wording this
section used to carry, which was true but left four databases without an explicit row.

**Vocabularies introduced ([RFC 0006](0006-service-decomposition-and-data-model.md#s4--enums-as-text) S4):** `NotificationType`, `NotificationChannel`, `DeliveryStatus`, `NotificationDeliveryScope`.

**Redis keyspaces**

| Pattern | TTL | Use |
| --- | --- | --- |
| `rl:{scope}:{identifier}` | 60s | Rate limit counters |
| `cache:products:{hash}` | 60s | Browse query results |
| `cache:inventory:{variant_id}` | 10s | Availability |
| `idem:{user_id}:{key}` | 24h | Idempotency fast path, ahead of the Postgres claim |
| `celery-*` | — | Broker results backend |
| `nt:user:{user_id}` | — | Pub/Sub channel for notification fan-out (C17). Not a stored key. The message body carries `origin_client_id` and `scope`, and the delivering replica filters per connection |
| `nt:ticket:{ticket}` | 60s | Single-use SSE stream ticket, redeemed with `GETDEL` (C19). The value is `{user_id, client_id, jwt_exp}` as JSON, so the stream's client identity comes from the redeemed ticket and never from the URL |
| `nt:conns:{user_id}` | 60s | Per-user connection count, for the 3-stream cap; refreshed by the heartbeat so a crashed replica's slots expire on their own |
| `nt:client:{user_id}:{client_id}` | 45s | Presence hint (C21). Set on stream open, refreshed by the 20s heartbeat, deleted on clean close. The value is `1` — deliberately *not* a replica id |

**The C20 retreat, stated rather than buried.** C20 said there is no shared connection
registry, and `nt:client:*` is one. What makes it acceptable is what it does *not*
contain: no replica address, so no routing decision is ever made from it, no sticky
session is implied, and there is no consistency to maintain — it expires on its own if a
replica dies. It is consulted at exactly one place, the decision whether an `origin_first`
notification can be targeted, and both error directions are bounded: a false negative
broadcasts a notification that could have been targeted (cosmetic), and a false positive
targets a dead socket, which the promoter fixes within the ~75s worst case. The cheapest
correct alternative — no presence key at all, always publish targeted and always let the
promoter widen — is one line away and is the recorded fallback if the key disappoints; it
was rejected only because it makes the 45s delay the *common* case rather than the
failure case.

**Rejected:** `nt:cursor:{user_id}:{client_id}` as a write-behind cache of `cursor_seq`.
Acks are batched at ≤1 per 2s per client, so the Postgres upsert is not a hot path, and a
second copy of a cursor is a second thing to be wrong. `nt:conns:*` could likewise be
derived by counting `nt:client:{user_id}:*`, but that is a `SCAN` on the connection-open
path — two keys, stated reason, no cleverness.

#### 3.1.4. Breaking changes

| Change | Impact | Handling |
| --- | --- | --- |
| `X-Request-ID` semantics change to the trace ID (C12) | Header name and behaviour are unchanged; only the value's provenance differs | No client depends on the format |
| `POST /orders/{id}/shipment/status` superseded by a signed webhook | The admin endpoint remains | Additive; the old route is documented as test-only |
| Media processing moves from a Temporal activity to a Celery task | The publishing workflow's timing changes; the activity now enqueues and awaits | Internal; the workflow contract is unchanged |
| Rate limiting introduced on `/auth/login` and `/orders` | Clients can now receive `429` | Documented; generous limits; `Retry-After` always present |
| Browse and inventory reads become cached | Up to 60 (or 10) seconds of staleness | Documented per endpoint; invalidation on update keeps the common case fresh |
| `notification` now holds long-lived connections | It is no longer a plain request/response service: deploys must drain connections, the gateway must not buffer, and capacity is measured in sockets as well as RPS | §6.3. No sticky sessions are required (C20), so the load balancer configuration is unchanged; the cost is real but contained to deployment |
| A stream no longer replays unbounded history on connect (C22) | A client that relied on the stream to populate its list must now read `GET /notifications`. The `hello` frame says which case it is in | New behaviour in the same milestone — nothing has shipped against the unbounded version. The `cold_start` flag and `catchup_truncated` exist so the client never has to guess |
| Frames are filtered by delivery scope (C21) | A second device stops receiving *toasts* for another device's checkout | Counts and lists are never filtered, so nothing disappears — only the toast moves. G5d asserts it |

No database migration destroys or rewrites data. Every schema change is additive — `notifications.seq` is a new `bigserial` column and existing rows backfill in insertion order; `origin_client_id`, `delivered_at`, `delivered_client_id` and `promoted_at` are nullable; `delivery_scope` defaults to `all_devices`, which is the pre-revision behaviour, so existing rows need no backfill at all.

#### 3.1.5. Action Plan

Ordered so that the event backbone is proven end to end on one topic before it is widened, and so observability lands early enough to be used while debugging the rest.

| # | Task | Output | Depends on |
| --- | --- | --- | --- |
| T1 | Kafka (KRaft) + Schema Registry in Compose under an `events` profile; topic creation script | Broker healthy; topics exist; console produce/consume works | RFC 0002 done |
| T2 | Avro schemas for all fourteen topics, in `libs/common/events/schemas/`, registered by a script, including the optional `client_id` envelope field with `["null","string"]` and a `null` default (C5, C21) | Registry holds every schema; compatibility set to `BACKWARD`; a test proves a v1 consumer reads a v2 record and a v2 consumer reads a v1 record | T1 |
| T3 | `libs/common`: event envelope (with `client_id`), request context (with `client_id`), Avro serialiser, producer wrapper, consumer base class with dedupe, retry, and DLQ | Unit-tested | T2 |
| T4 | `outbox-relay` service: poll, `SKIP LOCKED` claim, produce, mark published, metrics | **One topic end to end**; crash-restart test shows no loss (G1) | T3 |
| T5 | Relay widened to all four producing services | Every outbox row reaches its topic | T4 |
| T6 | OTel Collector, Jaeger, Prometheus, Grafana in Compose under an `observability` profile | All four UIs reachable | — |
| T7 | `libs/common` tracing: auto-instrument FastAPI, SQLAlchemy, httpx; OTLP exporter; trace ID into the log formatter | A gateway → catalog call is one trace with SQL spans (G7, part) | T6 |
| T8 | Trace context into and out of the event envelope; producer and consumer instrumentation (C11) | A trace spans producer and consumer (G7) | T7, T5 |
| T9 | Temporal worker instrumentation — workflow and activity spans | The trace reaches the workflow (G7 complete) | T7 |
| T10 | Prometheus metrics: RED per service, plus consumer lag, DLQ depth, outbox backlog, and the SSE set — active connections, fan-out publish→frame latency, reconnects/min, catch-up rows per reconnect, dropped slow consumers. Plus the scoped-delivery set: `notification_frames_total{scope,outcome=delivered\|filtered}`, `notification_ack_lag_seconds`, `notification_catchup_rows`, `notification_catchup_truncated_total`, `notification_cold_start_total`, `notification_promoted_total`, and `notification_undelivered_age_seconds` (p99) — the last is the only instrument that detects "the scope rule is wrong and nobody is being toasted", which is otherwise a silent failure | `/metrics` on every service; Prometheus scrapes all targets | T7 |
| T11 | Grafana dashboards: platform overview, per-service RED, event pipeline health, business metrics | Dashboards provisioned as code, loading with live data (G8) | T10 |
| T12 | RabbitMQ + Celery worker + Beat in Compose; the seven tasks; queue routing | Tasks execute; Beat schedules fire | T1 |
| T13 | Media processing moved to Celery; the publishing activity becomes enqueue-and-await | Publishing workflow still green; the activity no longer does image work (G9) | T12 |
| T14 | `notification` service: skeleton, schema, consumer for six event types, templates | Notifications persisted per event type (G5) | T3, T12 |
| T15 | Notification dispatch as a Celery task with attempt recording and a logging adapter | Attempts recorded; a failed dispatch is retried and visible | T14 |
| T16 | Notification read APIs and the admin view | Tests green, isolation enforced | T14 |
| T16a | `notifications.seq`, the stream-ticket endpoint, and the SSE handler with catch-up and heartbeat (C18, C19). The catch-up is bounded from the first commit — the unbounded version is not shipped even as an intermediate step | A single replica pushes to a connected client; `Last-Event-ID` replays a gap (G5b) | T16 |
| T16b | Redis Pub/Sub fan-out: `PUBLISH` after commit in the consumer, dynamic `SUBSCRIBE`/`UNSUBSCRIBE` per connection (C17) | **Two replicas: consume on one, deliver on the other** (G5a) | T16a, T20 |
| T16c | Gateway SSE pass-through — no buffering, no compression, extended read timeout, rate-limit exemption on the stream route | Measured end-to-end push latency through the gateway (G5a) | T16b |
| T16d | Connection hygiene: bounded queues, the 3-stream cap, jittered `retry:`, drain on shutdown, and the connection/latency metrics | A slow client is dropped and recovers; a rolling restart does not stampede | T16c, T10 |
| T16e | `X-Client-Id` end to end: gateway pass-through and shape validation, request context, `outbox.client_id`, the envelope field and its Avro defaults, and `orders.origin_client_id` so workflow-emitted events keep their origin (C21) | A checkout from client A produces an `order.confirmed` carrying A's id; a carrier webhook produces one carrying null | T3, T5 |
| T16f | `stream_cursors`, the bounded catch-up (50 / 24h), the `hello` and `catchup_truncated` frames, and cold start (C22) | A never-seen `client_id` against 500 notifications yields zero frames and a correct badge (G5c) | T16a |
| T16g | `POST /notifications/ack`: batched, idempotent, rate-limited; `delivered_at`; cursor advance with the 5-second safe-seq clamp (C23) | Acks stamp and advance; a double ack is a no-op; a high ack cannot strand a lower `seq` (G5e) | T16f |
| T16h | Scoped delivery: `NotificationDeliveryScope` per type, the client-bound stream ticket, the `nt:client:*` presence key, per-connection frame filtering, and `promote_undelivered_notifications` on Beat (C21) | Two devices, one user: an origin-scoped frame reaches one and both badges move; with the origin offline the notification is promoted within the stated bound (G5d) | T16b, T16e, T16g |
| T17 | MongoDB in Compose; `analytics` skeleton; collections and indexes | Connectivity and index creation | T1 |
| T18 | Analytics consumers for every topic, with ingest-time aggregation (C9) | Counters and buckets move on consumption | T17, T3 |
| T19 | All nine [Part A](../../part-a.md) §5 metrics + their read APIs, merchant-scoped | One test per metric (G6) | T18 |
| T20 | Redis: rate limiting at the gateway, browse and inventory caching with invalidation | Limits enforced; cache hit and invalidation tested | T1 |
| T21 | `catalog` and `inventory` consumers: merchant suspension, cache invalidation, stock row creation on publish | Cross-service reactions working without a synchronous call | T5 |
| T22 | Signed shipment webhook replacing the admin simulation | HMAC verified; replay rejected | T5 |
| T23 | DLQ handling, a replay tool, and the poison-message test (G4) | A bad event lands in DLQ; the partition keeps moving; replay works | T3 |
| T24 | Compose profiles finalised; `make up`, `up-events`, `up-observability`, `up-all` | A developer can run any subset | T11, T12 |
| T25 | End-to-end: checkout producing one trace across five components, all metrics moving, a notification recorded — and the frame landing on the originating device only | `make test` green cold (G7, G10) | T22, T19, T16 |
| T26 | The five diagnosis walkthroughs from [Part A](../../part-a.md) §6, written up in the README | A reader can follow each from symptom to cause (G10) | T25 |

### 3.2. Rationale

**Why the outbox relay rather than publishing from the handler.** A handler that commits to Postgres and then produces to Kafka has a window: crash in between, and the state changed but nobody was told. There is no retry that fixes it, because the process that knew is gone. The outbox makes the event part of the same transaction as the state change, so the two cannot disagree; the relay then has the much easier job of publishing rows that are already durable, and its failure mode is a duplicate rather than a loss. [RFC 0002](0002-core-business-logic-and-workflows.md) wrote the tables specifically so this week is a new process rather than four handler rewrites.

**Why at-least-once with idempotent consumers, not Kafka transactions.** Exactly-once in Kafka is exactly-once *within* Kafka — it does not extend to a Postgres write or an email. The end-to-end property still has to come from consumer idempotency, so transactions add coordination cost and operational complexity without removing the work they appear to remove. Recording `(consumer_group, event_id)` in the same transaction as the effect is simpler, debuggable, and gives the actual guarantee.

**Why Avro and a Schema Registry rather than JSON.** Events outlive the code that wrote them. JSON gives no compatibility checking, so a renamed field is discovered by a consumer crash in production. `BACKWARD` compatibility in the registry means the incompatible change fails at registration — in CI, with a clear message. Part A names the Schema Registry explicitly, and this is what it is for.

**Why push rather than polling, and why this shape of push.** [Part A](../../part-a.md) §1 names unreliable order confirmations and dispatch updates during traffic spikes as a problem to solve, and [RFC 0002](0002-core-business-logic-and-workflows.md) B9 makes checkout asynchronous — so there is a real gap between "we got your order" and "your order is confirmed" that something has to close. Polling closes it badly: latency is bounded below by the poll interval, and the load is proportional to *idle* users rather than to events, which is exactly backwards for a system whose stated problem is behaviour under spikes. A poll every 10 seconds across 20,000 idle users is 2,000 req/s of mostly-empty responses; the same users on SSE are 20,000 idle sockets and zero requests.

The non-obvious part is not choosing SSE — it is C16, that push must sit *over* a durable store rather than replace it. Most of the complexity in real-time delivery comes from trying to make the connection itself reliable: acknowledgements, per-connection buffers, replay logs, sticky routing. None of that is needed here — and the acknowledgement this RFC does keep (C23) is a *report* that nothing depends on, never a guarantee — because the notification is already durable in Postgres before anything is pushed. That lets the fan-out bus be at-most-once Redis Pub/Sub instead of a second Kafka topic or a Redis Stream with consumer groups, and it lets a replica die mid-frame without anyone needing to know. The `seq` cursor (C18) then converts every failure mode — dropped frame, killed pod, closed laptop — into the same cheap catch-up query. One mechanism covers all of them, which is why the design is small.

**Why Celery *and* Kafka *and* Temporal.** The distinction is operational, not aesthetic. Kafka broadcasts a fact to unknown consumers and retains it for replay. Celery does one job soon, with per-task retry, and is the right shape for "resize this image" or "send this email". Temporal runs a durable multi-step process with compensation. Media processing is a Celery task; the fact that a product was published is a Kafka event; the publishing sequence is a Temporal workflow. Using one for another's job is the anti-pattern this design avoids, and demonstrating the distinction is a large part of what Milestone 3 is for.

**Why analytics never queries the source.** If `analytics` could call `order`, then a slow analytics query could contend with checkout, and an analytics outage could become a checkout outage. Consuming events makes it structurally impossible for the reporting path to affect the transaction path — which is why it is a consumer rather than a caller, and why `raw_events` exists as its own escape hatch for backfilling a metric nobody thought of yet.

**Why counters on ingest but ranking on read.** "Total customers" has one query shape forever, so computing it once per event and storing the number is strictly cheaper than scanning. "Most popular products" is parameterised by window, scope, and ranking key, so pre-aggregating every combination is impossible; keeping per-product totals and sorting on read is the right split. Daily buckets sit between the two: pre-aggregated per day, composed on read into whatever window is asked for.

**Why trace context must cross Kafka.** A trace that stops at the producer answers "the API was fast" and nothing about where the work actually went. Part A §6 asks for failure diagnosis in background workers and notifications — which is exactly the part that lives past the producer. Carrying `traceparent` in the envelope and restoring it in the consumer is a few lines that turn five disconnected traces into one, and it is the difference between the observability stack being a demo and being useful.

**Why Redis now and not in Milestone 1.** [RFC 0001](0001-foundation-and-core-services.md) deferred it until a measured need existed. Three now do: login brute-force is a real hole with no other answer, the browse query is the hottest read path in the system, and Celery needs a result backend. Adding it now means it is introduced with reasons attached, which was the condition.

**Why the observability stack is off by default.** Four extra containers to run a unit test is a tax on every iteration of the rest of the project. A profile costs one flag and removes it.

### 3.3. Tradeoffs

| Trade being made | The cost | Why it is acceptable |
| --- | --- | --- |
| Twenty-one containers | Real resource pressure; slow cold start; the heaviest week to debug | Profiles mean the full set runs only for the demo and the review. Every component traces to a Part A requirement |
| At-least-once delivery | Every consumer must implement dedupe; duplicates are normal, not exceptional | The dedupe is ~15 lines in a shared base class, and it is the honest guarantee. Pretending otherwise fails at the worst time |
| Outbox relay as a separate process | Another component to run and monitor; adds up to one poll interval of publish latency | The alternative loses events. Latency is bounded and configurable |
| Avro over JSON | Harder to inspect by eye; the registry is another dependency; schema files to maintain | Compatibility enforcement is the point, and it is worth the inspection cost. `kafka-avro-console-consumer` covers the debugging need |
| Analytics eventually consistent | A dashboard can lag a checkout by seconds | Correct for reporting, and the isolation it buys is the reason the service exists |
| Cached browse and inventory reads | Up to 60s (10s) of staleness | Bounded, documented, invalidated on update. Stock uses the short TTL because it moves fastest |
| Three orchestration systems | Three sets of operational concepts, three failure modes, three UIs | Required by Part A, and each has a distinct job. The learning objective is exactly this distinction |
| Email and SMS not actually delivered | The demo shows a record for those channels, not a mailbox | `in_app` *is* delivered, over SSE, so the path is demonstrably end to end. No provider account is in scope, and the interesting engineering — consume, dedupe, render, push, retry, record — is all present |
| `notification` becomes a stateful, connection-holding service | Deploys must drain; capacity is sockets as well as RPS; the gateway needs streaming-specific configuration | Real-time order and shipment updates are the product requirement, not a nicety. The cost is bounded by C20: no sticky sessions, no replica-affine state, so scaling stays ordinary |
| A server-side per-device replay cursor | A table, a write path, and a second place a replay position can be wrong | Without it a page reload is a full-history replay, because `EventSource` keeps `Last-Event-ID` only across its own reconnects. One small row per device is the cheapest correct answer, and losing the table degrades to a cold start rather than to loss |
| Bounded catch-up — 50 frames, 24 hours | A client away for a week is told a number instead of being shown frames | The durable list has always been the mechanism for "returning later" (§3.1.1a). Replaying a week of notifications as *live* frames is not a resume, it is a UI incident |
| A short-TTL presence key — a partial retreat from C20 | A shared registry exists where the RFC said none would | It carries no routing information, so nothing becomes sticky; it expires on its own; and both error directions cost at most a delayed or over-broad toast. The alternative — always broadcast after a delay — is one line away if it disappoints |
| A client-minted device id | Spoofable, unauthenticated, and a device correlator in the database | Always scoped under the authenticated `user_id` and never an authorisation input (§6.2), so the worst a forger achieves is affecting their own toasts. Rows per user are capped at 20 |
| At-most-once fan-out over Redis Pub/Sub | A dropped message means a notification arrives on reconnect rather than instantly | Correct under C16 — the row is already durable. Buying at-least-once here would mean Redis Streams or a second Kafka topic, to protect data that is not at risk |
| A single-use ticket instead of header auth on the stream | One extra round trip before a stream opens, and a second auth path to maintain | 60ms once per connection, against keeping JWTs out of URLs and logs. The alternative that avoids both is WebSockets, which costs far more (§5, Alternative 10) |
| MongoDB for analytics | A second database technology to run and learn | Required by Part A, and it has a genuine workload here rather than a token table |
| DLQ without an automatic replay loop | A human decides when to replay | Automatic replay of a poison message is an infinite loop. The replay tool exists; the trigger is deliberate |

#### 3.3.1. Challenges and Open Questions

| # | Question | Current position | Needs a decision by |
| --- | --- | --- | --- |
| Q1 | Polling relay or logical replication (Debezium)? | Polling, 500ms interval. One small Python process versus Kafka Connect plus Debezium plus their configuration | Revisit only if latency matters |
| Q2 | One consumer process per service, or per topic? | Per service, subscribing to several topics, with the dedupe keyed by consumer group. Fewer containers, and the ordering guarantee is per key anyway | Decided |
| Q3 | Does `order` keep its synchronous `catalog` call, now that product events exist? | Yes, for now ([RFC 0002](0002-core-business-logic-and-workflows.md) Q1). Price and availability at purchase must be authoritative, and a projection introduces a staleness window on exactly the wrong fact | M4 review |
| Q4 | Should analytics keep `raw_events` at all? | Yes, 7-day TTL. Backfilling a new metric without replaying Kafka is worth the storage, and it is bounded | Decided |
| Q5 | Sampling for traces? | 100% locally. A parent-based sampler is configured but set to always-on, so turning it down is a variable | Not required |
| Q6 | Where does merchant suspension take effect — token issuance, or a catalog reaction? | Both. `identity` refuses to issue tokens; `catalog` consumes `merchant.status_changed` and hides the merchant's products. This closes [RFC 0001](0001-foundation-and-core-services.md) Q2 | Decided here |
| Q7 | Do we need consumer lag alerting? | Metrics and a dashboard panel, no alerting. There is nothing to alert into | Not required |
| Q8 | Should the DLQ replay be an API or a CLI? | CLI in `make`. An API for replaying arbitrary events is a footgun with no operator to use it | Decided |
| Q9 | Partition count of 3 with one broker — meaningful? | Yes: it forces the per-key ordering design to be real, and multi-partition consumer behaviour to be tested, which a single partition would hide | Decided |
| Q10 | Are the metric definitions in §3.1.1 what the reviewer expects? | Stated explicitly so the definitions can be challenged — particularly stockout rate, where "how often items are unavailable when wanted" admits several readings | M4 review |
| Q11 | One Pub/Sub channel per connected user, or a fixed set of sharded channels every replica subscribes to? | Per user. Redis handles tens of thousands of channels comfortably, and it means a replica receives only the messages it can deliver. Sharding trades that for fewer subscriptions and is worth revisiting only if `SUBSCRIBE` churn shows up in Redis latency | Revisit under load |
| Q12 | Should the SSE stream also carry non-notification events — order status, inventory changes? | No, not in M3. One stream per concern keeps the contract legible; a general-purpose client event bus is a design decision that deserves its own RFC rather than arriving as a side effect of notifications | M4 review |
| Q13 | Does adding push to M3 fit, given it is already the heaviest milestone? | T16a–T16h are ~2 days on top of the existing notification tasks because Redis (T20) and the gateway already exist. If the week slips, T16a+T16f (single-replica SSE with *bounded* catch-up) deliver the user-visible behaviour, and T16b's fan-out with T16h's scoping move to M4. The bounded catch-up is not part of the reducible scope: the unbounded version is the bug this design was revised to remove | Decided, with the fallback stated |
| Q14 | Should a cold-start client get *zero* frames, or the newest few unread as a courtesy? | Zero, plus `unread_count` in the `hello` frame. Any non-zero number is an arbitrary constant that will be argued about forever, and the list is one request away | Decided; revisit on UX feedback |
| Q15 | Is `origin_first` promotion worth a sweeper, or should origin-scoped types simply never promote? | Worth it, for `order_failed` specifically — a payment failure the user never sees is the worst outcome in the set. Promotion is one broadcast, idempotent and bounded | Decided |
| Q16 | Should `delivery_scope` be a per-user *preference* rather than a per-type constant? | Not in M3. The `channel` column already carries the preferences deferral (§2.2); scope would join it there | M4 |
| Q17 | Two tabs in one browser share a `client_id` and therefore a cursor. Per-tab ids instead? | No. Per-tab ids make every new tab a cold start, which defeats C22. The granularity of the report is “this browser profile saw it”, and the unread badge is what recovers the lagging tab. The limitation is recorded in §4 | Decided |
| Q18 | Does `client_id` need to survive logout? | Yes — it is a device correlator, not a session. It is never used for authorisation, and `stream_cursors` rows are keyed `(user_id, client_id)`, so a second user on the same browser gets their own row | Decided |

---

## 4. Dependencies/blockers/known issues

**New dependencies.**

| Dependency | Version | Used for |
| --- | --- | --- |
| Apache Kafka (KRaft, no ZooKeeper) | 3.8 / cp-7.7 | Event backbone |
| Confluent Schema Registry | 7.7.x | Avro schema governance |
| `confluent-kafka[avro]` | 2.6.x | Python producer and consumer |
| RabbitMQ | 3.13-management | Celery broker |
| Celery | 5.4.x | Background tasks and Beat |
| Redis | 7.4 | Cache, rate limit, Celery results |
| MongoDB + Motor | 7.x / 3.6.x | Analytics store, async driver |
| OpenTelemetry SDK + instrumentations | 1.28.x | Traces and metrics |
| OTel Collector (contrib) | 0.112.x | Signal fan-out |
| Jaeger | 1.62 (all-in-one) | Trace storage and UI |
| Prometheus / Grafana | 2.55 / 11.x | Metrics and dashboards |
| `sse-starlette` | 2.x | SSE responses in FastAPI. The same dependency [RFC 0005](0005-ai-assistant-and-streaming.md) takes for the assistant stream, pulled forward to this milestone |
| `redis[hiredis]` | 5.x | Async client, used for cache, rate limiting, and Pub/Sub fan-out |

**Blockers.** None. Every component is a public image.

**Known issues carried into this milestone.**

- `in_app` notifications are pushed over SSE; email, SMS, and Web Push are recorded and rendered but not sent.
- A notification for a user with no open connection waits until they reconnect — there is no offline push (§2.2), so "real-time" means "real-time while the app is open".
- Redis is a single node and Pub/Sub is at-most-once: a Redis restart drops in-flight frames, and clients recover on reconnect rather than instantly.
- Two tabs in one browser share a `client_id` and therefore a cursor (Q17), so a tab that never rendered a frame the other tab acked will not be re-shown it. Safe only because the unread count and the durable list are never cursor- or scope-filtered.
- A client that never acks cold-starts on every reload and has every origin-scoped notification promoted to broadcast after the grace period. Supported, and identical to the pre-ack behaviour (C23).
- An origin-scoped toast can be delayed by up to ~75 seconds when the originating device disappears without a clean close — the presence TTL plus the promotion grace plus the sweep interval (§3.1.1a).
- Single-broker Kafka: no replication, and a broker restart pauses the pipeline.
- Jaeger all-in-one stores traces in memory; a restart loses history.
- Prometheus retention is local and short; Grafana dashboards are provisioned but not alerting.
- DLQ replay is manual.
- Analytics is eventually consistent, so a dashboard can disagree with an order list for a few seconds.
- The full stack is heavy enough that running everything plus an IDE is uncomfortable on 16 GB.
- No log aggregation; correlation is by trace ID across `docker compose logs`.

---

## 5. Alternate solutions

**Alternative 1 — RabbitMQ for everything; no Kafka.** Already present for Celery; simpler operationally; adequate pub/sub via exchanges.

*Rejected.* RabbitMQ deletes a message once consumed, so a new consumer cannot read history — which breaks Milestone 5's indexer, any backfill, and the `raw_events` escape hatch. There is no replay, no offset, and no schema governance. Part A names Kafka *and* RabbitMQ precisely because they are different tools, and using one for both jobs would leave the retention and replay properties undemonstrated.

**Alternative 2 — Debezium CDC instead of an application-level outbox.** Read the WAL, no outbox table, no relay to write.

*Rejected.* It requires Kafka Connect, the Debezium connector, logical replication configuration, and a transform to shape rows into events — a heavier stack than the 150-line relay it replaces, and one that couples the event schema to the table schema unless the outbox pattern is used anyway. Debezium's own recommended approach for domain events is, in fact, an outbox table.

**Alternative 3 — Kafka transactions for exactly-once.**

*Rejected.* See §3.2. The guarantee does not extend past Kafka, so consumer idempotency is needed regardless, and the transaction adds coordination cost for a property already obtained.

**Alternative 4 — Celery for all asynchronous work; no Kafka.**

*Rejected.* Celery is point-to-point with no retention and no fan-out; every new consumer means the producer changes to enqueue another task. That is exactly the coupling events exist to remove, and Part A §4 asks for work that runs "independently from core request flows" with multiple reactions per fact.

**Alternative 5 — Postgres for analytics instead of MongoDB.** One less technology; SQL aggregation is excellent.

*Rejected.* Part A requires a NoSQL store, and this is the one workload in the system with a genuine document shape — per-metric documents whose fields differ, and a raw-event archive with no fixed schema. Adding Mongo for a token table elsewhere would be worse than using it here.

**Alternative 6 — Elastic APM or a single-vendor observability stack.**

*Rejected.* OpenTelemetry is what Part A names, and vendor-neutral instrumentation means the backend is a collector configuration rather than a code change. The three-backend split also demonstrates the signal separation the milestone is about.

**Alternative 7 — Skip Grafana; use Prometheus' own UI.** One fewer container.

*Rejected.* "Basic monitoring" is an explicit milestone outcome, and a dashboard is the artefact a reviewer can actually read. Grafana with provisioned dashboards-as-code is cheap.

**Alternative 8 — Log aggregation (Loki) in addition to traces.**

*Deferred.* The actual requirement is correlating logs with a request, which the trace ID in every log line already satisfies. At four log streams, `docker compose logs` is adequate; Loki would be a fifth observability container for marginal gain in this environment.

**Alternative 9 — Keep polling: the client calls `GET /notifications` on a timer.** No connections to hold, no fan-out bus, no gateway changes, and `notification` stays a plain request/response service.

*Rejected,* and it is the alternative this section exists to argue against. Two problems, both structural. Latency is bounded below by the poll interval, so "your payment failed" arrives up to ten seconds late by construction. And load scales with *idle users* rather than with events: 20,000 clients polling every 10s is 2,000 req/s of almost-always-empty responses, each one a JWT verification and an indexed query, and the rate rises precisely when a spike makes users open the app to check on their orders. [Part A](../../part-a.md) §1 names spike behaviour as the thing that is broken today, so a delivery mechanism whose cost peaks with anxiety is the wrong shape. Polling survives in one place only — as the client's fallback when SSE cannot connect.

**Alternative 10 — WebSockets instead of SSE.** Bidirectional, one well-known protocol, no ticket dance because auth can happen in the first frame.

*Rejected,* for the reasons [RFC 0005](0005-ai-assistant-and-streaming.md) E4 already rejected it for the assistant. The traffic is server → client only; nothing the client sends mid-stream matters, and marking a notification read is an ordinary `POST`. SSE is plain HTTP, so the existing gateway routing, JWT verification, rate limiting, tracing, and RFC 9457 error handling apply unchanged, and `EventSource` reconnects with `Last-Event-ID` natively — which is the entire recovery mechanism in C18, for free. WebSockets would mean a separate connection lifecycle, a stateful gateway path, a bespoke auth handshake, and a hand-written reconnect-and-resume client, to buy a bidirectionality no requirement asks for. It becomes the right answer the day the product needs typing indicators or client→server streaming; it is not that day.

**Alternative 11 — Every replica joins Kafka with its own consumer group and filters locally.** No Redis Pub/Sub, no second bus: each replica sees every event and delivers to whichever users it happens to hold.

*Rejected,* though it is elegant and genuinely tempting. It removes a moving part, but it makes consumer groups dynamic — created and abandoned per pod — which leaves offset garbage in the broker on every deploy, and it means every replica deserialises every event to discard most of them. Worse, it entangles the *delivery* path with the *persistence* path: the same consumer that must be idempotent and dedupe-backed (C4) would now also be the one fanning out to sockets, so a slow client and a stuck partition become the same problem. Splitting them is the point of C17 — Kafka consumption is partitioned and durable; delivery fan-out is broadcast and disposable.

**Alternative 12 — Redis Streams or a second Kafka topic for fan-out, giving at-least-once push.** Frames survive a Redis restart; delivery could be acknowledged per connection.

*Rejected* as protecting data that is not at risk. Under C16 the notification is already durable in Postgres before anything is published, so the fan-out bus is carrying a hint, not a record. Paying consumer-group bookkeeping, per-connection offsets, and trim policies to make that hint reliable buys a few seconds of latency in a rare failure — and the `seq` catch-up (C18) already covers that case with a query the system has to support anyway, since it also handles first connect, reconnect after sleep, and pod restart. Sticky sessions, the third variant of this idea, are rejected on the same ground plus the scaling cost they impose on every deploy.

---

## 6. Production readiness

### 6.1. Scalability and Fault tolerance

- **No event loss.** The outbox makes the state change and the event atomic; the relay's failure mode is duplication, which consumers absorb (G1, C4).
- **Poison messages cannot stall a partition.** Three retries with backoff, then DLQ, then offset commit (G4, C6).
- **Consumer lag is visible and bounded.** Exported to Prometheus and dashboarded; consumers scale horizontally up to the partition count.
- **Backpressure has a defined shape at every layer.** Kafka retains and consumers lag; RabbitMQ queues buffer per-queue so media cannot starve notifications; the gateway rate-limits; Temporal's task queue buffers. This is Part A's "degrades gracefully rather than collapsing", made concrete per layer rather than asserted.
- **Blast radius.** `analytics` or `notification` being down means events accumulate in Kafka and are processed on recovery — checkout is unaffected. Kafka being down means outbox rows accumulate in Postgres and publish on recovery — checkout is still unaffected. This is the strongest availability property the system has, and it follows directly from nothing synchronous being downstream of the order path.
- **Cache failure is not request failure.** A Redis outage degrades to uncached reads; the client never sees an error. Rate limiting fails open for the same reason.
- **Idempotent everywhere.** Producers may duplicate, consumers dedupe, tasks are re-runnable, and Beat sweeps are safe to run twice.
- **Connection scaling is horizontal and unsticky.** `notification` replicas hold sockets but no *replica-affine* state (C20). The two pieces of shared state — the per-device replay cursor in Postgres and the presence key in Redis — belong to no replica and both fail safe, so any replica serves any reconnect, no session affinity is configured, and adding a replica adds capacity with no rebalancing. ~10k idle SSE connections per replica; [Part A](../../part-a.md) §4's tens of thousands of concurrent users is 2–4 replicas.
- **Push failure degrades to latency, never to loss.** The notification is committed to Postgres before it is published (C16), so a dropped frame, a Redis restart, a killed replica, or a client that was offline all resolve identically: the next connection replays from `seq` (C18). There is no failure mode in the delivery path that loses a notification. There are failure modes that **downgrade** a frame to a list entry — a cold-started device (C22), a gap past the resume bound, an origin-scoped notification whose device never returns (C21) — and each of those is visible in the unread count and in the durable list, which are never scope- or cursor-filtered.
- **Slow clients cannot consume the server.** Each connection has a bounded queue; on overflow the connection is closed rather than buffered, which is safe precisely because reconnect is lossless. Unbounded per-connection buffering is the usual way a push system dies under load, and it is designed out rather than tuned.
- **Known ceilings.** One broker, one Mongo node, one Redis, in-memory Jaeger. All are single-node by choice and none is a design decision that blocks scaling.

### 6.2. Security considerations

- **Event payloads carry identifiers and the minimum needed to render.** No password hashes, no card data, no full addresses beyond what a shipment notification requires. Kafka is unauthenticated locally, so payload minimalism is the actual control.
- **Kafka, Schema Registry, RabbitMQ, Redis, Mongo, Prometheus, Grafana, and Jaeger are not exposed outside the Compose network**, except the UIs bound to localhost for the demo. Broker ports are never published.
- **Rate limiting** at the gateway closes the brute-force hole recorded in [RFC 0001](0001-foundation-and-core-services.md): 5 login attempts per minute per IP and per email, 30 orders per minute per user, 100 requests per minute per IP as a default.
- **The shipment webhook is HMAC-signed** with a timestamp and a replay window, and rejects unsigned or stale requests — the correct shape even for a simulated carrier.
- **Notification isolation** — a user reads only their own; the admin view requires `admin`. The SSE stream is subscribed to `nt:user:{user_id}` from the *redeemed ticket*, never from a client-supplied parameter, so one user cannot subscribe to another's channel.
- **`client_id` is client-minted and therefore forgeable, and is treated as such.** It is stored and queried only as part of `(user_id, client_id)`, with `user_id` taken from a verified token, so cross-user reach is unrepresentable rather than merely denied. It is never an authorisation input, never resolves a channel — the channel comes from the redeemed ticket — and never appears in a URL. It is a device correlator: not reused for analytics, pruned at 90 days, and capped at 20 rows per user, which is what closes the one real abuse (resource growth by a client that rotates its id, not access).
- **Stream tickets are single-use and short-lived** — 60 seconds, redeemed with an atomic `GETDEL`, so a ticket captured from a log or a browser history is almost always already spent. This is why the JWT itself is never placed in the stream URL (C19).
- **Long-lived connections are re-authorised, not authorised once.** A stream carries the token's expiry: when the underlying JWT would have expired, the stream closes with a typed `error` frame and the client re-authenticates. Otherwise a connection opened before a logout or a merchant suspension would outlive the decision indefinitely.
- **Analytics scoping** — a merchant's queries are filtered by the `merchant_id` in their token, in the aggregation pipeline, not in the response. A merchant must not be able to infer another merchant's revenue from a total.
- **Traces may contain URLs and user IDs**; request and response bodies are never recorded as span attributes, and headers are allow-listed.
- **Grafana** runs with a non-default admin password from `.env` and anonymous access disabled.
- **Accepted gaps** — no TLS or SASL on Kafka; no mTLS between services; Redis without auth on the internal network; Mongo with a single local credential. All are local-environment decisions, listed so they read as choices.

### 6.3. Deployment strategy

- **Compose profiles**, finalised this milestone: `core` (M1), `workflow` (M2), `events` (Kafka, Registry, RabbitMQ, Redis, Mongo, relay, consumers, Celery), `observability` (collector, Jaeger, Prometheus, Grafana), `ai` (M5). `make up` runs `core` + `workflow` + `events`; `make up-all` adds observability.
- **Topic and schema provisioning** is an idempotent init script — `make topics` — not manual console commands, so a cold clone reproduces the pipeline exactly.
- **Grafana dashboards and datasources are provisioned as code** from `ops/grafana/`, so they survive `make reset` and are reviewable in a diff.
- **Startup ordering** — Kafka before the relay and consumers, with health-gated `depends_on`. Consumers retry broker connection with backoff rather than crash-looping.
- **Consumers deploy independently of their services.** A consumer restart never interrupts an API.
- **Connection draining on deploy.** A rolling restart of `notification` closes streams with a jittered `retry:` hint and a grace period rather than dropping them, so reconnects spread instead of arriving as a thundering herd. No sticky sessions or session affinity are configured, by C20.
- **The gateway needs streaming-specific configuration** — proxy buffering off, no compression on `text/event-stream`, an extended read timeout, and the per-IP rate limit exempted on the stream route. Misconfiguring any of these degrades silently into "notifications feel slow" rather than failing loudly, which is why T16c tests it by measurement.
- **Rollback.** Per-service Alembic downgrade; schema registry versions are additive and never deleted; a bad consumer is rolled back and its offsets rewound if needed.

---

## 7. Test Plan

**Unit**

- Envelope construction and Avro round-trip for all fourteen schemas.
- Schema compatibility: a deliberately incompatible change is rejected by the registry checker in CI.
- Consumer base class: dedupe hit and miss, retry counting, DLQ routing, offset commit on skip.
- Metric aggregation functions, given a synthetic event sequence.
- Rate limiter: window boundaries, per-scope isolation, fail-open on a Redis error.
- Notification template rendering for each of the six types.
- Trace context extraction from and injection into the envelope.

**Integration** — real Kafka (testcontainers or a Compose service), real Postgres, real Mongo.

*relay*
- A committed outbox row reaches its topic with the right key and schema.
- **Crash between produce and mark-published → the row republishes; the consumer dedupes; the effect happens once** (G1).
- Two relay instances do not double-publish (`SKIP LOCKED`).
- Ordering: three events for one aggregate arrive on one partition in order (C3).

*consumers*
- Each of the fourteen topics is consumed by its declared consumers and produces the declared effect.
- **Replay: reset offsets to earliest, re-consume everything, assert every counter and record is unchanged** (G3).
- A malformed event goes to DLQ; the next event is processed normally (G4).
- A consumer down during publication catches up fully on restart.

*notification*
- Each of the six event types produces a notification with correct rendering and `user_id` (G5).
- Dispatch failure is retried by Celery and recorded as attempts.
- Isolation: another user's notification → `404`.

*notification — delivery (SSE)*
- **Two replicas: a client streams from replica A, the event is consumed by replica B, the frame arrives at the client** (G5a). This is the test that proves C17; a single-replica test would pass without any fan-out at all and is therefore not sufficient.
- **Resume: connect, receive up to `seq` N, kill the connection, publish M more, reconnect with `Last-Event-ID: N` → exactly M frames, in `seq` order, no duplicates, no holes** (G5b).
- **Cold start**: 500 seeded notifications, an unseen `client_id`, no `Last-Event-ID` → **exactly 0** `notification` frames, one `hello` with `cold_start: true` and the exact `unread_count`; a notification published afterwards arrives live (G5c).
- **Warm resume through the server cursor, with no `Last-Event-ID`**: ack to `seq` N, drop the connection, publish M, reconnect *without* the header → exactly M frames. This is the page-reload case, and the pre-revision design cannot pass it.
- **Bound enforcement**: 200 missed → 50 frames in ascending `seq` plus `catchup_truncated{omitted_count: 150}`; `GET /notifications?since_seq=` returns the rest (G5b).
- **Age bound**: a notification 25 hours old is not replayed even when the cursor sits below it, and is present in the list.
- **Ack idempotency**: the same payload twice → one `delivered_at`, an unchanged `delivered_client_id`, and an unmoved cursor (G5e).
- **Ack safe-seq clamp**: commit `seq` 84215 before 84214, ack 84215, reconnect after the 5-second window → 84214 is still delivered. The trap in §3.1.1a, asserted against the ack rather than only against catch-up.
- **Ack of a foreign id**: user B acks user A's notification id → `204`, and A's row is untouched.
- **Origin targeting**: clients A and B for one user; `order_confirmed` with `origin_client_id = A` → the frame reaches A only, the `unread_count` frame reaches **both**, and `GET /notifications` on B contains it (G5d).
- **Null origin**: `shipment.status_changed` from the carrier webhook → delivered to both A and B, despite the scope table.
- **Promotion**: `order_failed` with origin A while A is disconnected → no frame at first, `promoted_at` set by the sweeper, frame at B within the stated bound; running the promoter twice promotes once.
- **Presence false positive**: kill A's process without a clean close → the targeted frame is delivered nowhere, the key expires, and the promoter widens it.
- **Spoofing**: user B opens a stream with a ticket minted from B's token but carrying A's `client_id` string → B's own stream, A's notifications never reachable, and a `stream_cursors` row created under B.
- **Client-id cap**: 30 distinct `client_id`s for one user → at most 20 rows survive, evicted by `last_seen_at`.
- **Avro compatibility**: a v1-schema consumer reads a v2 record (unknown field ignored); a v2 consumer reads a v1 record (`client_id` resolves to null); the registry accepts the v2 registration under `BACKWARD`.
- **Gateway**: a malformed `X-Client-Id` is stripped and the mutation still succeeds, with `origin_client_id IS NULL`.
- Unit: the scope-derivation function over all twelve `type` × origin-presence combinations, and the catch-up bound calculator.
- The `seq` visibility window: two notifications committed out of sequence order are both delivered, and the client de-duplicates by `id` — the trap named in §3.1.1a, asserted rather than assumed.
- A stream ticket is single-use: the second redemption of the same ticket → `401`. An expired ticket → `401`. A ticket for user A cannot stream user B's channel.
- A connection whose token has expired is closed with a typed `error` frame rather than left open.
- The 3-stream-per-user cap rejects the fourth; heartbeat frames keep an idle connection alive past the proxy timeout.
- A slow consumer overflows its queue, is disconnected, and recovers its missed notifications on reconnect — asserting that dropping a connection is lossless.
- Through the gateway: the first frame is not buffered, and push latency from consume to client receipt is measured (G5a, T16c).

*analytics*
- **One test per metric** (G6), each asserting an exact value after a scripted event sequence — for example: three `user.registered` with `role=customer` → `total_customers == 3`; two confirmed orders of 100 and 200 → revenue 300 and average order value 150.
- Merchant scoping: merchant A's revenue query excludes merchant B's orders entirely.
- Daily bucketing across a date boundary lands in the right documents.
- Idempotency: the same event consumed twice moves a counter once.

*Redis*
- Browse cache hit, miss, and invalidation on `product.updated`.
- Rate limit returns `429` with `Retry-After` at the threshold and recovers after the window.
- Redis down → reads still succeed, limits fail open.

**Observability tests**

- **A checkout produces exactly one trace containing spans from gateway, order, inventory, the Temporal workflow, the relay, and the notification consumer** (G7). Asserted by querying the Jaeger API for the trace ID and checking the service set — not by looking at the UI.
- Every service's `/metrics` exposes request count, error count, and latency histogram, and Prometheus scrapes every target successfully (G8).
- Every log line emitted during one request carries the same trace ID (C12).
- Consumer lag, DLQ depth, outbox backlog, and active SSE connections are exported and non-null.

**End-to-end** — through the gateway, cold start, full stack.

1. Register a customer and a merchant → assert `user.registered` consumed and `total_customers` incremented.
2. Publish a product → assert `product.published` consumed, stock row created by the inventory consumer, `total_products_published` incremented, search document persisted.
3. Place and confirm an order **with an SSE stream already open for the customer** → assert `order.created` and `order.confirmed` on their topics, a notification row for the customer, **the notification frame received on the open stream within a second**, revenue and popularity updated, and one trace spanning all five components.
4. Place an order that fails payment → `order.failed` consumed, a failure notification recorded, the failure counter incremented by reason.
5. Order a product to exhaustion → `inventory.stockout` produced and reflected in the stockout rate.
6. Suspend a merchant → `merchant.status_changed` consumed by `catalog`; the merchant's products disappear from browse without any synchronous call (Q6).
7. Inject a malformed event → DLQ receives it, the pipeline keeps running, the DLQ depth metric moves.

**Milestone review walkthroughs** — G10 requires demonstrating diagnosis, not just instrumentation. One walkthrough per [Part A](../../part-a.md) §6 flow, each starting from a symptom:

| Symptom | Path to the cause |
| --- | --- |
| "A product never published" | `publish-status` → workflow id → Temporal UI → failing activity → the trace for that activity → the exception |
| "Checkout is slow" | Grafana latency panel → the slow endpoint → an exemplar trace → the slow span (catalog call, or reservation lock wait) |
| "Stock looks wrong" | Ledger reconciliation query → the movement rows → `reference_id` → the order → its trace |
| "A background job didn't run" | Flower / Celery metrics → queue depth → task failure → the task's trace and exception |
| "A customer got no notification" | `orders.notified_at` → the `order.confirmed` event id → consumer dedupe table → DLQ → delivery attempts |
| "A customer got it late, or only on refresh" | Active-connection metric (was the client even connected?) → fan-out publish→frame latency → reconnect rate → catch-up rows per reconnect. Distinguishes "never pushed" from "pushed to nobody" from "delivered on reconnect" |

**Explicitly not tested** — load and throughput beyond functional concurrency; Kafka failover (single broker); alerting.

---

## 8. Impact Analysis

**On Milestones 1–2.** Mostly additive, because the outbox groundwork was laid early. Services gain a `processed_events` table if they consume, an OTLP exporter, and a `/metrics` endpoint. The only behavioural changes are rate limiting, caching, and media processing moving to Celery — each recorded in §3.1.4.

**On other RFCs.** Origin-scoped delivery (C21) reaches outside this RFC in two places, both additive and both recorded here so neither is discovered during implementation:

| RFC | Change | Why it cannot live in this RFC |
| --- | --- | --- |
| [0000](0000-system-architecture.md) §3.1.2 | The event envelope gains `client_id` (`"opaque client token \| null"`) | The envelope is defined once, there. Restating it here is the duplication [RFC 0006](0006-service-decomposition-and-data-model.md) §3.1.3.2 exists to prevent |
| [0002](0002-core-business-logic-and-workflows.md) | `order_db.orders` gains `origin_client_id`, written at order creation | `order.confirmed` is emitted by the Temporal workflow after the request ended, so the origin has to be persisted to survive the gap (§3.1.1a). Columns are specified in [RFC 0006](0006-service-decomposition-and-data-model.md#orders) |

**On later milestones.**

| Milestone | What this RFC makes easy | What it makes harder |
| --- | --- | --- |
| 4 — Part A review | Every Part A requirement now has a demonstrable artefact: events flowing, metrics computed, one trace across five components, five diagnosis walkthroughs | — |
| 5 — Retrieval | `indexer-worker` is one more consumer of `catalog.product.published`, using the same base class, and `products.search_document` is already persisted so a backfill needs no workflow replay | The full stack plus a vector store is the heaviest configuration of the project |
| 6 — AI assistant | `assistant.interaction` is already in the catalogue; the four Part B metrics slot into the existing analytics pipeline; tracing covers LLM calls automatically once the client is instrumented. **`sse-starlette` and the gateway's streaming pass-through now land here instead, so [RFC 0005](0005-ai-assistant-and-streaming.md) T14 is largely done in advance and its streaming risk drops** | Streaming responses need care in tracing — a span must not close at first byte |

**On effort.** The largest milestone in the project: two services, five infrastructure systems, seven Celery tasks, fourteen topics, and three observability backends. The action plan's ordering is the mitigation — T4 proves the whole event path on one topic before T5 widens it, and observability (T6–T11) lands mid-week so it can be used to debug everything after it.

**Risks.**

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| The milestone does not fit in a week | **High** | High | Priority order if time runs short: event pipeline (T1–T5) > analytics metrics (T17–T19) > tracing (T6–T9) > notifications (T14–T16) > SSE delivery (T16a–T16h) > Celery migration (T13) > caching (T20) > dashboards (T11). The first three are the milestone's named outcomes; the rest can be thinner |
| Local resources exhausted | **High** | High | Profiles; `make up-min`; documented memory expectations; Jaeger in-memory rather than with a store |
| Kafka + Avro + Registry learning curve | High | Medium | T1–T4 are a single-topic spike before anything is widened |
| Consumer idempotency implemented shallowly and only failing on replay | Medium | High | The replay test (G3) is an acceptance criterion, not an optional extra |
| Trace context silently dropped across Kafka, leaving fragmented traces | Medium | Medium | G7 is asserted programmatically against the Jaeger API, not judged by eye |
| Metric definitions disagreeing with the reviewer's reading | Medium | Low | Definitions stated explicitly in §3.1.1 and raised as Q10 |
| Cache invalidation bugs producing stale catalog reads | Medium | Medium | Short TTLs mean every bug self-heals within a minute; invalidation is tested |
| SSE push added to the heaviest milestone in the project | Medium | Medium | T16a–T16h sit on infrastructure that already exists (Redis from T20, the gateway from M1), so the marginal cost is ~2 days. Q13 states the fallback explicitly: ship T16a+T16f for single-replica push with bounded catch-up, and move fan-out and scoping to M4 |
| An implementer scope-filters the unread count or the notification list “for consistency” | Medium | **High** | This is the one change that turns origin targeting from a UX decision into silent notification loss. C21 states the invariant in the constraint itself, and G5d asserts that the non-origin device's badge still moves |
| Gateway buffering silently degrading push into delayed polling | Medium | Medium | The failure looks like "notifications feel slow", not like an error, so T16c asserts it by **measuring** first-frame latency through the gateway rather than by inspection |

**On reviewers.** Milestone 3's review should be able to check: an event survives a relay crash; replaying a topic changes nothing; a poison message does not stop the pipeline; each of the nine metrics has a defensible definition and a test; one checkout is one trace across five components; and each of Part A §6's five flows can be diagnosed from a symptom.

---

## 9. Useful Links/References

**Project documents**

- [RFC 0000 — System Architecture](0000-system-architecture.md) · [RFC 0001](0001-foundation-and-core-services.md) · [RFC 0002](0002-core-business-logic-and-workflows.md)
- [Part A](../../part-a.md) §4, §5, §6 · [Milestones](../../milestones.md) · [Execution Guidelines](../../execution-guidelines.md)

**Technical references**

- Transactional outbox — https://microservices.io/patterns/data/transactional-outbox.html
- Confluent Schema Registry compatibility — https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html
- `confluent-kafka-python` — https://docs.confluent.io/kafka-clients/python/current/overview.html
- Kafka idempotent producer and delivery semantics — https://kafka.apache.org/documentation/#semantics
- Celery routing and task design — https://docs.celeryq.dev/en/stable/userguide/routing.html
- OpenTelemetry Python — https://opentelemetry.io/docs/languages/python/
- OTel Collector configuration — https://opentelemetry.io/docs/collector/configuration/
- W3C Trace Context — https://www.w3.org/TR/trace-context/
- RED method — https://grafana.com/blog/2018/08/02/the-red-method-how-to-instrument-your-services/
- PostgreSQL `SKIP LOCKED` — https://www.postgresql.org/docs/16/sql-select.html#SQL-FOR-UPDATE-SHARE
- MongoDB TTL indexes — https://www.mongodb.com/docs/manual/core/index-ttl/
- HTML Living Standard, Server-Sent Events (`Last-Event-ID`, `retry:`, reconnection) — https://html.spec.whatwg.org/multipage/server-sent-events.html
- `sse-starlette` — https://github.com/sysid/sse-starlette
- Redis Pub/Sub — https://redis.io/docs/latest/develop/interact/pubsub/
- Why `bigserial` gaps break naive cursors — https://www.postgresql.org/docs/16/functions-sequence.html
- Web Push protocol / VAPID (the deferred offline channel) — https://datatracker.ietf.org/doc/html/rfc8292

---

## 10. Related tickets/issues

| ID | Title | Maps to |
| --- | --- | --- |
| *(to be created)* | M3-T1 … M3-T26, plus M3-T16a … M3-T16d | The tasks in §3.1.5, one ticket each |
| — | Milestone 3 — Events & Observability | This RFC |
| — | RFC 0001 Q2 | Merchant suspension propagation — closed here (Q6) |
| — | RFC 0001 §4 | Login rate limiting — closed here |
| — | RFC 0002 Q7 | Media processing moved to Celery — closed here (T13) |
| — | RFC 0004 — Retrieval Layer | Consumes `catalog.product.published` produced here |

---

## 11. Notes

- **On sequencing within the week.** T4 — one topic, end to end, with a crash test — is the most important task in the milestone. Everything after it is repetition of a proven pattern. Widening to fourteen topics before the first one survives a restart would mean debugging fourteen things at once.
- **On what "basic" means.** The milestone text says "basic analytics metrics" and "basic monitoring". Basic here means all nine of Part A's metrics computed correctly with a test each, and RED metrics with dashboards — not a subset. What is deliberately *not* included is alerting, SLOs, and log aggregation, listed in §2.2.
- **On the debt this closes.** Three known issues carried from earlier RFCs end here: login rate limiting, merchant-suspension propagation, and inline media processing. Each is marked in §10.
- **On resource pressure.** This is the week the laptop becomes the constraint. The profiles are not a convenience; they are how the milestone stays workable. Expect ~8 GB for the full stack.
- **Definition of done for Milestone 3.** Goals G1–G10 plus G5a/G5b verified by their stated checks; `make up-all && make migrate && make seed && make test` green from a clean clone; the replay, poison-message, single-trace, and two-replica-push tests green; Grafana dashboards loading with live data; the five diagnosis walkthroughs written up; this RFC moved to **DONE** with review outcomes folded in.

---

## 12. Comments

*Reviewer comments go here. Please raise objections against the numbered decisions (C1–C20), the open questions (Q1–Q13), the metric definitions in §3.1.1, or the rejected alternatives in §5, so the discussion attaches to something specific.*

| Reviewer | Section | Comment | Resolution |
| --- | --- | --- | --- |
| | | | |
