# RFC 0001 — Platform & Shared Infrastructure

| | |
| --- | --- |
| **Owner** | @muhammad.adnan |
| **Collaborators** | SarwanAhmed |
| **Status** | **DRAFT** · IN-REVIEW · DONE |
| **Created** | Sep 29, 2026 |
| **Supersedes** | Platform and infrastructure content of [archive/0000](archive/0000-system-architecture.md) (non-HTTP components, infrastructure table, container counts, deployment), [archive/0001](archive/0001-foundation-and-core-services.md) (`common`, repository layout, local setup), [archive/0002](archive/0002-core-business-logic-and-workflows.md) (Temporal and MinIO setup, outbox helper, idempotency helper, service tokens), [archive/0003](archive/0003-events-and-observability.md) (Kafka, Schema Registry, `outbox-relay`, RabbitMQ and Celery, Redis, the OpenTelemetry stack, Compose profiles), [archive/0004](archive/0004-retrieval-layer.md) (`indexer-worker` runtime, `ai` profile), [archive/0005](archive/0005-ai-assistant-and-streaming.md) (Celery bulk queue, budget fast path), [archive/0006](archive/0006-service-decomposition-and-data-model.md) (S4 mechanism, R8, deployment of roles and migrations) |
| **Depends on** | [RFC 0000 — Architecture](0000-architecture.md) |
| **Depended on by** | [0002 gateway](0002-gateway-service.md) · [0003 identity](0003-identity-service.md) · [0004 catalog](0004-catalog-service.md) · [0005 order](0005-order-service.md) · [0006 inventory](0006-inventory-service.md) · [0007 notification](0007-notification-service.md) · [0008 analytics](0008-analytics-service.md) · [0009 assistant](0009-assistant-service.md) |

> **Reading note.** This RFC specifies everything in SmartRetail that is not an HTTP
> service: the shared library `common`, the four worker runtimes
> (`outbox-relay`, `temporal-worker`, `celery-worker`, `indexer-worker`), the
> infrastructure they run on (Kafka and Schema Registry, RabbitMQ, Redis, MinIO,
> Temporal, PostgreSQL provisioning), the observability stack, and the local
> environment (Compose, Makefile, Dockerfiles, migrations, seeding, secrets).
> Cross-cutting rules — the boundary rules R1–R8, the decisions A1–A14, the event
> catalogue and envelope, the schema conventions S1–S15, and the DDL of the shared
> `outbox` and `processed_events` tables — are defined once in
> [RFC 0000](0000-architecture.md) and are only referenced here. Business logic that
> *runs inside* a worker (the publishing and order sagas, notification dispatch,
> chunking and embedding, rollups) is specified in the RFC of the service that owns
> it; this document covers hosting, runtime behaviour and failure handling only.
> Decision IDs from the archive are kept verbatim (A*, B*, C*, D*, E*, R*, S*).
> Archived open questions are numbered per source RFC, so they are written with the
> source prefix — `0003-Q1` is Q1 of [archive/0003](archive/0003-events-and-observability.md).
> New decisions made in this RFC are prefixed `PLT-`.

---

## Contents

1. [Title](#1-title)
2. [Overview](#2-overview)
   - 2.1. [Goals](#21-goals)
   - 2.2. [Non-goals](#22-non-goals)
3. [Solution Overview](#3-solution-overview)
   - 3.1. [Specification](#31-specification)
     - 3.1.1. [Architectural changes](#311-architectural-changes)
       - 3.1.1.1. [common](#3111-common)
       - 3.1.1.2. [outbox-relay](#3112-outbox-relay)
       - 3.1.1.3. [temporal-worker](#3113-temporal-worker)
       - 3.1.1.4. [celery-worker](#3114-celery-worker)
       - 3.1.1.5. [indexer-worker runtime](#3115-indexer-worker-runtime)
       - 3.1.1.6. [Kafka + Schema Registry](#3116-kafka--schema-registry)
       - 3.1.1.7. [RabbitMQ](#3117-rabbitmq)
       - 3.1.1.8. [Redis](#3118-redis)
       - 3.1.1.9. [MinIO](#3119-minio)
       - 3.1.1.10. [Temporal server](#31110-temporal-server)
       - 3.1.1.11. [Observability stack](#31111-observability-stack)
       - 3.1.1.12. [Local environment — Compose, Makefile, Dockerfiles](#31112-local-environment--compose-makefile-dockerfiles)
     - 3.1.2. [API changes](#312-api-changes)
       - 3.1.2.1. [common public API](#3121-common-public-api)
       - 3.1.2.2. [The event envelope on the wire](#3122-the-event-envelope-on-the-wire)
       - 3.1.2.3. [Avro schema registration flow](#3123-avro-schema-registration-flow)
       - 3.1.2.4. [Operational endpoints — /health, /health/ready, /metrics](#3124-operational-endpoints--health-healthready-metrics)
       - 3.1.2.5. [Worker contracts — task queues, Celery queues, consumer groups](#3125-worker-contracts--task-queues-celery-queues-consumer-groups)
     - 3.1.3. [Data/schema changes](#313-dataschema-changes)
       - 3.1.3.1. [PostgreSQL instance, databases and roles](#3131-postgresql-instance-databases-and-roles)
       - 3.1.3.2. [Redis keyspaces](#3132-redis-keyspaces)
       - 3.1.3.3. [Kafka topic configuration](#3133-kafka-topic-configuration)
       - 3.1.3.4. [MinIO buckets and key layout](#3134-minio-buckets-and-key-layout)
       - 3.1.3.5. [Temporal namespaces](#3135-temporal-namespaces)
       - 3.1.3.6. [MongoDB instance](#3136-mongodb-instance)
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

**Platform & Shared Infrastructure — the shared library, the worker runtimes, the event backbone, the queues and caches, object storage, workflow hosting, the observability stack, and the local environment that every SmartRetail service stands on.**

The eight services in [RFC 0000 §3.1.1.1](0000-architecture.md#3111-service-inventory) each own a piece of state. Everything in this RFC owns none. It is the machinery that lets those services verify a caller the same way, fail the same way, publish an event without losing it, consume one without applying it twice, run work off the request path, and be followed end to end by a single trace — and that lets a developer run any subset of the whole on one laptop.

---

## 2. Overview

[Part A](../part-a.md) names the stack: FastAPI, PostgreSQL, a NoSQL store, Redis, Celery with RabbitMQ, Kafka with Schema Registry, Temporal, OpenTelemetry, Prometheus, Grafana, Jaeger, Docker and Docker Compose. [Part B](../part-b.md) adds a vector store and an LLM provider. Neither says how those pieces are wired, which of them a service may touch, or what happens when one of them is down. Those are the decisions this RFC makes.

Three rules shape all of it.

1. **The platform owns no business state.** No table in this RFC is a domain table. `common` holds mechanisms, never vocabularies or rules (R8). Workers host code that belongs to a service; they do not own what that code writes (PLT-1).
2. **Every asynchronous path has a defined failure shape.** An event that cannot be published stays in the outbox. An event that cannot be processed goes to a DLQ after bounded retries. A task that cannot run is retried and recorded. A dependency that is down degrades a path rather than failing it, wherever the owning service can tolerate that.
3. **Every component is observable in the same way.** One trace across HTTP, Kafka, Temporal and Celery; one metric naming scheme with one label vocabulary; one JSON log line shape carrying the trace ID.

The platform also carries the one constraint that is not a design choice: the laptop. The full stack is twenty-odd containers. Compose profiles are how the project stays workable, and they are part of the design, not a convenience ([archive/0003 §11](archive/0003-events-and-observability.md#11-notes)).

### 2.1. Goals

| # | Goal | How it is verified |
| --- | --- | --- |
| G1 | Every outbox row reaches Kafka exactly once, in per-aggregate order, with no loss across a crash | Relay killed mid-batch and restarted; every row ends `published_at IS NOT NULL` with no duplicate `event_id` on the topic |
| G2 | Every published event validates against a registered schema, with `BACKWARD` compatibility enforced | Contract test per topic; an incompatible schema registration is rejected by the registry |
| G3 | Consumers are idempotent — replaying the entire topic changes nothing | Replay test: reset consumer offsets to earliest, re-consume, assert every counter and record is unchanged |
| G4 | Failed events are retried with backoff and land in a DLQ rather than blocking the partition | Poison-message test: one bad event goes to DLQ, the partition keeps moving |
| G5 | One request is traceable end to end across gateway, service, workflow, event, and consumer | Jaeger shows a single trace spanning all five for one checkout, asserted through the Jaeger API |
| G6 | RED metrics for every service and consumer, on a Grafana dashboard | Dashboard loads with live data; every panel has a query that returns |
| G7 | Background work is off the request path | No handler performs media processing, email rendering, or rollup computation inline |
| G8 | Failure diagnosis is possible for each of [Part A](../part-a.md) §6's five named flows | A walkthrough per flow, starting from a symptom and reaching a cause (§7) |
| G9 | Cross-cutting conventions are implemented once — auth, errors, pagination, config, idempotency, tracing — and reused rather than reimplemented per service | Every service imports them from `common`; no service defines its own error handler, JWT verifier or pagination envelope |
| G10 | `common` contains no business rule and no domain vocabulary (R8) | An AST scan of `common` for any service's domain status literal returns nothing, and that scan is a unit test |
| G11 | Local setup is one command per concern and works from a cold clone | `make reset && make up-all && make migrate && make seed && make test` green on a machine that has never run it; API docs reachable |
| G12 | Any subset of the stack can be run on its own | `make up-min`, `make up`, `make up-events`, `make up-observability`, `make up-ai`, `make up-all` each start a healthy subset |
| G13 | Schema is version-controlled per service and migrations are an explicit step | Alembic takes every database from empty to head in CI and locally; no container migrates on start |

### 2.2. Non-goals

| Excluded | Why / owner |
| --- | --- |
| Business logic inside workers — saga steps, compensations, chunking, embedding, rollup maths, dispatch rendering | Owned by the service whose state it changes: [0004](0004-catalog-service.md), [0005](0005-order-service.md), [0006](0006-inventory-service.md), [0007](0007-notification-service.md), [0008](0008-analytics-service.md), [0009](0009-assistant-service.md). This RFC hosts them |
| Gateway routing, rate-limit *policy* and SSE proxying | [RFC 0002](0002-gateway-service.md). This RFC supplies the Redis keyspace and the limiter mechanism |
| Kubernetes, cloud deployment, managed services, CI/CD pipelines | The target environment for the whole assignment is Docker Compose on a developer machine ([Execution Guidelines](../execution-guidelines.md)) |
| Alerting — Alertmanager, PagerDuty, on-call | No operational context to alert into. Dashboards and traces cover the stated requirement (`0003-Q7`) |
| Log aggregation — Loki, ELK | Structured JSON to stdout plus `docker compose logs` is sufficient at this scale. Correlation, which is the actual requirement, comes from the trace ID being in every line |
| Kafka Streams, ksqlDB, Flink | Consumers in Python are adequate for the metric set; a stream processor is a large new concept for no requirement |
| Multi-broker Kafka, replication factor > 1 | One laptop. Topic configuration makes the change a variable |
| Exactly-once Kafka semantics (transactions) | At-least-once plus idempotent consumers is the standard, simpler, and sufficient answer (C4, §3.2) |
| CDC via Debezium instead of the application-level outbox | §5, Alternative 2 |
| Event sourcing as primary persistence | [RFC 0000](0000-architecture.md) §5 |
| TLS, SASL, mTLS between components | Local environment. Listed as accepted gaps in §6.2 so they read as choices |
| A schema-convention linter for S1–S15 | A genuine improvement, out of scope; conventions are enforced by the tests in [RFC 0000 §7](0000-architecture.md#7-test-plan) |
| Load and throughput testing | No traffic figure is specified anywhere in Parts A or B. Capacity statements here are structural |

---

## 3. Solution Overview

### 3.1. Specification

#### 3.1.1. Architectural changes

**Where the platform sits.** Services own state; the platform moves it, schedules work over it, and watches it.

```
                         client ── HTTP / JSON / SSE ──▶ gateway (:8000)          [RFC 0002]
                                                            │
      ┌───────────────┬──────────────┬───────────────┬──────┴───────┬──────────────┬──────────────┐
      ▼               ▼              ▼               ▼              ▼              ▼              ▼
  identity        catalog         order          inventory    notification     analytics      assistant
  (:8001)         (:8002)         (:8003)        (:8004)        (:8005)         (:8006)        (:8007)
      │  every service imports common: auth · config · logging · errors · pagination · db ·
      │  idempotency · outbox · events · consumer base · tracing · metrics · DomainEnum
      ▼
 ┌──────────────── PostgreSQL 16 (pgvector image) — one instance, one database + role per service ──┐
 │ identity_db · catalog_db · order_db · inventory_db · notification_db · assistant_db · temporal*  │
 └──────┬─────────────────────────────────────────────────────────────────────────────────────────┘
        │ outbox (5 producer DBs)                                   MongoDB (analytics_db) ◀─┐
        ▼                                                                                     │
 ┌──────────────┐  produce   ┌────────────────────────────────────────┐  consume              │
 │ outbox-relay │──────────▶│ Kafka (KRaft) + Schema Registry (Avro)  │──────────┬────────────┤
 └──────────────┘            │ smartretail.<domain>.<event> + .dlq     │          │            │
                             └────────────────────────────────────────┘   service consumers  indexer-worker
                                                                          (catalog, inventory, (assistant code)
 ┌──────────────────┐ gRPC ┌─────────────────────┐                        notification, analytics)
 │ temporal-worker  │◀────▶│ Temporal server + UI │   ┌───────┐
 │ publishing, order│      └─────────────────────┘   │ MinIO │◀── catalog upload, media task
 └──────────────────┘                                 └───────┘
 ┌──────────────────┐ AMQP ┌──────────┐      ┌─────────────────────────────────────────────┐
 │ celery-worker    │◀────▶│ RabbitMQ │      │ Redis: rate limits · idempotency fast path · │
 │ (+ Beat)         │      └──────────┘      │ cache · notification pub/sub · SSE tickets · │
 │ celery-worker-ai │                        │ presence · token budgets · Celery results    │
 └──────────────────┘                        └─────────────────────────────────────────────┘

 every component ── OTLP traces ──▶ OTel Collector ──▶ Jaeger
 every component ── /metrics ◀── scrape ── Prometheus ──▶ Grafana
 every component ── JSON logs (trace_id on every line) ──▶ stdout
```

**Decisions this RFC carries.** Each is defined where its archive text put it and is restated here because this RFC implements it.

| ID | Decision | Implemented in |
| --- | --- | --- |
| A6 | Async all the way down — FastAPI `async def`, SQLAlchemy 2.0 async, `asyncpg`; no sync I/O inside the event loop | `common` DB session (§3.1.1.1) |
| A7 | RFC 9457 problem details, one shape everywhere, produced by one handler in `common` | §3.1.1.1 |
| A8 | `X-Request-ID` in, W3C `traceparent` out; from the event backbone onwards it is the OpenTelemetry trace ID | §3.1.1.11 |
| A10 | Transactional outbox for every state-change event; `outbox-relay` publishes | §3.1.1.1, §3.1.1.2 |
| A11 | Consumers are idempotent and keyed | Consumer base, §3.1.1.1 |
| A12 | Temporal owns multi-step business processes; Kafka owns notifications of fact; Celery owns fire-and-forget tasks | §3.1.1.3, §3.1.1.4, §3.1.1.6 |
| B1 | Activities are thin adapters over the service layer; no business rule in workflow code | §3.1.1.3 |
| B2 | Workflows hold no business data, only identifiers and step results | §3.1.1.3 |
| B3 | Every activity is idempotent and keyed by a deterministic idempotency token derived from `(workflow_id, activity_name, attempt-invariant input)` | §3.1.1.3 |
| B7 | Idempotency keys are claimed by a unique insert (`INSERT … ON CONFLICT DO NOTHING`), not by a read-then-write | §3.1.1.1 idempotency mechanism |
| B11 | The outbox is written in the state-change transaction, in every producing service | §3.1.1.1 outbox writer |
| C1 | The outbox relay is a separate process, not a thread in each service; the only writer of `published_at` | §3.1.1.2 |
| C2 | The relay uses `FOR UPDATE SKIP LOCKED` to claim a batch, so multiple relay instances can run without double-publishing, and a stuck row never blocks the ones behind it | §3.1.1.2, refined by PLT-2 |
| C3 | Ordering is per aggregate, not global — the Kafka key is `aggregate_id` | §3.1.1.2, §3.1.1.6 |
| C4 | At-least-once delivery with idempotent consumers, not Kafka transactions; `(consumer_group, event_id)` recorded in the same transaction as the effect | Consumer base |
| C5 | Avro with Schema Registry, `BACKWARD` compatibility; incompatible changes fail in CI | §3.1.1.6, §3.1.2.3 |
| C6 | Retry in-process with backoff, then DLQ: three attempts, then `<topic>.dlq` with its error, and the offset is committed | Consumer base, refined by PLT-5 |
| C7 | Celery handles one-shot jobs; Kafka handles facts; Temporal handles processes. Media processing moves out of the Temporal activity into Celery; notification dispatch is a Celery task; scheduled rollups run on Beat | §3.1.1.4 |
| C10 | One OTel Collector; services export OTLP to it and never talk to Jaeger directly, so changing a backend is collector configuration | §3.1.1.11, refined by PLT-7 |
| C11 | Trace context propagates over HTTP *and* over Kafka — `traceparent` travels with the event and is restored by the consumer | §3.1.1.11, PLT-6 |
| C12 | The trace ID replaces the request ID as the correlation identifier; `X-Request-ID` is retained as an alias and carries the trace ID | §3.1.1.11 |
| C13 | Redis arrives with concrete jobs, not speculatively: gateway rate limiting, the idempotency-key fast path, caching of the product browse query — and, by extension, Celery results, notification fan-out, SSE tickets and token budgets | §3.1.1.8 |
| C14 | The observability stack is a Compose profile, off by default | §3.1.1.12 |
| C17 | Notification fan-out between replicas is Redis Pub/Sub, one channel per connected user (mechanism here; delivery semantics in [0007](0007-notification-service.md)) | §3.1.1.8 |
| D3 | Indexing is event-driven, never polled; `indexer-worker` is one more consumer on the shared base class | §3.1.1.5 |
| E12 | Generation is synchronous-streaming for interactive use and Celery-backed for bulk | §3.1.1.4 |
| R8 | `common` contains no business rule | §3.1.1.1 R8 guard |
| S4 | Enums as `text`; the mechanism (`DomainEnum`, `DomainEnumText`) lives in `common`, the vocabularies in each service | §3.1.1.1; the rule is [RFC 0000 §3.1.3.1](0000-architecture.md#3131-schema-conventions-s1s15) |
| S12, S13 | Identical `outbox` DDL in every producer; `processed_events` in every consumer | DDL in [RFC 0000 §3.1.3.2](0000-architecture.md#3132-shared-tables--outbox-and-processed_events) |
| S14 | Per-service Alembic history, explicit step, working `downgrade` on every migration | §3.1.1.12 |

**Decisions added by this RFC.** Each resolves a gap or a conflict between archive sources; the conflict is named in the decision.

- **PLT-1 — Workers are hosts, not owners.** Activity and task code lives in the owning service's package (`services/<svc>/app/workflows/`, `services/<svc>/app/tasks.py`) and calls that service's service layer, exactly as a router does (B1, the layering seam of [RFC 0000 §3.1.1.7](0000-architecture.md#3117-module-structure-inside-a-service)). A worker process imports that code and binds each activity or task to **its owning service's settings object and database role only**. An effect on *another* service's state is never a direct write: it is a call to that service's internal API with a service token ([archive/0002 §3.1.2](archive/0002-core-business-logic-and-workflows.md#312-api-changes) — the `/internal/v1/inventory/reservations*` routes). This reconciles B1 ("an activity calls one service function") with R1 ("a service reads and writes only its own database"): the function is called in-process when the workflow's owner owns the state, and through the API when it does not. The cost — one worker process holding more than one service's credentials — is stated in §3.3 and bounded by a static binding test (§7).
- **PLT-2 — One active relay loop per source database.** `SKIP LOCKED` (C2) prevents double-publishing across relay instances but does not preserve per-aggregate order across them: instance A can hold rows 1–100 while instance B publishes row 150 of the same aggregate first, breaking C3. Each relay instance therefore takes a session-level `pg_try_advisory_lock` per source database; the holder runs the claim loop, the others are hot standbys. `SKIP LOCKED` is retained as the safety net against double-publishing during a lock hand-over.
- **PLT-3 — The relay has its own role in every producer database,** `outbox_relay`, granted `SELECT`, `UPDATE (published_at)` and `DELETE` on `outbox` and nothing else. This is the platform's one bounded exception to R1, and it is what makes C1 possible without giving the relay a service's credentials.
- **PLT-4 — A poison outbox row holds its aggregate, not the table.** A row the relay cannot serialise is not skipped (that would reorder its aggregate, C3) and does not block other aggregates (C2). It and every later row with the same `aggregate_id` are held, counted, and surfaced (§3.1.1.2).
- **PLT-5 — The consumer base distinguishes a poison event from an unavailable dependency.** C6's three-attempts-then-DLQ applies to failures specific to the event. A handler that raises `DependencyUnavailable` (database down, embedding provider down) pauses the partition with backoff instead, so an outage does not dead-letter every event it touches. This reconciles C6 with [archive/0004 §6.1](archive/0004-retrieval-layer.md#61-scalability-and-fault-tolerance) ("embeddings unavailable → indexing retries with backoff and the event waits in Kafka").
- **PLT-6 — Trace context and actor are persisted with the outbox row.** C11 requires `traceparent` to travel with the event, and the envelope requires `actor_id`; the shared `outbox` DDL in [archive/0006](archive/0006-service-decomposition-and-data-model.md#outbox) carries neither, and the relay publishes long after the request's span has closed. **Proposed amendment to [RFC 0000 §3.1.3.2](0000-architecture.md#3132-shared-tables--outbox-and-processed_events):** `outbox.traceparent text NULL` and `outbox.actor_id uuid NULL`, and an optional `traceparent` envelope field (`["null","string"]`, default `null`, the same `FULL`-compatible shape as `client_id`). Until the amendment lands, the relay starts a new trace linked to `correlation_id` (which C12 makes the trace ID), so traces join by ID but lose parentage.
- **PLT-7 — Traces go through the collector; metrics are exposed at `/metrics` and scraped.** C10 routes all signals through the collector; archive/0003 T10 and the operational-endpoint convention put `/metrics` on every service with Prometheus scraping every target. Both are kept: application metrics are produced by the OpenTelemetry metrics SDK and exposed through its Prometheus reader at `/metrics`, so no service depends on a Prometheus client library and no service pushes to Prometheus; traces are exported over OTLP to the collector, which is the only thing that knows Jaeger exists.
- **PLT-8 — One metric vocabulary.** Names, units and labels follow §3.1.1.11; high-cardinality identifiers are never labels.
- **PLT-9 — JWKS first, mounted key as fallback.** [RFC 0000](0000-architecture.md#3114-authentication-and-authorisation-split) serves the public key as JWKS at `/api/v1/auth/jwks`, cached by every verifier; [archive/0001](archive/0001-foundation-and-core-services.md#331-challenges-and-open-questions) `0001-Q4` puts the public key in each service's environment. The verifier loads the mounted public key at start-up and refreshes from JWKS in the background, selecting by `kid`. A verifier therefore starts and serves authenticated traffic with `identity` down (R7), and key rotation needs no redeploy.
- **PLT-10 — Rate-limit counters are fixed windows whose TTL equals the window.** Archive/0003 lists `rl:{scope}:{identifier}` at 60 s; the assistant's 20 generations per hour needs 3600 s. The TTL is the window length of the rule that created the key.
- **PLT-11 — Token budgets have a Redis fast path and a Postgres record**, reconciled by Beat (key layout in §3.1.3.2; the budget rules are [0009](0009-assistant-service.md)'s).
- **PLT-12 — Bulk generation runs in a separate `celery-worker-ai`** that consumes only the `assistant` queue, so the LLM API key — the only real credential in the system — is present in `assistant`, `indexer-worker` and that worker, and not in the shared `celery-worker` that hosts every Part A task.
- **PLT-13 — Beat is a singleton.** Locally it runs embedded (`celery worker -B`) in the single `celery-worker` replica; scaling `celery-worker` beyond one replica moves Beat to its own container first.
- **PLT-14 — One Temporal namespace, two task queues** (§3.1.3.5, §3.1.2.5).
- **PLT-15 — Workflow determinism and versioning rules** are platform rules, stated once (§3.1.1.3).
- **PLT-16 — One private MinIO bucket with a fixed key layout** (§3.1.3.4).
- **PLT-17 — `processed_events` rows are retained 30 days in Postgres,** matching the Mongo variant's 30-day TTL. `processed_at` is indexed "for the retention sweep" in archive/0006, but no archive RFC names the sweep; it is `sweep_processed_events`, per consuming service, on Beat (§3.1.1.4).
- **PLT-18 — Published outbox rows are retained 7 days,** matching Kafka's default retention, then deleted by `sweep_published_outbox` under the `outbox_relay` role.
- **PLT-19 — The idempotency fast path is a read-through of completed replies only** (§3.1.1.1). Postgres remains the claim.
- **PLT-20 — `make up-all` starts every profile,** including `ai`. [archive/0003 §6.3](archive/0003-events-and-observability.md#63-deployment-strategy) says it "adds observability"; [archive/0000 §6.3](archive/0000-system-architecture.md#63-deployment-strategy) says "brings up everything", and archive/0004's definition of done runs `make reindex` straight after it, which needs `ai`.
- **PLT-21 — A fixed port map** (§3.1.1.12). Ports bound to the host are UIs and the gateway only.
- **PLT-22 — DLQ topics mirror their source topic's partitioning and keep 30 days,** longer than the source, because replay is a deliberate human act (`0003-Q8`).
- **PLT-23 — A consumer group is named after the consuming component,** and that name is the `consumer_group` value in `processed_events`.
- **PLT-24 — One JSON log line schema** (§3.1.1.11).
- **PLT-25 — Keys are generated, never committed** (`make keys`, §3.1.1.12).
- **PLT-26 — The event-type registry lives in `common`.** One table maps `event_type` → topic, schema file and `event_version`. The outbox DDL carries no `event_version`, so the version is a property of code shared by the writer and the relay, not of the row.
- **PLT-27 — A `common` change ships by rebuilding every image that consumes it,** together, with `make up`; modules that carry a cross-service contract change expand/contract (§3.1.1.1, "Change propagation and rollout").

##### 3.1.1.1. common

**Responsibility.** Shared, non-domain code that every service imports so a cross-cutting convention has one implementation: JWT verification and the claim model, the role and scope dependencies, configuration, structured logging, the RFC 9457 error handler, pagination, the async DB session, the `Idempotency-Key` mechanism, the event envelope and Avro serialisation, the outbox writer, the consumer base with `processed_events` dedupe, the Kafka producer wrapper, tracing and metrics setup, the Temporal client factory, the Celery app factory, the Redis helpers (cache, rate limiter), the service-token client, health checks, and the `DomainEnum` / `DomainEnumText` pair. It is a package (`smartretail_common`) installed into every image's shared base layer ([archive/0000 §6.3](archive/0000-system-architecture.md#63-deployment-strategy)).

**What it must not contain (R8).** Any business rule belonging to a single service — including any *vocabulary*. `DomainEnum` is the mechanism; the values (`draft`, `confirmed`, …) live in each service's `app/models/enums.py`. It answers "who is this", never "may they" ([RFC 0000 §3.1.1.4](0000-architecture.md#3114-authentication-and-authorisation-split)). It never imports from `services.*`. It holds no persistence model for any domain table: services share an *event* contract and an *API* contract, never a persistence model ([archive/0006 §5](archive/0006-service-decomposition-and-data-model.md#5-alternate-solutions), alternative 6).

**Module layout.**

```
common/smartretail_common/
├── auth.py            # JWT verify (RS256, JWKS + mounted key), Principal, current_principal,
│                      # optional_principal, require_roles, require_scope
├── service_token.py   # client-credentials token client for service→service calls, cached
├── config.py          # BaseServiceSettings (pydantic-settings); fail-fast validation
├── context.py         # request contextvars: trace_id/request_id, client_id, principal
├── logging.py         # JSON formatter, trace/span ids on every line, uvicorn log routing
├── errors.py          # Problem model, DomainError hierarchy, exception handlers
├── pagination.py      # limit/offset params (ceiling 100) + Page[T] envelope
├── db.py              # async engine + session dependency + unit-of-work helper
├── db_types.py        # DomainEnumText — maps a DomainEnum to TEXT (S4)
├── enums.py           # DomainEnum base + UnknownEnumValueError (S4)
├── idempotency.py     # Idempotency-Key dependency: canonical hash, Postgres claim, Redis fast path
├── outbox.py          # outbox writer helper — add(session, …) in the caller's transaction
├── events/
│   ├── registry.py    # event_type → topic, schema file, event_version (PLT-26)
│   ├── envelope.py    # Envelope model and builder
│   ├── serde.py       # Avro serialiser / deserialiser against Schema Registry
│   ├── producer.py    # confluent-kafka producer wrapper (idempotent, acks=all)
│   ├── consumer.py    # EventConsumer base: dedupe, retry, DLQ, pause, offset commit
│   ├── dedupe.py      # DedupeStore protocol + PostgresDedupeStore (processed_events)
│   └── schemas/       # *.avsc — one per topic, plus envelope.avsc (the shared fields)
├── tracing.py         # OTel SDK setup, OTLP exporter, auto-instrumentation, propagators
├── metrics.py         # OTel meter provider + Prometheus reader; standard instruments
├── temporal.py        # Temporal client factory with the tracing interceptor
├── celery_app.py      # Celery app factory, routing table, task base class, retry defaults
├── redis.py           # async client factory, Cache and RateLimiter with fail-open wrappers
└── health.py          # /health and /health/ready routers, pluggable readiness checks
```

**Authentication and the claim model.** Tokens are RS256 JWTs signed only by `identity` (A4); every service re-verifies (A5). Verification is local: signature, `exp`, `nbf`, `iss` and `aud` against the cached key set, with no network call on the request path. The claim model is:

| Field | From claim | Meaning |
| --- | --- | --- |
| `subject` | `sub` | A user id (`uuid`) for a user token; `service:<name>` for a service token (e.g. `service:temporal-worker`, [archive/0002 §3.1.2](archive/0002-core-business-logic-and-workflows.md#312-api-changes)) |
| `token_type` | derived | `user` or `service`, from the `sub` form |
| `role` | `role` | `customer` \| `merchant` \| `admin` on a user token; absent on a service token. The set is `identity`'s `UserRole` ([0003](0003-identity-service.md)); `common` treats it as an opaque string |
| `merchant_id` | `merchant_id` | Present on a merchant's token; the scope every merchant-owned query filters by. Never taken from a body or query parameter ([RFC 0000 §6.2](0000-architecture.md#62-security-considerations)) |
| `scopes` | `scope` | Space-separated scopes on a service token; empty on a user token |
| `expires_at` | `exp` | Access tokens live 15 minutes ([0003](0003-identity-service.md)) |
| `key_id` | header `kid` | Selects the verification key (PLT-9) |

`Principal` is immutable and is stored in the request context so the log formatter and the outbox writer can read `actor_id` without being passed it.

**The role and scope dependencies.**

| Dependency | Behaviour |
| --- | --- |
| `current_principal` | Requires a valid bearer token. Missing → `401`; malformed, bad signature, wrong key, expired → `401` with a problem detail; never distinguishes the cases in the body beyond `detail` |
| `optional_principal` | As above but returns `None` for an absent token — for public endpoints whose visibility depends on the caller (browse, anonymous chat). A *present but invalid* token is still `401` |
| `require_roles(*roles)` | `current_principal` plus a role check; wrong role → `403`. **Rejects service tokens** — user-facing routes reject service tokens ([archive/0002](archive/0002-core-business-logic-and-workflows.md#312-api-changes)) |
| `require_scope(scope)` | For internal routes. Requires a service token carrying `scope`; **rejects user tokens outright**, so a stolen customer token cannot call `/internal/v1/inventory/reservations` |

The role check is coarse. Ownership and merchant scoping are the owning service's decision, in its service layer, against `Principal.merchant_id` ([RFC 0000 §3.1.1.4](0000-architecture.md#3114-authentication-and-authorisation-split)).

**Service tokens.** `ServiceTokenClient` obtains a short-lived service JWT from `identity` by a client-credentials grant (endpoint defined in [0003](0003-identity-service.md)), caches it until 60 s before `exp`, and attaches it to outbound `httpx` calls made by workers. Scope names belong to the service that defines the internal route.

**Configuration.** `BaseServiceSettings` is a `pydantic-settings` model read from the environment only (twelve-factor). Every service subclasses it. Validation runs at import; a missing or malformed variable fails the container immediately and loudly rather than at the first request that needs it ([archive/0001 §6.3](archive/0001-foundation-and-core-services.md#63-deployment-strategy)). Shared variables:

| Variable | Default | Source / note |
| --- | --- | --- |
| `SERVICE_NAME` | — (required) | Also the OTel `service.name` and the log `service` field |
| `ENVIRONMENT` | `local` | |
| `LOG_LEVEL` | `INFO` | |
| `DATABASE_URL` | — (required for services with a store) | `postgresql+asyncpg://<svc>_role:…@postgres:5432/<svc>_db` — one per service (R1) |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` | `10` / `20` | [archive/0001 §6.1](archive/0001-foundation-and-core-services.md#61-scalability-and-fault-tolerance) — sized so a scaled-out service does not exhaust Postgres connections |
| `JWT_PUBLIC_KEY_PATH` | `/run/keys/jwt_public.pem` | Mounted from the gitignored `keys/` directory (`0001-Q4`, PLT-9) |
| `JWKS_URL` | `http://identity:8001/api/v1/auth/jwks` | PLT-9 |
| `JWKS_REFRESH_SECONDS` | `300` | PLT-9 |
| `JWT_ISSUER` / `JWT_AUDIENCE` | `smartretail-identity` / `smartretail` | PLT; values fixed by [0003](0003-identity-service.md) |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` | |
| `REDIS_URL` | `redis://redis:6379/0` | |
| `CELERY_BROKER_URL` | `amqp://smartretail:…@rabbitmq:5672/smartretail` | |
| `CELERY_RESULT_BACKEND` | `redis://redis:6379/1` | Redis as the results backend (C13) |
| `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` | `temporal:7233` / `smartretail` | PLT-14 |
| `MINIO_ENDPOINT` / `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` / `MINIO_MEDIA_BUCKET` | `http://minio:9000` / from `.env` / from `.env` / `smartretail-media` | PLT-16; credentials in `catalog` and `celery-worker` only |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4317` | C10 |
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | `0003-Q5`: 100% locally; turning it down is a variable |
| `OTEL_SDK_DISABLED` | `false` | Set `true` when the `observability` profile is off, so exporters do not retry into a missing collector |
| `CONSUMER_MAX_ATTEMPTS` | `3` | C6 |
| `CONSUMER_BACKOFF_INITIAL_MS` / `CONSUMER_BACKOFF_MAX_MS` | `500` / `8000` | PLT |

Component-specific settings are in each component's subsection below; service-specific ones are in each service RFC's §3.1.1.6.

**Structured logging.** JSON to stdout, one object per line, schema in §3.1.1.11 (PLT-24). A request-id contextvar is set by middleware from the inbound `X-Request-ID` (which the gateway has set to the trace ID, C12) and read by the formatter, so every line emitted while serving a request carries the same value without being passed it. Uvicorn's access and error loggers are routed through the same formatter.

**The RFC 9457 error handler (A7).** One `Problem` model and one set of exception handlers, installed by `install_error_handlers(app)`:

```json
{
  "type":       "https://smartretail.local/errors/product-not-found",
  "title":      "Product not found",
  "status":     404,
  "detail":     "…",
  "instance":   "/api/v1/products/…",
  "request_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "violations": [ { "field": "items[0].quantity", "message": "must be ≥ 1", "code": "greater_than_equal" } ]
}
```

| Raised | Status | Notes |
| --- | --- | --- |
| `RequestValidationError` (FastAPI/Pydantic) | `422` | `violations[]` populated from the Pydantic errors |
| `DomainError` subclasses (`NotFound`, `Conflict`, `Forbidden`, `Unprocessable`, `RateLimited`, `NotImplementedYet`) | `404` / `409` / `403` / `422` / `429` / `501` | Each carries a `type` slug and `title`. A service's own errors (e.g. `order`'s `IllegalTransition`, which maps to `409`) subclass these; the classes and their slugs live in the service, not here |
| `RateLimited` | `429` | Always sets `Retry-After` |
| Auth failures | `401` / `403` | From the dependencies above |
| `httpx.TimeoutException` on an upstream call | `504` | Used by the gateway's 10-second upstream timeout ([0002](0002-gateway-service.md)) |
| `DependencyUnavailable` | `503` | With `Retry-After` |
| Anything else | `500` | Generic `title`, no stack trace, no SQL, no exception text ([archive/0001 §6.2](archive/0001-foundation-and-core-services.md#62-security-considerations)); the exception is logged with the trace ID so the response's `request_id` finds it |

Content type is `application/problem+json`. The same `Problem` serialises into an SSE `error` frame for streaming endpoints ([0007](0007-notification-service.md), [0009](0009-assistant-service.md)).

**Pagination.** `PageParams` parses `limit` (default 20) and `offset` (default 0). `limit` above the ceiling of **100** is clamped, not rejected ([archive/0001 §7](archive/0001-foundation-and-core-services.md#7-test-plan)); negative values are `422`. The envelope is `{"items": [...], "total": N, "limit": L, "offset": O}` ([RFC 0000 §3.1.2.2](0000-architecture.md#3122-api-conventions)). Limit/offset is chosen over cursors because listings are filtered and sorted by arbitrary columns; a cursor variant is additive for a path that needs it (the notification stream's `since_seq` is such a path, owned by [0007](0007-notification-service.md)).

**The DB session.** `make_engine(settings)` builds one async engine per process (`asyncpg`, `pool_size=10`, `max_overflow=20`, `pool_pre_ping=True`). `get_session` is the FastAPI dependency; `unit_of_work(session)` wraps `session.begin()` so the state change, the audit row (S7) and the outbox row (B11) commit or roll back together. Sync drivers are not provided (A6). The engine is created lazily on first use, so a worker hosting several services' code (PLT-1) opens only the engines its bound tasks need.

**The `Idempotency-Key` mechanism.** A dependency, `idempotent(store=…)`, for every non-idempotent POST that creates business state ([RFC 0000 §3.1.2.2](0000-architecture.md#3122-api-conventions)). The table it claims into is the owning service's (`order_db.idempotency_keys`, defined in [0005](0005-order-service.md#idempotency_keys)); the mechanism is generic over a small store protocol so a later service reuses it.

```
request with Idempotency-Key: K, principal U, body B
  1. key shape check (1–255 visible ASCII; a UUID is recommended)      → 400 if malformed, 400 if absent where required
  2. h = sha256(canonical_json(B))                                     # sorted keys, no insignificant whitespace
  3. Redis fast path (PLT-19): GET idem:{U}:{K}
       hit and hit.request_hash == h → replay hit.status + hit.body verbatim, no DB access
       hit and hit.request_hash != h → 422 (same key, different body is a client bug)
       miss or Redis error          → continue (fail open)
  4. Postgres claim (B7): INSERT … (key, user_id, request_hash, state='in_progress', expires_at=now()+24h)
                          ON CONFLICT (user_id, key) DO NOTHING RETURNING …
       inserted       → run the handler; in the same transaction write order_id, response_status,
                        response_body, state='completed'; after commit SET idem:{U}:{K} {hash,status,body} EX 86400
       not inserted   → read the existing row:
                          request_hash != h         → 422
                          state = 'in_progress'     → 409 with Retry-After (an in-flight duplicate gets no second order)
                          state = 'completed'       → replay response_status + response_body verbatim
```

Keys are scoped per user, so one caller cannot probe or collide with another's ([archive/0002 §6.2](archive/0002-core-business-logic-and-workflows.md#62-security-considerations)). Postgres is the only arbiter; Redis only ever holds *completed* replies, so a lost or stale Redis entry can make a replay slower but never produce a second order. Rows expire after 24 hours and are removed by `sweep_idempotency_keys` (§3.1.1.4). The `IdempotencyState` vocabulary (`in_progress`, `completed`) is `order`'s ([0005](0005-order-service.md)); the store adapter supplied by the service maps to it.

**The event envelope builder.** `Envelope.build(event_type, payload, *, aggregate_id, causation_id=None)` fills `event_id` (uuid4), `event_version` and topic from the registry (PLT-26), `occurred_at`, `producer` (from `SERVICE_NAME`), and `correlation_id`, `actor_id`, `client_id` and `traceparent` from the request context. The envelope's fields are defined once in [RFC 0000 §3.1.2.4](0000-architecture.md#3124-event-envelope); its wire form is §3.1.2.2.

**The outbox writer helper (B11, A10).**

```python
await outbox.add(
    session,                         # the caller's session, inside its unit_of_work
    event_type="order.confirmed",    # must exist in the registry; unknown type → ValueError at write
    aggregate_type="order",
    aggregate_id=order.id,
    payload=OrderConfirmed(...).model_dump(mode="json"),
    causation_id=None,               # the event_id that caused this, when written by a consumer
)
```

It inserts one `outbox` row in the caller's transaction and never commits. The payload is validated against the topic's Avro schema **at write time**, so a payload the relay could not serialise is rejected inside the business transaction, where it can still be rolled back, rather than becoming a poison row (PLT-4). `correlation_id`, `client_id` (the `X-Client-Id` of the request, or `null` for a workflow, scheduler or webhook — [archive/0003](archive/0003-events-and-observability.md#311-architectural-changes) C21), `actor_id` and `traceparent` (PLT-6) come from the context. A workflow activity writes an outbox row the same way; since it has no request context, it passes `client_id` explicitly from persisted state (`orders.origin_client_id`, [0005](0005-order-service.md#orders)).

**The consumer base with `processed_events` dedupe (C4, C6, A11, S13).**

```python
class EventConsumer(ABC):
    group: ClassVar[str]              # PLT-23 — also processed_events.consumer_group
    topics: ClassVar[list[str]]
    dedupe: DedupeStore               # PostgresDedupeStore here; analytics supplies a Mongo store (0008)

    @abstractmethod
    async def handle(self, event: Envelope, tx: Transaction) -> None: ...   # the effect, in tx
    async def after_commit(self, event: Envelope) -> None: ...             # e.g. Redis PUBLISH, Celery enqueue
```

Processing one message:

```
poll → deserialise (Avro, Schema Registry)             ── failure: poison → DLQ immediately, commit offset
     → extract traceparent (Kafka header, else envelope field) → start CONSUMER span as its child
     → BEGIN
         INSERT INTO processed_events (consumer_group, event_id, event_type, processed_at)
           VALUES (:group, :event_id, :type, now()) ON CONFLICT DO NOTHING
         conflict → ROLLBACK, count outcome=duplicate, commit offset, done
         handle(event, tx)                               # the effect, same transaction
       COMMIT
     → after_commit(event)                               # side effects that must follow durability
     → commit offset (manual; enable.auto.commit=false)

handle raises:
  DependencyUnavailable → ROLLBACK; pause the partition; back off (0.5 s … 8 s); resume and retry;
                          no attempt counted, never dead-lettered (PLT-5)
  any other exception   → ROLLBACK; retry in-process up to CONSUMER_MAX_ATTEMPTS (3) with exponential backoff;
                          then produce to <topic>.dlq with the original key, value and headers plus
                          dlq.error.class, dlq.error.message, dlq.attempts, dlq.consumer_group, dlq.failed_at;
                          commit the offset and move on (C6)
```

The dedupe row and the effect are one transaction, which is why C4 and the dedupe table are a single design rather than two ([archive/0003 §3.1.1](archive/0003-events-and-observability.md#311-architectural-changes)). `after_commit` runs only after the transaction is durable, so a Redis fan-out or a Celery enqueue never announces an effect that rolled back; a crash between commit and `after_commit` loses only that side effect, which is why the side effects enqueued there must be re-derivable (the notification row is durable, [0007](0007-notification-service.md)). One consumer process per service subscribes to several topics, with dedupe keyed by consumer group (`0003-Q2`). Consumers retry broker connection with backoff rather than crash-looping ([archive/0003 §6.3](archive/0003-events-and-observability.md#63-deployment-strategy)).

**Tracing setup.** `setup_tracing(app, settings)` configures the OTel SDK with resource attributes (`service.name`, `service.version`, `deployment.environment`), the OTLP gRPC exporter to the collector with a batch span processor, W3C `tracecontext` and `baggage` propagators, the parent-based always-on sampler (`0003-Q5`), and auto-instrumentation for FastAPI, SQLAlchemy, `httpx`, `redis`, Celery and — via the Temporal interceptor — workflows and activities. Kafka producer and consumer spans are created by `events/producer.py` and `events/consumer.py`. Request and response bodies are never recorded as span attributes and headers are allow-listed ([archive/0003 §6.2](archive/0003-events-and-observability.md#62-security-considerations)). Propagation rules are in §3.1.1.11.

**Metrics setup.** `setup_metrics(app, settings)` installs the OTel meter provider with a Prometheus reader, mounts `/metrics`, and registers the standard instruments of §3.1.1.11 (HTTP server RED, HTTP client, DB pool, consumer, producer). Services add their own instruments through the same meter.

**`DomainEnum` and `DomainEnumText` (S4).** The rule — enums are `text`, never a Postgres `ENUM` or a `CHECK`; the permitted set is a `DomainEnum` in the owning service's `app/models/enums.py`; strict on write, tolerant on read — and its rationale are defined in [RFC 0000 §3.1.3.1](0000-architecture.md#3131-schema-conventions-s1s15). `common` supplies the mechanism only:

| Symbol | Contract |
| --- | --- |
| `DomainEnum(StrEnum)` | `values()`, `has(raw)`, `parse(raw, *, default=None)`. A member compares equal to its string and serialises to JSON and Avro with no encoder hook |
| `UnknownEnumValueError(ValueError)` | Names the enum class and the offending value |
| `DomainEnumText(enum_class, *, on_unknown="passthrough", fallback=None)` | A `TypeDecorator` over `Text`, `cache_ok = True`. **Write strict**: a non-member raises `UnknownEnumValueError` (surfacing as `sqlalchemy.exc.StatementError` with the error on `.orig`, because it runs inside the flush — a backstop, not the primary gate). **Read tolerant**: `passthrough` returns the raw string, `fallback` returns the configured member, `raise` raises |
| `render_item` hook | A helper each service's `alembic/env.py` passes to `context.configure(render_item=…, compare_type=True)`, so autogenerate renders `DomainEnumText` as `sa.Text()` rather than a constructor call missing its `enum_class` |
| `widen_for_response(enum_class)` | A Pydantic `BeforeValidator` that widens a response field to `Enum \| str` for `passthrough` columns, so a tolerant ORM read is not cancelled by a strict response schema. Request schemas stay strict |

**The R8 "no business rules" guard.** Two unit tests in `common/tests/`, run in every service's CI job:

1. **Vocabulary scan.** Discover every `DomainEnum` subclass in `services/*/app/models/enums.py`, collect the union of their values, and walk the AST of every module under `smartretail_common` for string constants in that set. Any hit fails. An allow-list is supported but empty; adding to it is a review decision. This is the grep [archive/0006 §7](archive/0006-service-decomposition-and-data-model.md#7-test-plan) requires, made robust to formatting.
2. **Import direction.** No module under `smartretail_common` imports `services.*` or any service package.

The remaining half of R8 — "does this helper encode a rule?" — is a review question at every design review, because no test can answer it.

**Configuration** (component). None beyond the table above.

**Failure behaviour.** A `common` defect is a platform-wide defect; the mitigation is that every service's CI runs the library's own tests, and that the library is versioned with the repository rather than published, so a change and its consumers are reviewed in one diff. Every helper that touches an optional dependency (Redis, the collector) fails open: a cache miss, an allowed request, a dropped span — never a failed request.

**Change propagation and rollout (PLT-27).** The library is never deployed on its own. Each service's Dockerfile copies `common` into its build and runs a regular (non-editable) `pip install`, so every image holds a **snapshot of the library taken at build time**. Editing `common` changes nothing in a running container; a service sees the change only when its image is rebuilt and its container recreated. The package version stays `0.1.0` and services depend on it unpinned (`smartretail-common`), because the repository *is* the version.

| Step | Effect of a `common` change |
| --- | --- |
| Edit `common/…` | None at runtime. Running containers keep the copy they were built with |
| `make dev-install` (local venv) | Installed editable (`pip install -e common`), so tests and a local `uvicorn` see the change immediately; a running `uvicorn` still needs a restart |
| `make test` | Runs the library's suite and then every service's suite — the change is tested against all its consumers in one run |
| `make lint` | mypy resolves `smartretail_common` from source (`mypy_path`), so a signature change surfaces in every consumer |
| `make up` (`compose up -d --build --wait`) | The `COPY common` layer is invalidated in every Dockerfile, so every consuming image rebuilds, and Compose recreates every container whose image changed. All services restart on the new library |
| `docker compose build <svc> && docker compose up -d <svc>` | Only `<svc>` gets the new library; every other service keeps the old one — a deliberate, temporary version skew |

**Version skew is normal, so contract-bearing code changes expand/contract.** A rebuild is not atomic — Compose recreates containers one at a time, and a single service can be rebuilt on its own — so for some window two services run different copies of the library. That is harmless for modules whose behaviour stays inside one process (`logging.py`, `pagination.py`, `config.py`, `db.py`, `health.py`). It is not harmless for modules that implement a contract *between* services, because one side may be on the old copy:

- `auth.py` — JWT verification and the claim model (`identity` mints, every other service verifies here)
- `errors.py` — the RFC 9457 problem-details shape that the gateway passes through and clients parse
- `context.py` — `X-Request-ID` propagation
- once they exist: the service-token client, the event envelope, the Avro serialiser and the event-type registry (PLT-26)

A change to one of these is made backward compatible first: the reading side accepts both old and new forms before any writing side emits the new one (for example, verifiers tolerate a new claim before `identity` requires it). Removals and renames follow in a separate change, once every service runs the expanded version. This is the library's application of the independent-deployability test in [RFC 0000 §6.3](0000-architecture.md#63-deployment-strategy): any single service can be rebuilt and restarted without touching another.

**Rollout procedure.**

1. Make the change; if it touches a contract-bearing module, make it additive.
2. `make test` and `make lint` — every consumer is exercised and type-checked.
3. `make up` — rebuild and recreate every consumer together; do not leave a partial rebuild running.
4. `make test-e2e` — smoke test through the gateway.
5. If an old form must be removed, do it in a follow-up change after step 3 has completed for every service.

**Observability.** The library *is* the observability implementation for every service; see §3.1.1.11.

##### 3.1.1.2. outbox-relay

**Responsibility.** Publish every committed `outbox` row, in every producing database, to its Kafka topic exactly once from the consumer's point of view, in per-aggregate order, and be the only writer of `published_at` (C1). It knows nothing about any domain: it reads rows the outbox writer has already validated and maps `event_type` to a topic through the registry (PLT-26).

**Source databases.** Derived from the event catalogue ([RFC 0000 §3.1.3.2](0000-architecture.md#3132-shared-tables--outbox-and-processed_events)): `identity_db`, `catalog_db`, `order_db`, `inventory_db`, `assistant_db`. Each is reached with the `outbox_relay` role (PLT-3). `producer` in the envelope is taken from the source database, never from the row.

**Claiming and publishing.** Per source database, while holding that database's advisory lock (PLT-2):

```sql
-- every OUTBOX_POLL_INTERVAL_MS (500 ms, 0003-Q1); immediately again if the batch was full
BEGIN;
SELECT id, event_id, event_type, aggregate_type, aggregate_id, payload,
       correlation_id, causation_id, client_id, actor_id, traceparent, created_at
  FROM outbox
 WHERE published_at IS NULL
 ORDER BY id
 LIMIT :batch                          -- OUTBOX_BATCH_SIZE, 100
   FOR UPDATE SKIP LOCKED;             -- C2
-- for each row, in id order, skipping aggregates currently held (PLT-4):
--   build envelope → Avro-serialise → produce(topic, key=aggregate_id, value, headers)
-- flush: wait for every delivery report
UPDATE outbox SET published_at = now() WHERE id = ANY(:acked_ids);
COMMIT;
```

The query is served by the partial index on `outbox (id) WHERE published_at IS NULL` defined with the table in [RFC 0000 §3.1.3.2](0000-architecture.md#3132-shared-tables--outbox-and-processed_events) — "the relay's only query". Because published rows leave the index, its size is the backlog, not the table, and the scan stays cheap however large the table grows between sweeps.

**Ordering (C3).** The Kafka key is `aggregate_id`, so every event of one aggregate lands on one partition. Within a batch rows are produced in `id` order by an idempotent producer (`enable.idempotence=true`, `acks=all`, `max.in.flight.requests.per.connection=5`), which Kafka guarantees not to reorder even across internal retries. Across relay instances, order is preserved by PLT-2. Global ordering is neither required nor achievable at any useful throughput (C3).

**At-least-once, and why it is safe.** If the relay crashes after producing but before `UPDATE … published_at`, the transaction rolls back, the rows are unlocked, and they are republished on restart — duplicate `event_id`s that the consumer dedupe absorbs (C4). If Kafka acknowledges only part of a batch, only the acknowledged ids are marked; the remainder is retried in the next cycle in `id` order. There is no path in which a committed row is marked published without an acknowledgement.

**Retry.** A broker or registry error rolls the batch back and retries with exponential backoff (0.5 s doubling to 30 s). The poll loop itself never exits on error; it reports unreadiness while the broker is unreachable, and outbox rows accumulate in Postgres and publish on recovery — checkout is unaffected ([archive/0003 §6.1](archive/0003-events-and-observability.md#61-scalability-and-fault-tolerance)).

**Poison rows (PLT-4).** A row that fails *serialisation* — its payload does not match the registered schema, its `event_type` is not in the registry — is not retryable by waiting. Because the outbox writer validates at write time, this should only happen after a bad deploy or a hand-edited row. The relay:

1. logs the row id, event type and error at `ERROR` with the row's `correlation_id`;
2. adds `aggregate_id` to an in-memory hold set, so this row and every later row of the same aggregate are skipped (order is preserved; other aggregates flow);
3. exports `outbox_poison_rows{database}` and `outbox_oldest_unpublished_age_seconds{database}`, which the event-pipeline dashboard shows (§3.1.1.11);
4. re-attempts held aggregates every 60 s, so a fixed schema or a corrected row releases the hold with no restart.

Whether the outbox should gain `publish_attempts` / `last_error` columns so the hold survives restarts and is visible in SQL is open (Q-PLT-2).

**Retention.** `sweep_published_outbox` (Beat, daily, §3.1.1.4) deletes rows with `published_at < now() - interval '7 days'` in batches of 5 000, per database, under the `outbox_relay` role (PLT-18). Unpublished rows are never swept.

**Configuration.**

| Variable | Default | Note |
| --- | --- | --- |
| `OUTBOX_SOURCES` | `identity_db,catalog_db,order_db,inventory_db,assistant_db` | Each with its DSN, `OUTBOX_DSN_<DB>`, using the `outbox_relay` role |
| `OUTBOX_POLL_INTERVAL_MS` | `500` | `0003-Q1`; adds at most one interval of publish latency |
| `OUTBOX_BATCH_SIZE` | `100` | |
| `OUTBOX_POISON_RETRY_SECONDS` | `60` | PLT-4 |
| `OUTBOX_BACKOFF_MAX_SECONDS` | `30` | |
| plus the shared Kafka, Schema Registry and OTel variables | | |

**Failure behaviour.**

| Failure | Behaviour |
| --- | --- |
| Relay down | Rows accumulate unpublished; producers are unaffected; published on restart. Backlog visible as `outbox_backlog_rows` |
| Kafka or Schema Registry down | Batches roll back and retry with backoff; `/health/ready` reports unready; no row is marked |
| One source database down | That source's loop backs off; other sources continue |
| Crash mid-batch | Rows unlocked by rollback, republished, deduped downstream (G1) |
| Poison row | Its aggregate is held; everything else flows (PLT-4) |
| Two relay instances | One holds each database's advisory lock; the other is a standby; `SKIP LOCKED` prevents a double publish during hand-over (C2, PLT-2) |

**Observability.** Spans: one `outbox.poll` span per cycle per database, and one `PRODUCER` span per row named `publish <topic>`, parented on the row's `traceparent` (PLT-6). Metrics: `outbox_backlog_rows{database}`, `outbox_oldest_unpublished_age_seconds{database}`, `outbox_published_total{database,topic}`, `outbox_publish_errors_total{database,reason}`, `outbox_poison_rows{database}`, `outbox_batch_duration_seconds{database}`, `outbox_relay_lock_held{database}`. Logs: one line per batch at `DEBUG`, one per error at `ERROR`, with `database` and `event_id` fields.

##### 3.1.1.3. temporal-worker

**Responsibility.** Host the workflow and activity code of the two durable processes [Part A](../part-a.md) specifies, connect them to the Temporal server, and give them tracing, metrics, retries and timeouts. The worker owns no state and no business rule (B1, B2, PLT-1).

**Registered workflows.**

| Workflow | Owner and logic | Task queue | Workflow id | Activities hosted |
| --- | --- | --- | --- | --- |
| `ProductPublishingWorkflow` | `catalog` — steps, compensation and publish-failure records in [RFC 0004 §3.1.1.4](0004-catalog-service.md#3114-workflows-and-async-jobs) | `catalog-publishing` | Owned by [0004](0004-catalog-service.md); recorded in `products.publish_workflow_id` | `validate_product`, `process_media` (enqueue-and-await the Celery task, C7), `build_search_document`, `sync_downstream`, `mark_published`, and the compensation |
| `OrderWorkflow` | `order` — saga steps, compensations, cancellation signal in [RFC 0005 §3.1.1.4](0005-order-service.md#3114-workflows-and-async-jobs) | `order-lifecycle` | `order-{order_id}` (`0002-Q3`: one workflow per order; a duplicate start is rejected by Temporal, a second line of defence behind the idempotency key) | `reserve_inventory`, `set_order_status`, `charge_payment`, `commit_inventory`, `create_shipment`, `refund_payment`, `release_inventory`, `notify` |

Inventory activities (`reserve_inventory`, `commit_inventory`, `release_inventory`, and the stock initialisation inside `sync_downstream`) call `inventory`'s internal routes with a service token whose `sub` is `service:temporal-worker` ([RFC 0006 §3.1.2.1](0006-inventory-service.md#3121-endpoints)); they never touch `inventory_db` (PLT-1). Activities owned by the workflow's own service call its service layer in-process with that service's role.

**Hosting.** One container, one Python process, two `Worker` instances — one per task queue — sharing a client. Separate task queues mean a burst of orders cannot starve publishing and vice versa, and each can later move to its own container by configuration. The worker is deployed separately from the services it calls, so workflow code can be redeployed without restarting an API ([archive/0002 §6.3](archive/0002-core-business-logic-and-workflows.md#63-deployment-strategy)). Services that *start* workflows (`catalog`, `order`) do so through the `common` Temporal client factory; they do not run workers.

**Default activity policy.** Owning RFCs override per activity; these are the defaults every activity inherits.

| Setting | Default | Source |
| --- | --- | --- |
| Retry | Exponential backoff, initial 1 s, coefficient 2, maximum interval 30 s, **maximum 3 attempts** on transient errors | "A network blip up to three" ([archive/0002 §6.1](archive/0002-core-business-logic-and-workflows.md#61-scalability-and-fault-tolerance)) |
| Business failure | Raised as `ApplicationError(non_retryable=True)` — a declined card is retried zero times | B10, [archive/0002 §6.1](archive/0002-core-business-logic-and-workflows.md#61-scalability-and-fault-tolerance) |
| `start_to_close_timeout` | 30 s | "Timeouts at every level" — per attempt |
| `schedule_to_close_timeout` | 2 min | Per attempt chain |
| Heartbeat | Required, 10 s, for any activity that waits (e.g. `process_media` awaiting Celery) | PLT |
| Workflow execution timeout | 1 h | So nothing hangs indefinitely; stranded stock is additionally reclaimed by the reservation sweeper (B5) |
| Idempotency | Every activity takes a deterministic token `sha256(workflow_id, activity_name, attempt-invariant input)` and passes it to the effect it performs (B3) | B3 |

**Determinism rules (PLT-15).** Workflow code is replayed from history, so it must be a pure function of its inputs and history.

1. No I/O in workflow code — no database, HTTP, Kafka, Redis, file or environment access. All of it is an activity.
2. No wall-clock time, randomness or UUIDs from the standard library: use `workflow.now()`, `workflow.random()`, `workflow.uuid4()`.
3. No threads, no `asyncio` primitives other than those the SDK provides; timers are `workflow.sleep()` / `asyncio.sleep` inside the sandbox.
4. Workflow inputs and state are identifiers and step results only (B2) — `order_id`, never the order. Temporal history is readable in its UI, so nothing sensitive goes into it ([archive/0002 §6.2](archive/0002-core-business-logic-and-workflows.md#62-security-considerations)).
5. The SDK sandbox stays enabled; imports of service modules into workflow files are pass-through only for pure data classes.
6. Workflow files import no business logic — only activity stubs and step orchestration (B1).

**Versioning rules.** A change to a workflow's command sequence is guarded with `workflow.patched("<change-id>")` so in-flight executions replay on the old branch; the patch is removed with `workflow.deprecate_patch()` only after no execution predating it remains open. Activity signatures are additive (new optional fields only). With no production traffic, the local path is drain-and-restart — stop starting new workflows, let running ones finish, redeploy ([archive/0002 §6.3](archive/0002-core-business-logic-and-workflows.md#63-deployment-strategy)); `make reset` is the answer to a broken local history. A replay test over recorded histories (§7) catches a non-deterministic change before it ships.

**Configuration.**

| Variable | Default | Note |
| --- | --- | --- |
| `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` | `temporal:7233` / `smartretail` | |
| `TEMPORAL_TASK_QUEUES` | `catalog-publishing,order-lifecycle` | One `Worker` per queue |
| `TEMPORAL_MAX_CONCURRENT_ACTIVITIES` | `50` per queue | |
| `TEMPORAL_MAX_CONCURRENT_WORKFLOW_TASKS` | `20` per queue | |
| `SERVICE_TOKEN_CLIENT_ID` / `SERVICE_TOKEN_CLIENT_SECRET` | from `.env` | For the client-credentials grant |
| `CATALOG_DATABASE_URL`, `ORDER_DATABASE_URL` | — | Bound only to that service's activities (PLT-1) |
| `INVENTORY_BASE_URL`, `CATALOG_BASE_URL` | `http://inventory:8004`, `http://catalog:8002` | Internal API calls |

**Failure behaviour.**

| Failure | Behaviour |
| --- | --- |
| Worker killed mid-workflow | Temporal persists every event; on restart the workflow resumes from the last completed activity. Tested by actually killing the worker (see [0005](0005-order-service.md)) |
| Worker saturated | The task queue buffers; workers scale horizontally with `--scale`; order creation stays fast because it only persists and starts a workflow (backpressure) |
| Temporal server down | Starting a workflow fails at the service, which returns a problem detail; running workflows pause and resume when it returns |
| A downstream service down | The activity fails and retries under its policy, then the workflow compensates as its owner specifies — e.g. `inventory` down fails the order cleanly with a recorded reason ([archive/0002 §6.1](archive/0002-core-business-logic-and-workflows.md#61-scalability-and-fault-tolerance)) |
| Non-deterministic deploy | Workflow tasks fail and retry without advancing; caught by the replay test before deploy |

**Observability.** The Temporal OpenTelemetry `TracingInterceptor` on the client and the worker produces `StartWorkflow`, `RunWorkflow`, `StartActivity` and `RunActivity` spans, with context carried in Temporal headers, so the trace that began at the HTTP request reaches every activity (archive/0003 T9). SDK metrics are exported through the Prometheus reader with the `temporal_` prefix (workflow completed/failed/timed out, activity execution latency and failures, schedule-to-start latency, sticky cache hits). Log lines carry `workflow_id`, `run_id` and `activity`. The Temporal UI is the per-execution view; `GET /api/v1/orders/{id}/workflow` ([0005](0005-order-service.md#3121-endpoints)) and `GET /api/v1/products/{id}/publish-status` ([0004](0004-catalog-service.md#3121-endpoints)) expose the same facts without a Temporal login.

##### 3.1.1.4. celery-worker

**Responsibility.** Run one-shot jobs off the request path — media derivatives, notification dispatch, scheduled sweeps and rollups, bulk generation — with per-task retry, over RabbitMQ (C7, A12). Celery is point-to-point with no retention and no fan-out; anything that is a *fact* goes through Kafka, anything that is a *process* goes through Temporal.

**Hosting.** Two Celery workers, both from the platform's Celery app factory:

| Worker | Queues | Profile | Credentials |
| --- | --- | --- | --- |
| `celery-worker` (+ Beat, embedded, PLT-13) | `media`, `notifications`, `analytics`, `maintenance` | `events` | Bound per task to the owning service's settings (PLT-1); MinIO for `media`; `outbox_relay` role for the outbox sweep |
| `celery-worker-ai` | `assistant` | `ai` | `assistant_db` and the LLM API key only (PLT-12) |

Separate queues so a burst of media processing cannot starve notification delivery ([archive/0003 §3.1.1](archive/0003-events-and-observability.md#311-architectural-changes)), and bulk generation has its own queue so a 200-product job cannot starve interactive chat ([archive/0005 §6.1](archive/0005-ai-assistant-and-streaming.md#61-scalability-and-fault-tolerance)).

**Task inventory and routing.** Each task's logic is specified by its owner; this table is the routing and scheduling contract.

| Task | Queue | Trigger | Owner (logic) |
| --- | --- | --- | --- |
| `process_media` | `media` | Enqueued and awaited by the publishing workflow's media activity — replaces the inline Temporal activity (`0002-Q7`) | [0004 catalog](0004-catalog-service.md#3114-workflows-and-async-jobs) |
| `dispatch_notification` | `notifications` | `notification` consumer, after persisting | [0007 notification](0007-notification-service.md#3114-workflows-and-async-jobs) |
| `promote_undelivered_notifications` | `notifications` | Beat, every 15 s — widens an unacknowledged `origin_first` notification to all devices | [0007](0007-notification-service.md#3114-workflows-and-async-jobs) |
| `sweep_stale_stream_cursors` | `maintenance` | Beat, daily — 90-day retention on `stream_cursors`, and the 20-rows-per-user cap | [0007](0007-notification-service.md#3114-workflows-and-async-jobs) |
| `rebuild_daily_rollups` | `analytics` | Beat, hourly | [0008 analytics](0008-analytics-service.md#3114-workflows-and-async-jobs) |
| `sweep_expired_reservations` | `maintenance` | Beat, every minute — replaces the bespoke sweeper loop (B5) | [0006 inventory](0006-inventory-service.md#3114-workflows-and-async-jobs) |
| `reconcile_inventory_ledger` | `maintenance` | Beat, daily | [0006](0006-inventory-service.md#3114-workflows-and-async-jobs) |
| `sweep_idempotency_keys` | `maintenance` | Beat, daily — 24-hour retention | [0005 order](0005-order-service.md#3114-workflows-and-async-jobs) |
| `sweep_published_outbox` | `maintenance` | Beat, daily — 7-day retention of published rows, per producer database (PLT-18) | Platform (§3.1.1.2) |
| `sweep_processed_events` | `maintenance` | Beat, daily — 30-day retention, per consuming Postgres database (PLT-17) | Platform mechanism; runs against each consumer's own table under its own role |
| `generate_bulk_content` | `assistant` | `POST /api/v1/assistant/generate/bulk` enqueues one job (E12) | [0009 assistant](0009-assistant-service.md#3114-workflows-and-async-jobs) |
| `reconcile_token_budgets` | `assistant` | Beat, every 5 min — Redis fast path → `token_budgets` (PLT-11) | [0009](0009-assistant-service.md#3114-workflows-and-async-jobs) |

Task names are fully qualified in code (`smartretail.<owner>.<task>`); the routing table in `celery_app.py` maps by name to queue, so a task cannot land on the wrong queue by a caller's mistake. Beat entries live in the same module so the schedule is reviewable in one diff. Any assistant maintenance that 0009 schedules (for example the 90-day `query_log` anonymisation, `0004-Q10`) joins the `assistant` queue on `celery-worker-ai`.

**Execution defaults.**

| Setting | Value | Why |
| --- | --- | --- |
| `task_acks_late` | `true` | A task is acknowledged after it finishes, so a killed worker's task is redelivered |
| `task_reject_on_worker_lost` | `true` | Same, for a hard kill |
| `worker_prefetch_multiplier` | `1` | Long media tasks do not hoard messages |
| Retry | `autoretry_for` transient errors, exponential backoff with jitter, `max_retries=3` unless the owner overrides | Tasks are re-runnable; every task must be idempotent |
| `task_time_limit` / `task_soft_time_limit` | 300 s / 270 s (`media`, `assistant`); 60 s / 50 s otherwise | |
| Serializer | JSON; arguments are identifiers only | Tasks carry ids, like workflows (B2) |
| Result backend | Redis (`celery-*` keys), results expire after 24 h | Only `process_media` and `generate_bulk_content` read results |
| Beat schedule overlap | Every Beat task is safe to run twice | "Beat sweeps are safe to run twice" ([archive/0003 §6.1](archive/0003-events-and-observability.md#61-scalability-and-fault-tolerance)) |

**Failure behaviour.**

| Failure | Behaviour |
| --- | --- |
| Task raises a transient error | Retried with backoff up to the limit; each attempt is visible in task metrics and, for dispatch, in `delivery_attempts` ([0007](0007-notification-service.md)) |
| Retries exhausted | The task is marked failed in the result backend and logged at `ERROR`; the owning service records the failure in its own domain record (a failed delivery attempt, a failed bulk item). There is no RabbitMQ dead-letter queue — the domain record is the audit |
| Worker killed mid-task | Redelivered (late ack); the task must be idempotent |
| RabbitMQ down | Enqueue fails at the producer. For `dispatch_notification` the notification row is already durable and the in-app channel is unaffected; for `process_media` the activity fails and Temporal retries it; Beat ticks missed during the outage are simply the next tick |
| Burst on one queue | Buffers in RabbitMQ per queue; other queues keep flowing (backpressure) |

**Observability.** Celery instrumentation creates a `PRODUCER` span on `apply_async` and a `CONSUMER` span for execution, carrying `traceparent` in the task message headers (§3.1.1.11). Metrics: `celery_task_duration_seconds{task,queue,outcome}`, `celery_task_retries_total{task}`, `celery_queue_depth{queue}` (from RabbitMQ's Prometheus plugin, §3.1.1.7), `celery_beat_last_run_timestamp{task}`. Flower is available on demand (`make flower`) for the "a background job didn't run" walkthrough (§7).

##### 3.1.1.5. indexer-worker runtime

**Responsibility.** Run `assistant`'s indexing consumer as its own container, so indexing load and failure are isolated from the `assistant` API (D3). The chunking, rendering, embedding, cache and upsert logic — and the backfill — are `assistant`'s, specified in [RFC 0009 §3.1.1.4](0009-assistant-service.md#3114-workflows-and-async-jobs). The code lives in `services/assistant/`; this container is that code on the shared consumer base, with `assistant_db` credentials and the embedding API key (D2: `assistant` owns `assistant_db`, including the vectors).

**Runtime.**

| Aspect | Specification |
| --- | --- |
| Consumer group | `assistant-indexer` (PLT-23); `processed_events` rows in `assistant_db` carry that group |
| Topics | `smartretail.catalog.product.published`, `smartretail.catalog.product.updated`, `smartretail.catalog.product.unpublished` |
| Concurrency | One process, one consumer, cooperative-sticky assignment. Horizontal scaling up to the partition count (3) by `--scale indexer-worker=N`; more replicas than partitions are idle standbys |
| Per-message work | One transaction per event, as the base class requires; embedding calls batched within an event (≤ 96 inputs per call, owned by 0009) |
| Retry and DLQ | The base class: 3 attempts then `<topic>.dlq` for a poison event; `DependencyUnavailable` for an embedding-provider outage pauses the partition and waits in Kafka (PLT-5) |
| Backfill | `make reindex` runs the same code as a one-shot command in the `indexer-worker` image, resumable and idempotent ([0009](0009-assistant-service.md)) |
| Profile | `ai` |

**Configuration.** `EMBEDDING_MODEL`, `EMBEDDING_DIMENSION` (validated at start-up against `index_state`; a mismatch logs a loud warning and marks the index stale, owned by 0009), the embedding provider key, `DATABASE_URL` for `assistant_db`, and the shared Kafka and OTel variables.

**Failure behaviour.** A failed index leaves the product published and keyword-searchable; it does not fail the publish, and the DLQ preserves the event for replay ([archive/0004 §6.1](archive/0004-retrieval-layer.md#61-scalability-and-fault-tolerance)). `indexer-worker` down: events wait in Kafka, lag grows, the index catches up on restart. Indexing lag is bounded by Kafka retention and visible as a metric.

**Observability.** The base class's consumer metrics with `consumer_group="assistant-indexer"`; indexing-specific metrics (lag, cache hit rate, tokens and cost) are named in [0009 §3.1.1.7](0009-assistant-service.md#3117-observability). The indexing trace links to the publishing trace through the event's `traceparent` ([archive/0004 §7](archive/0004-retrieval-layer.md#7-test-plan)).

##### 3.1.1.6. Kafka + Schema Registry

**Responsibility.** The event backbone: retain every fact for replay, fan it out to consumers the producer does not know about, and refuse any event whose schema would break an existing consumer (C5). Kafka is used because a new consumer must be able to read history — the indexer, any backfill, and analytics' `raw_events` escape hatch all depend on it — which RabbitMQ cannot provide (§5, Alternative 1).

**Deployment.** Apache Kafka in KRaft mode (no ZooKeeper), one broker, Confluent Platform 7.7 images (Kafka 3.8); Confluent Schema Registry 7.7.x, storing schemas in the `_schemas` topic. Profile `events`. Broker port 9092 is never published to the host ([archive/0003 §6.2](archive/0003-events-and-observability.md#62-security-considerations)).

**Topics.** Named `smartretail.<domain>.<event>` per the [event catalogue](0000-architecture.md#3123-event-catalogue); one schema per topic; one DLQ per topic. Full configuration is in §3.1.3.3. Topic creation and schema registration are one idempotent init script, `make topics` — not manual console commands, so a cold clone reproduces the pipeline exactly ([archive/0003 §6.3](archive/0003-events-and-observability.md#63-deployment-strategy)). New topics outside the catalogue require an amendment to RFC 0000 first.

**Partitioning.** 3 partitions per topic with one broker. That is meaningful: it forces the per-key ordering design to be real and multi-partition consumer behaviour to be tested, which a single partition would hide (`0003-Q9`). Consumers scale horizontally up to the partition count.

**Producer and consumer configuration** (set by `common`, not by services).

| Client | Setting | Value |
| --- | --- | --- |
| Producer | `enable.idempotence` / `acks` | `true` / `all` |
| Producer | `max.in.flight.requests.per.connection` | `5` (order-preserving with idempotence) |
| Producer | `linger.ms` / `compression.type` | `5` / `lz4` |
| Producer | key serialiser / value serialiser | UTF-8 string (`aggregate_id`) / Confluent Avro |
| Consumer | `enable.auto.commit` | `false` — offsets committed after the dedupe transaction |
| Consumer | `auto.offset.reset` | `earliest` — a new consumer group reads history |
| Consumer | `partition.assignment.strategy` | `cooperative-sticky` |
| Consumer | `max.poll.interval.ms` | `300000` |

**Compatibility.** The registry's global compatibility level is `BACKWARD`: a consumer on the new schema can read data written with the old one. Adding a field requires a default; removing one requires that consumers already tolerate its absence. The optional envelope fields (`client_id`, `traceparent`) are written as `["null","string"]` with `"default": null` — `null` first in the union for the default to be legal — which makes the change `FULL`, not merely `BACKWARD`: a v2 reader resolves a v1 record through the default and a v1 reader skips the unknown field ([archive/0003 §3.1.3](archive/0003-events-and-observability.md#313-dataschema-changes)). A breaking change is a new topic, never a new incompatible version. Registry versions are additive and never deleted.

**DLQ topics.** `<topic>.dlq`, same key, 3 partitions, 30-day retention (PLT-22). Headers carry `dlq.error.class`, `dlq.error.message`, `dlq.attempts`, `dlq.consumer_group` and `dlq.failed_at`, plus the original `traceparent`. There is no automatic replay loop, because automatic replay of a poison message is an infinite loop ([archive/0003 §3.3](archive/0003-events-and-observability.md#33-tradeoffs)). Replay is a CLI in `make` (`0003-Q8`): `make dlq-replay TOPIC=<topic> GROUP=<group> [FROM=<offset>] [EVENT_ID=<id>]` re-produces selected DLQ messages to the source topic under the `dlq-replay` consumer group, whose committed offset is also what "DLQ depth" is measured against. Because the source consumer dedupes by `event_id`, replaying an event that did eventually succeed is harmless.

**Consumer lag.** Every consumer computes its own lag per assigned partition (high watermark minus committed offset) and exports `kafka_consumer_lag{consumer_group,topic,partition}`; `/health/ready` reports it as a field (§3.1.2.4). There is no lag *alerting* — metrics and a dashboard panel only (`0003-Q7`). No separate lag exporter container is run.

**Configuration.** `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`; broker-side `KAFKA_NUM_PARTITIONS=3`, `KAFKA_DEFAULT_REPLICATION_FACTOR=1`, `KAFKA_LOG_RETENTION_HOURS=168`, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` (a typo'd topic name fails rather than silently creating a topic), `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL=backward`.

**Failure behaviour.**

| Failure | Behaviour |
| --- | --- |
| Broker down | Outbox rows accumulate in Postgres and publish on recovery; consumers retry connection with backoff; checkout is unaffected. A restart pauses the pipeline (single broker) |
| Registry down | The relay cannot serialise and backs off; consumers with the schema already cached keep consuming |
| A consumer down during publication | It catches up fully on restart from its committed offset |
| Consumer slow | Kafka retains and the consumer lags rather than dropping ([RFC 0000 §6.1](0000-architecture.md#61-scalability-and-fault-tolerance)) |

**Observability.** Producer spans from the relay, consumer spans from the base class, both following the OpenTelemetry messaging conventions (`messaging.system=kafka`, `messaging.destination.name`, `messaging.kafka.message.key`, `messaging.kafka.partition`). Lag, DLQ arrivals and DLQ depth on the event-pipeline dashboard. Debugging by eye uses `kafka-avro-console-consumer` inside the registry container ([archive/0003 §3.3](archive/0003-events-and-observability.md#33-tradeoffs)).

##### 3.1.1.7. RabbitMQ

**Responsibility.** Celery's broker — durable, per-queue buffering of one-shot tasks. It is not an event bus: messages are deleted once consumed, there is no replay and no schema governance, which is exactly why facts do not go through it (§5, Alternative 1).

**Deployment.** `rabbitmq:3.13-management`, profile `events`. One vhost, `smartretail`, one application user from `.env`. The Prometheus plugin (`rabbitmq_prometheus`, port 15692) is enabled so queue depth is scraped. The management UI (15672) is bound to localhost only.

**Queues.** `media`, `notifications`, `analytics`, `maintenance`, `assistant` — durable, declared by the Celery app factory at worker start, direct exchange `celery`, routing key equal to the queue name. Messages are persistent. Queue-length limits are not set: RabbitMQ buffering is the designed backpressure for this layer ([archive/0003 §6.1](archive/0003-events-and-observability.md#61-scalability-and-fault-tolerance)).

**Configuration.** `RABBITMQ_DEFAULT_USER`, `RABBITMQ_DEFAULT_PASS`, `RABBITMQ_DEFAULT_VHOST=smartretail` from `.env`; consumers use `CELERY_BROKER_URL`.

**Failure behaviour.** RabbitMQ down: enqueues fail at the caller (see the Celery table above); workers reconnect with backoff; persisted messages survive a broker restart.

**Observability.** `rabbitmq_queue_messages_ready{queue}` and `rabbitmq_queue_messages_unacked{queue}` from the plugin, presented as `celery_queue_depth` on the workers dashboard.

##### 3.1.1.8. Redis

**Responsibility.** Every piece of state in the platform that is fast, disposable, and reconstructible — and nothing that is not. Redis arrived with concrete jobs attached rather than speculatively, which was the condition set for introducing it (C13): gateway rate limiting, the idempotency-key fast path, and caching of the browse query; with Celery's results backend, notification fan-out, SSE stream tickets and presence, and the token-budget fast path following on existing paths. Every keyspace, with its TTL, is in §3.1.3.2.

**Deployment.** `redis:7.4`, single node, profile `events`, no persistence required (`appendonly no`), `maxmemory 256mb` with `maxmemory-policy volatile-lru` so only keys with a TTL are ever evicted. Database 0 for application keys, database 1 for Celery results. No auth on the internal network (accepted gap, §6.2). Client: `redis[hiredis]` 5.x async.

**The rule that makes Redis safe: it is never the source of truth.**

| Use | Source of truth when Redis is gone |
| --- | --- |
| Rate limits | None needed — limits fail **open** ([archive/0003 §6.1](archive/0003-events-and-observability.md#61-scalability-and-fault-tolerance)) |
| Idempotency fast path | `idempotency_keys` in `order_db` (PLT-19) |
| Browse and inventory cache | The owning service's database; a Redis outage degrades to uncached reads and the client never sees an error |
| Notification fan-out (Pub/Sub) | The `notifications` row, already durable before `PUBLISH`; a dropped message is recovered by `seq` catch-up on reconnect (C16–C18, [0007](0007-notification-service.md)) |
| SSE stream tickets | None — a lost ticket means the client requests another; streams cannot open while Redis is down |
| Connection count and presence | Reconstructible; both expire on their own and fail safe ([0007](0007-notification-service.md)) |
| Token budgets | `token_budgets` in `assistant_db`, reconciled by Beat (PLT-11) |
| Celery results | Only needed while an awaiting caller is polling; the Temporal activity retries on loss |

**Rate-limiter mechanism.** `RateLimiter.hit(scope, identifier, limit, window_s)` is a fixed-window counter: `INCR rl:{scope}:{identifier}:{window_start}` then `EXPIRE … window_s` on first increment (PLT-10). Over the limit returns `429` with `Retry-After` set to the seconds until the window rolls. A Redis error returns *allowed* and increments `rate_limit_decisions_total{outcome="fail_open"}`. The rules that use it — 5 login attempts per minute per IP and per email, 30 orders per minute per user, 100 requests per minute per IP by default, 20 searches per minute per IP and 60 per authenticated user, 10 assistant messages per minute authenticated and 3 anonymous, 20 generations per hour per merchant, 12 notification acks per minute per `(user, client)`, and the exemption of `/notifications/stream` from the per-IP limit — are policy owned by [0002](0002-gateway-service.md) and the owning services.

**Cache mechanism.** `Cache.get_or_set(key, ttl, loader)` with JSON values and fail-open on any Redis error; `Cache.invalidate(prefix)` uses `SCAN` + `UNLINK` over a prefix. Invalidation is triggered by the owning service's consumer (for example `catalog` on `product.updated`), not by the platform.

**Pub/Sub mechanism.** `publish(channel, message)` and an async subscriber that multiplexes dynamic `SUBSCRIBE`/`UNSUBSCRIBE` over one connection per replica. At-most-once, which is sufficient because the row is already durable (C16, C17). One channel per connected user rather than sharded channels (`0003-Q11`, revisited only if `SUBSCRIBE` churn shows up in Redis latency).

**Failure behaviour.** A Redis restart drops in-flight Pub/Sub frames (clients recover on reconnect rather than instantly), empties caches, resets rate windows, and empties budget counters until reconciliation re-seeds them from Postgres. Nothing is lost.

**Observability.** `cache_requests_total{cache,outcome=hit|miss|error}`, `rate_limit_decisions_total{scope,outcome=allowed|limited|fail_open}`, `redis_command_duration_seconds{command}` from the instrumentation, and `redis_pubsub_messages_total{outcome}`.

##### 3.1.1.9. MinIO

**Responsibility.** S3-compatible object storage for product media binaries — originals and derived sizes. `catalog` owns the media *metadata* (`product_media`, [0004](0004-catalog-service.md#product_media)); MinIO holds the bytes, keyed by `product_media.storage_key` and the `derived` map. `media` is not a service ([RFC 0000 §3.1.1.2](0000-architecture.md#3112-the-ownership-test)): it is object storage plus a Celery task.

**Deployment.** `minio/minio` (latest), profile `workflow`, API on 9000 and console on 9001 (localhost only). A one-shot `minio-init` container (`minio/mc`) creates the bucket idempotently at start-up with versioning off and a private-by-default policy ([archive/0002 §6.3](archive/0002-core-business-logic-and-workflows.md#63-deployment-strategy)). Credentials come from `.env` (accepted gap: default local credentials). Access is via `boto3` 1.35.x.

**Access.** `catalog` writes originals on upload (multipart through `catalog`, so validation happens before anything is stored — `0002-Q6`; presigned direct-to-MinIO upload is the scalable answer, deferred to [0004](0004-catalog-service.md)). `celery-worker` reads originals and writes derivatives in `process_media`. Nothing else holds MinIO credentials. Because the bucket is private, reads are served through `catalog` or by short-lived presigned GET URLs that `catalog` issues ([0004](0004-catalog-service.md)). Layout in §3.1.3.4.

**Failure behaviour.** MinIO down: uploads fail at `catalog` with a problem detail; `process_media` fails and is retried by the publishing workflow's policy, which then compensates as [0004](0004-catalog-service.md) specifies — the product stays in `draft` with a recorded failure, never half-published.

**Observability.** MinIO's own Prometheus endpoint (`/minio/v2/metrics/cluster`) is scraped; `boto3` calls appear as client spans under the task or request span.

##### 3.1.1.10. Temporal server

**Responsibility.** Durable execution for the two sagas: persisted workflow history, task queues, timers, retries, signals (order cancellation) and a UI for per-execution diagnosis. Workflow execution state is owned by Temporal and referenced from `order` and `catalog` by `workflow_id` only ([RFC 0000 §3.1.3.6](0000-architecture.md#3136-ownership-map)).

**Deployment.** Temporal server 1.25.x (`temporalio/auto-setup` locally) and `temporalio/ui`, profile `workflow`; gRPC on 7233, UI on 8080 (localhost). Persistence is PostgreSQL: the `temporal` and `temporal_visibility` databases in the shared instance, created by the Postgres init script, with the Temporal schema set up by the auto-setup entrypoint before the server starts ([archive/0002 §4](archive/0002-core-business-logic-and-workflows.md#4-dependenciesblockersknown-issues), [§6.3](archive/0002-core-business-logic-and-workflows.md#63-deployment-strategy)). Namespace registration is part of the same init (§3.1.3.5). Temporal connects with its own role, granted on its two databases only.

**Configuration.** `DB=postgres12`, `DB_PORT=5432`, `POSTGRES_USER`/`POSTGRES_PWD` for the `temporal` role, `POSTGRES_SEEDS=postgres`, `DEFAULT_NAMESPACE=smartretail`, `DEFAULT_NAMESPACE_RETENTION=7d`; UI: `TEMPORAL_ADDRESS=temporal:7233`.

**Failure behaviour.** Server down: `catalog` and `order` cannot start workflows and return a problem detail (publish and checkout fail cleanly rather than hang); running workflows pause and resume from history when it returns. Single-node Temporal is a known ceiling, not a design constraint.

**Observability.** Server metrics on its Prometheus endpoint (9090 inside its container) are scraped; SDK metrics come from the worker; spans from the interceptors. The Temporal UI shows completed and compensated executions — part of the diagnosis walkthrough for "a product never published" (§7).

##### 3.1.1.11. Observability stack

**Responsibility.** Make every failure in [Part A](../part-a.md) §6's five named flows — publishing, checkout, inventory sync, workers, notifications — diagnosable from a symptom to a cause, with three signals: traces in Jaeger, metrics in Prometheus rendered by Grafana, and JSON logs on stdout correlated by trace ID. OpenTelemetry is the only instrumentation API in application code, so backends are configuration (C10, §5 Alternative 6).

**Pipeline.**

```
 service / worker ──OTLP gRPC :4317──▶ otel-collector ──OTLP──▶ jaeger (all-in-one, in-memory)
        │                                   │
        │                                   └─ /metrics :8888 (collector self-metrics)
        │
        └──/metrics (OTel SDK Prometheus reader)◀──scrape 15 s── prometheus ──▶ grafana
```

**OTel Collector** (`otel/opentelemetry-collector-contrib` 0.112.x, config in `ops/otel-collector/config.yaml`):

```yaml
receivers:
  otlp: { protocols: { grpc: { endpoint: 0.0.0.0:4317 }, http: { endpoint: 0.0.0.0:4318 } } }
processors:
  memory_limiter: { check_interval: 1s, limit_mib: 256 }
  batch: {}
  attributes/scrub:              # defence in depth; the SDK already allow-lists headers
    actions:
      - { key: http.request.header.authorization, action: delete }
      - { key: http.request.header.cookie,        action: delete }
exporters:
  otlp/jaeger: { endpoint: jaeger:4317, tls: { insecure: true } }
service:
  pipelines:
    traces: { receivers: [otlp], processors: [memory_limiter, attributes/scrub, batch], exporters: [otlp/jaeger] }
  telemetry: { metrics: { address: 0.0.0.0:8888 } }
```

A metrics pipeline is deliberately absent (PLT-7): metrics are pulled from `/metrics`, so a missing collector never loses a metric and Prometheus's target health is the "is it up" signal for every component. If a backend ever needs pushed metrics, adding an `otlp` metrics pipeline with a `prometheusremotewrite` exporter is collector configuration only.

**Jaeger** 1.62 all-in-one, in-memory storage (a restart loses history — accepted), OTLP receiver enabled, UI on 16686 (localhost). The G5 assertion queries Jaeger's HTTP API for a trace ID and checks the set of services, rather than a person looking at the UI.

**Prometheus** 2.55, config in `ops/prometheus/prometheus.yml`, local short retention (`--storage.tsdb.retention.time=2d`), exemplar storage enabled (`--enable-feature=exemplar-storage`) so a latency panel links to a trace.

```yaml
global: { scrape_interval: 15s, evaluation_interval: 15s }
scrape_configs:
  - job_name: services
    metrics_path: /metrics
    static_configs:
      - targets: [gateway:8000, identity:8001, catalog:8002, order:8003, inventory:8004,
                  notification:8005, analytics:8006, assistant:8007]
  - job_name: workers                 # ops port 8090 on each worker (§3.1.2.4)
    static_configs:
      - targets: [outbox-relay:8090, temporal-worker:8090, celery-worker:8090,
                  celery-worker-ai:8090, indexer-worker:8090]
  - job_name: infrastructure
    static_configs:
      - targets: [otel-collector:8888, rabbitmq:15692, temporal:9090]   # temporal: PROMETHEUS_ENDPOINT=0.0.0.0:9090
  - job_name: minio
    metrics_path: /minio/v2/metrics/cluster
    static_configs:
      - targets: [minio:9000]
```

Targets for components whose profile is not running are simply down; the platform overview dashboard shows `up` per target so "not running" and "not healthy" are distinguishable.

**Grafana** 11.x, datasources (Prometheus, Jaeger) and dashboards provisioned as code from `ops/grafana/`, so they survive `make reset` and are reviewable in a diff. Admin password from `.env`, anonymous access disabled.

| Dashboard | Panels | Owner of the panel content |
| --- | --- | --- |
| Platform overview | `up` per target, request rate and error rate per service, p95 latency per service, outbox backlog, total consumer lag, DLQ arrivals, Celery queue depth | Platform |
| Service RED | Templated by `service`: rate, errors, duration (p50/p95/p99) per route, with exemplars; DB pool usage; outbound call latency by peer | Platform |
| Event pipeline health | Outbox backlog and oldest-unpublished age per database, relay publish rate, poison rows, lock holder; consumer lag per group/topic/partition; dedupe hits; retries; DLQ arrivals and depth | Platform |
| Workers | Celery queue depth, task rate/duration/failures/retries per task, Beat last-run per task; Temporal workflow completed/failed/timed-out, activity failures, schedule-to-start latency | Platform |
| Business metrics | The nine [Part A](../part-a.md) §5 metrics and the four [Part B](../part-b.md) assistant metrics | [0008](0008-analytics-service.md#3117-observability) |
| Notification delivery | Active SSE connections, fan-out latency, reconnects, catch-up rows, scoped-delivery set | [0007](0007-notification-service.md#3117-observability) |
| Retrieval | Indexing lag, embedding cache hit rate, tokens and cost, search latency by mode, zero-result rate | [0009](0009-assistant-service.md#3117-observability) |
| Assistant | Time-to-first-token, total latency, tokens, cost, refusal rate, error rate by cause | [0009](0009-assistant-service.md#3117-observability) |

Dashboards provide no alerting (§2.2).

**Metric and label conventions (PLT-8).**

| Rule | Detail |
| --- | --- |
| Naming | `snake_case`, `<domain>_<thing>_<unit>`; units as suffixes (`_seconds`, `_bytes`); counters end `_total`; OTel instrument names follow semantic conventions and are translated by the Prometheus reader (`http.server.request.duration` → `http_server_request_duration_seconds`) |
| Units | Durations in seconds, sizes in bytes, money as a labelled `currency` with the amount in the value only where a metric must carry money (business metrics, owned by 0008) |
| Required label | `service` (from the OTel resource `service.name`) on every series; `job` and `instance` from Prometheus |
| Allowed labels | Bounded vocabularies only: `method`, `route` (the templated path, never the raw URL), `status_code`, `outcome`, `topic`, `consumer_group`, `partition`, `queue`, `task`, `database`, `peer_service`, `reason` (from a fixed set), `mode`, `scope` (a delivery or rate-limit scope *kind*, never an id) |
| Forbidden labels | `user_id`, `merchant_id`, `customer_id`, `order_id`, `product_id`, `variant_id`, `event_id`, `client_id`, `trace_id`, email, IP — anything unbounded. A link from a metric to a request is an **exemplar**, not a label |
| Histograms | Latency buckets `0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10` s by default; streaming endpoints add `30, 60, 120` |

**Standard instruments** registered by `common` in every component that has the relevant code path:

| Metric | Type | Labels |
| --- | --- | --- |
| `http_server_request_duration_seconds` | histogram (RED: `_count` rate, 5xx share, buckets) | `service`, `method`, `route`, `status_code` |
| `http_server_active_requests` | gauge | `service`, `method`, `route` |
| `http_client_request_duration_seconds` | histogram | `service`, `peer_service`, `method`, `status_code` |
| `db_client_connections_usage` | gauge | `service`, `state=idle\|used` |
| `consumer_events_total` | counter | `consumer_group`, `topic`, `outcome=processed\|duplicate\|retried\|dead_lettered\|paused` |
| `consumer_handler_duration_seconds` | histogram | `consumer_group`, `topic` |
| `kafka_consumer_lag` | gauge | `consumer_group`, `topic`, `partition` |
| `dlq_events_total` | counter | `topic`, `consumer_group` |
| `dlq_depth` | gauge (per DLQ, high watermark − `dlq-replay` offset) | `topic` |
| `outbox_*` | see §3.1.1.2 | `database`, `topic`, `reason` |
| `celery_*` | see §3.1.1.4 | `task`, `queue`, `outcome` |
| `cache_requests_total`, `rate_limit_decisions_total` | counter | see §3.1.1.8 |

Service-specific metrics — including the notification scoped-delivery set (`notification_frames_total{scope,outcome}`, `notification_ack_lag_seconds`, `notification_catchup_rows`, `notification_catchup_truncated_total`, `notification_cold_start_total`, `notification_promoted_total`, `notification_undelivered_age_seconds`), the retrieval set and the assistant set — are defined in each owning RFC's §3.1.1.7 and must follow these conventions.

**Trace propagation.**

| Hop | Mechanism |
| --- | --- |
| Client → gateway → service (HTTP) | W3C `traceparent` / `tracestate` headers, injected and extracted by the FastAPI and `httpx` instrumentations. The gateway starts the root span when none arrives and sets `X-Request-ID` to the trace ID (C12); every response carries `traceparent` and `X-Request-ID` ([archive/0003 §3.1.2](archive/0003-events-and-observability.md#312-api-changes)). A client-supplied `X-Request-ID` is recorded as a span attribute, not adopted |
| Service → outbox → relay → Kafka | The outbox writer stores the current span context as `outbox.traceparent` (PLT-6). The relay's `publish <topic>` span is a child of it and injects its own context into the Kafka `traceparent` header and the envelope field |
| Kafka → consumer | The base class extracts the header (falling back to the envelope field) and starts a `CONSUMER` span as its child (C11). A trace therefore does not end at the producer |
| Service → Temporal → activity | The Temporal `TracingInterceptor` on the client and worker carries context in Temporal headers; `RunWorkflow` and `RunActivity` spans are children of the span that started the workflow (archive/0003 T9) |
| Activity or consumer → Celery → task | Celery instrumentation injects `traceparent` into task headers on `apply_async` and extracts it on execution |
| Consumer → Redis Pub/Sub → SSE frame | Not propagated as a parent: the fan-out is an at-most-once hint (C16). The publishing replica records the publish time in the message so [0007](0007-notification-service.md) can measure publish→frame latency |
| Streaming responses (SSE) | The server span stays open until the stream ends, never closing at first byte ([archive/0003 §8](archive/0003-events-and-observability.md#8-impact-analysis), [archive/0005 §3.3](archive/0005-ai-assistant-and-streaming.md#33-tradeoffs)) |

The result is G5: one checkout produces one trace containing spans from `gateway`, `order`, `inventory`, the Temporal workflow and its activities, `outbox-relay`, and the `notification` consumer.

**Log format (PLT-24).** One JSON object per line on stdout:

| Field | Always | Content |
| --- | --- | --- |
| `ts` | yes | RFC 3339, UTC, millisecond precision |
| `level` | yes | `DEBUG` … `CRITICAL` |
| `service` | yes | `SERVICE_NAME` |
| `logger` | yes | Python logger name |
| `msg` | yes | The message |
| `trace_id`, `span_id` | when a span is active | Hex, as in `traceparent` |
| `request_id` | when serving a request | Equal to `trace_id` (C12) |
| `user_id` | when a principal is present | The token's `sub` — never an email |
| `client_id` | when `X-Client-Id` was present | Opaque device token |
| `event_id`, `event_type`, `consumer_group` | in consumers and the relay | |
| `workflow_id`, `run_id`, `activity` | in the Temporal worker | |
| `task`, `task_id`, `queue` | in Celery | |
| `exc_type`, `exc_message`, `stack` | on exceptions | Never returned to a client |

Never logged: passwords, access or refresh tokens, stream tickets, API keys, request or response bodies, full addresses. Correlation across `docker compose logs` is by `trace_id`; Loki is not run (§5, Alternative 8).

**Configuration.** `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_TRACES_SAMPLER`, `OTEL_SDK_DISABLED` (§3.1.1.1); `GF_SECURITY_ADMIN_PASSWORD`, `GF_AUTH_ANONYMOUS_ENABLED=false`; Prometheus and collector configuration files under `ops/`.

**Failure behaviour.** The observability profile off: exporters are disabled by `OTEL_SDK_DISABLED=true`, `/metrics` still serves, logs still flow — nothing in the request path depends on the stack. Collector down while the SDK is enabled: the batch processor drops spans after its queue fills; requests are unaffected. Jaeger restart: trace history lost (in-memory).

##### 3.1.1.12. Local environment — Compose, Makefile, Dockerfiles

**Responsibility.** Let one developer run, on one laptop, exactly the subset of the platform they are working on, and let a reviewer reproduce the whole of it from a cold clone.

**Repository layout.**

```
SmartRetail/
├── docker-compose.yml           # every container, grouped by profile
├── Makefile                     # the single entry point (targets below)
├── .env.example                 # every variable, with safe local defaults; .env is gitignored
├── keys/                        # gitignored — RS256 key pair from `make keys`
├── README.md                    # overview, architecture, setup, API tour, role matrix,
│                                # the diagnosis walkthroughs, AI section and cost model
├── pyproject.toml               # workspace; ruff, mypy, pytest config
├── docs/                        # specification documents and RFCs
├── common/                      # smartretail_common (§3.1.1.1) + its tests
├── services/
│   ├── gateway/app/…            # no alembic — no store
│   ├── identity/{app,alembic,tests}/
│   ├── catalog/{app,alembic,tests}/
│   ├── order/{app,alembic,tests}/
│   ├── inventory/{app,alembic,tests}/
│   ├── notification/{app,alembic,tests}/
│   ├── analytics/{app,tests}/   # MongoDB; indexes created by an idempotent init command
│   └── assistant/{app,alembic,tests}/   # also the indexer-worker entrypoint
├── workers/
│   ├── outbox_relay/            # the relay (platform code, no domain imports)
│   ├── temporal_worker/         # worker bootstrap; imports services/*/app/workflows
│   └── celery_worker/           # worker bootstrap; imports services/*/app/tasks
├── ops/
│   ├── postgres/init/           # databases, roles, grants, extensions
│   ├── kafka/                   # topic definitions consumed by `make topics`
│   ├── otel-collector/config.yaml
│   ├── prometheus/prometheus.yml
│   ├── grafana/{datasources,dashboards}/
│   └── prompts/                 # prompt templates seeded into assistant_db (0009)
├── scripts/                     # topics.py, dlq_replay.py, seed.py, keys.sh
└── tests/e2e/                   # through-the-gateway end-to-end suites
```

Inside each service the layering is the one fixed in [RFC 0000 §3.1.1.7](0000-architecture.md#3117-module-structure-inside-a-service): a root `main.py`, then `app/api/` (`router.py` and `handlers/`, HTTP only), `services/` (`*_repo.py`, the only layer permitted business rules), `models/` (ORM and Pydantic contract, `enums.py`), `configs/`, `infrastructure/`, `exception_handlers/`, `utils/` (state machines, helpers), `consumers/`, `workflows/`, `tasks/`.

**Compose profiles.**

| Profile | Containers |
| --- | --- |
| `core` | `postgres` (image `pgvector/pgvector:pg16` — a drop-in for `postgres:16`; the extension is created by migration in `assistant_db` only), `gateway`, `identity`, `catalog` |
| `workflow` | `temporal`, `temporal-ui`, `minio`, `minio-init`, `temporal-worker`, `order`, `inventory` |
| `events` | `kafka`, `schema-registry`, `rabbitmq`, `redis`, `mongo`, `outbox-relay`, `notification`, `analytics`, `celery-worker` (with embedded Beat) |
| `observability` | `otel-collector`, `jaeger`, `prometheus`, `grafana` — off by default (C14, `0000-Q9`) |
| `ai` | `assistant`, `indexer-worker`, `celery-worker-ai` |

Startup ordering is health-gated `depends_on: condition: service_healthy` — Postgres before every service, Kafka and the registry before the relay and consumers, Temporal before the worker ([archive/0003 §6.3](archive/0003-events-and-observability.md#63-deployment-strategy)). Services in a later profile that another profile's container calls are reached by DNS and degrade when absent (for example `order` without `events` still writes outbox rows).

**Port map (PLT-21).** Host-bound: `gateway` 8000, `temporal-ui` 8080, MinIO console 9001, RabbitMQ management 15672, Jaeger 16686, Prometheus 9090, Grafana 3000 — all bound to `127.0.0.1`. Internal only: services 8001–8007, Postgres 5432, Kafka 9092, Schema Registry 8081, Temporal 7233, MinIO 9000, RabbitMQ 5672, Redis 6379, MongoDB 27017, collector 4317/4318, worker ops port 8090. Broker ports are never published ([archive/0003 §6.2](archive/0003-events-and-observability.md#62-security-considerations)).

**Container count** — worth stating, because the laptop is a real constraint ([archive/0000 §3.1.5](archive/0000-system-architecture.md#315-action-plan)):

| Stack | App containers | Worker containers | Infrastructure containers (incl. one-shot init) |
| --- | --- | --- | --- |
| `core` | 3 | 0 | 1 |
| + `workflow` | 5 | 1 | 4 (`temporal`, `temporal-ui`, `minio`, `minio-init`) |
| + `events` | 7 | 3 (`temporal-worker`, `outbox-relay`, `celery-worker`) | 9 (+ `kafka`, `schema-registry`, `rabbitmq`, `redis`, `mongo`) |
| + `observability` | 7 | 3 | 13 |
| + `ai` (everything) | 8 | 5 (+ `indexer-worker`, `celery-worker-ai`) | 13 |

Expect roughly 8 GB for the full stack; running everything plus an IDE on 16 GB is uncomfortable, which is what the profiles are for.

**Makefile targets.**

| Target | Does |
| --- | --- |
| `make up-min` | `core` only |
| `make up` | `core` + `workflow` + `events` |
| `make up-events` | `core` + `events` |
| `make up-observability` | Adds `observability` to whatever is running |
| `make up-ai` | `make up` + `ai` |
| `make up-all` | Every profile (PLT-20) |
| `make down` | Stop, keep volumes |
| `make reset` | Stop and remove volumes — a genuine cold start |
| `make logs [S=<svc>]` | Follow logs |
| `make keys` | Generate the RS256 key pair into `keys/` if absent (PLT-25) |
| `make migrate` | For each service with a store, `docker compose run --rm <svc> alembic upgrade head` — explicit, never on start (S14) |
| `make topics` | Create topics and DLQs, set configs, register every schema; idempotent |
| `make seed` | Load demo data idempotently (below) |
| `make test` | Unit, integration and e2e suites |
| `make lint` / `make fmt` | ruff (lint and format) and mypy |
| `make reindex` | Full, resumable backfill of the retrieval index ([0009](0009-assistant-service.md)) |
| `make evaluate` | Retrieval evaluation ([0009](0009-assistant-service.md)) |
| `make evaluate-answers` | Answer-quality evaluation ([0009](0009-assistant-service.md)) |
| `make dlq-replay TOPIC=… GROUP=…` | Re-produce DLQ messages (§3.1.1.6) |
| `make flower` | Start Flower against RabbitMQ on demand |

A first-time reader runs `make keys && make up && make migrate && make topics && make seed`. `make up` runs `make keys` and `make topics` itself when their outputs are missing, so the three-command path of the README (`make up && make migrate && make seed`) still works.

**Multi-stage Dockerfiles.** One per service and worker, sharing a base layer for `common` ([archive/0000 §6.3](archive/0000-system-architecture.md#63-deployment-strategy)):

```dockerfile
# ---- base: interpreter + common, shared by every image (cached once)
FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN useradd --uid 10001 --create-home app
COPY common /src/common
RUN pip install --no-cache-dir /src/common

# ---- builder: the service's dependencies into a venv
FROM base AS builder
COPY services/<svc>/pyproject.toml /src/services/<svc>/
RUN python -m venv /venv && /venv/bin/pip install --no-cache-dir /src/common /src/services/<svc>

# ---- runtime: venv + code, non-root
FROM python:3.12-slim AS runtime
RUN useradd --uid 10001 app
COPY --from=builder /venv /venv
COPY services/<svc>/app /app/app
COPY services/<svc>/alembic* /app/
USER app
ENV PATH=/venv/bin:$PATH
HEALTHCHECK CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/health' % os.environ['PORT'])" || exit 1
CMD uvicorn --factory main:create_application --host 0.0.0.0 --port "$PORT" --loop uvloop   # shell form, so $PORT expands
```

Worker images use the same stages with a different `CMD`. Images carry no secrets and no keys; both are mounted at run time. SIGTERM drains in-flight requests before the event loop closes ([archive/0001 §6.1](archive/0001-foundation-and-core-services.md#61-scalability-and-fault-tolerance)).

**Migrations as an explicit step.** Per-service Alembic histories (S14, A1), run by `make migrate`, never on application start-up — automatic migration means N scaled replicas racing to migrate the same database ([archive/0001 §6.3](archive/0001-foundation-and-core-services.md#63-deployment-strategy)). Every revision has a working `downgrade`. Every service's `alembic/env.py` carries the S4 `render_item` hook (§3.1.1.1). CI runs `alembic upgrade head` from empty and `alembic check`, which must report no pending diff — the proof that a vocabulary change really was migration-free. `analytics` has no Alembic; its collections and indexes (TTL indexes included) are created by an idempotent command that `make migrate` also runs ([0008](0008-analytics-service.md)).

**Seeding.** `make seed` is idempotent and re-runnable, and goes through the public APIs wherever a workflow is involved, so seeding exercises the real paths: an admin (the only way an admin is created, besides another admin), two merchants and two customers, categories, stock for every variant, products **published through the real publishing workflow** (the earlier shortcut of writing `published` directly is retired), sample orders in several states, the retrieval seed corpus of ~200 realistic products across 8 categories ([0009](0009-assistant-service.md)), and prompt templates from `ops/prompts/`. What each service seeds is specified in its own RFC; `scripts/seed.py` orchestrates them in dependency order.

**`.env` and keys.** `.env.example` lists every variable with a safe local placeholder; `.env` is gitignored. `make keys` generates the RS256 pair into the gitignored `keys/` directory; the private key is mounted only into `identity`, the public key into every verifier (PLT-9). Real credentials in the system are, in full: the LLM provider key (in `assistant`, `indexer-worker`, `celery-worker-ai` only), the Grafana admin password, MinIO, RabbitMQ and database role passwords, and the service-token client secret — all from `.env`.

**Failure behaviour.** A missing variable fails its container at start-up (config validation). A missing key pair fails `identity` and every verifier at start-up with a message naming `make keys`. A profile that is not running degrades its dependants as each subsection states.

**Observability.** `make logs` and `docker compose ps` for liveness; the platform overview dashboard for everything else.

#### 3.1.2. API changes

The platform exposes no public business endpoint. Its interfaces are internal: the library API services import, the event envelope on the wire, the schema registration flow, the operational endpoints every component serves, and the names workers and services agree on.

##### 3.1.2.1. common public API

What a service may import. Anything not listed is private to the package and may change without notice.

| Module | Public symbols | Used by |
| --- | --- | --- |
| `auth` | `Principal`, `TokenType`, `current_principal`, `optional_principal`, `require_roles(*roles)`, `require_scope(scope)`, `JwtVerifier` | Every service; `gateway` for early rejection |
| `service_token` | `ServiceTokenClient`, `service_http_client()` (an `httpx.AsyncClient` that attaches the token) | `temporal-worker`, `celery-worker`, any internal caller |
| `config` | `BaseServiceSettings`, `KafkaSettings`, `RedisSettings`, `TemporalSettings`, `CelerySettings`, `MinioSettings`, `OtelSettings` | Every component |
| `context` | `request_context()`, `get_request_id()`, `get_client_id()`, `get_principal()`, `RequestContextMiddleware` | Every service |
| `logging` | `configure_logging(settings)` | Every component |
| `errors` | `Problem`, `DomainError`, `NotFound`, `Conflict`, `Forbidden`, `Unprocessable`, `RateLimited`, `NotImplementedYet`, `DependencyUnavailable`, `install_error_handlers(app)` | Every service |
| `pagination` | `PageParams`, `Page[T]`, `LIMIT_CEILING = 100` | Every service with a list endpoint |
| `db` | `make_engine(settings)`, `get_session`, `unit_of_work(session)`, `Base` (declarative base with S1/S3 mixins) | Every Postgres service |
| `db_types` | `DomainEnumText`, `render_item` | Every Postgres service, every `alembic/env.py` |
| `enums` | `DomainEnum`, `UnknownEnumValueError`, `widen_for_response(enum_class)` | Every service's `app/models/enums.py` |
| `idempotency` | `idempotent(store)`, `IdempotencyStore` (protocol), `canonical_request_hash(body)` | `order`; any later service with a state-creating POST |
| `outbox` | `outbox.add(session, *, event_type, aggregate_type, aggregate_id, payload, causation_id=None, client_id=…)`, `OutboxRow` (mapped to the shared DDL) | Every producer |
| `events` | `Envelope`, `EventRegistry`, `AvroSerde`, `EventProducer`, `EventConsumer`, `DedupeStore`, `PostgresDedupeStore`, `run_consumers(*consumers)` | The relay; every consumer |
| `tracing` | `setup_tracing(app_or_none, settings)`, `current_traceparent()` | Every component |
| `metrics` | `setup_metrics(app_or_none, settings)`, `meter` | Every component |
| `temporal` | `get_temporal_client(settings)`, `default_activity_options()`, `activity_idempotency_token(...)` | `catalog`, `order`, `temporal-worker` |
| `celery_app` | `make_celery(name, settings)`, `ROUTES`, `BEAT_SCHEDULE`, `SmartRetailTask` | `celery-worker`, `celery-worker-ai`, services that enqueue |
| `redis` | `get_redis(settings)`, `Cache`, `RateLimiter`, `PubSub` | `gateway`, `catalog`, `inventory`, `notification`, `assistant`, `order` |
| `health` | `health_router(checks=…)`, `DatabaseCheck`, `KafkaCheck`, `RedisCheck`, `TemporalCheck`, `ConsumerLagReport` | Every component |

Versioning: the package is versioned with the repository. A breaking change to a public symbol changes every caller in the same diff, and every service's CI runs on it.

##### 3.1.2.2. The event envelope on the wire

The envelope's fields and meaning are defined once in [RFC 0000 §3.1.2.4](0000-architecture.md#3124-event-envelope): `event_id`, `event_type`, `event_version`, `occurred_at`, `producer`, `correlation_id`, `causation_id`, `actor_id`, `client_id`, `payload`. This section fixes only how the relay puts it on Kafka.

| Kafka part | Content |
| --- | --- |
| Topic | `smartretail.<event_type>` from the registry (PLT-26) |
| Key | `aggregate_id` as a UTF-8 string (C3) |
| Value | Confluent Avro: magic byte, schema id, then one Avro record containing the envelope fields and a typed `payload` record |
| Headers | `traceparent` (and `tracestate` if present), `event_id`, `event_type`, `producer` — duplicated from the envelope so a tool can filter without deserialising |

Each topic's schema is the shared envelope fields from `common/events/schemas/envelope.avsc`, expanded by the registration script, plus that topic's payload record:

```json
{
  "type": "record", "name": "OrderConfirmed", "namespace": "smartretail.order",
  "fields": [
    {"name": "event_id",       "type": {"type": "string", "logicalType": "uuid"}},
    {"name": "event_type",     "type": "string"},
    {"name": "event_version",  "type": "int"},
    {"name": "occurred_at",    "type": {"type": "long", "logicalType": "timestamp-micros"}},
    {"name": "producer",       "type": "string"},
    {"name": "correlation_id", "type": "string"},
    {"name": "causation_id",   "type": ["null", "string"], "default": null},
    {"name": "actor_id",       "type": ["null", {"type": "string", "logicalType": "uuid"}], "default": null},
    {"name": "client_id",      "type": ["null", "string"], "default": null},
    {"name": "traceparent",    "type": ["null", "string"], "default": null},
    {"name": "payload",        "type": {"type": "record", "name": "OrderConfirmedPayload", "fields": [ "… owned by 0005 …" ]}}
  ]
}
```

`traceparent` is the PLT-6 proposal. Payload fields are owned by the producing service's RFC (§3.1.2.3 *Events produced*) and must carry identifiers and the minimum needed to render — no credentials, no password hashes, no card data, no full addresses beyond what a shipment notification needs; Kafka is unauthenticated locally, so payload minimalism is the actual control ([archive/0003 §6.2](archive/0003-events-and-observability.md#62-security-considerations)). `DomainEnum` values serialise as Avro `string`, not Avro `enum`, so adding a vocabulary value is not a schema change — the same reasoning as S4.

##### 3.1.2.3. Avro schema registration flow

```
 developer edits common/events/schemas/<topic>.avsc
        │
        ▼
 CI: scripts/topics.py --check
        │  1. expand envelope fields into each topic schema
        │  2. parse every schema (fastavro) — syntax and logical types
        │  3. POST /compatibility/subjects/<topic>-value/versions/latest
        │       against a registry started by the test job, pre-loaded with the last released schemas
        │  4. round-trip: a fixture record per topic serialises and deserialises
        │  5. old-reader/new-writer and new-reader/old-writer tests for every changed schema
        │  → any incompatible change fails the build, with the registry's message
        ▼
 make topics (local) / deploy step
        │  1. create topics and <topic>.dlq with the configs in §3.1.3.3 (idempotent: describe, create if absent, alter configs)
        │  2. PUT /config  {"compatibility": "BACKWARD"}   (global)
        │  3. POST /subjects/<topic>-value/versions   for each schema (no-op if identical)
        │  4. record the returned ids in the log
        ▼
 producers and consumers resolve schemas by id at run time (cached by the serde)
```

Subject naming is `TopicNameStrategy` (`<topic>-value`); keys are plain strings and have no registered schema. Producers never auto-register (`auto.register.schemas=false`): a schema that was not registered by the flow above cannot be produced, so every event on the wire went through the compatibility check. The registry is the enforcement point; CI is where the failure is reported.

##### 3.1.2.4. Operational endpoints — /health, /health/ready, /metrics

Every service serves these on its own port; every worker serves them on the ops port 8090. They are internal: the gateway does not route them.

| Endpoint | Semantics | Status codes |
| --- | --- | --- |
| `GET /health` | **Liveness.** The process is running and its event loop responds. Checks nothing external, so a dependency outage never gets a healthy container restarted | `200 {"status":"ok"}` |
| `GET /health/ready` | **Readiness.** Required checks: a database round trip (`SELECT 1`) for every store the component owns; the Kafka broker (metadata fetch) for the relay and every consumer; the Temporal frontend for `temporal-worker`; RabbitMQ for Celery workers. Optional checks — reported, never failing: Redis, the collector. Consumers additionally **report** broker connectivity and consumer lag per partition; lag is data, not unreadiness | `200` when every required check passes (`"status":"ready"` or `"degraded"` if an optional check fails); `503` with the same body otherwise |
| `GET /metrics` | Prometheus exposition (OpenMetrics when requested, so exemplars are carried) from the OTel SDK's Prometheus reader (PLT-7) | `200` |
| `GET /docs`, `GET /openapi.json` | Per-service OpenAPI; the gateway also serves an aggregated view ([0002](0002-gateway-service.md)) | `200` |

Readiness body:

```json
{
  "status": "degraded",
  "checks": {
    "database": {"ok": true,  "latency_ms": 2},
    "kafka":    {"ok": true},
    "redis":    {"ok": false, "required": false, "error": "connection refused"}
  },
  "consumers": [
    {"group": "notification", "topic": "smartretail.order.confirmed", "partition": 0, "lag": 0}
  ]
}
```

Compose health checks call `/health/ready` with a short interval, which is what makes `depends_on: service_healthy` meaningful: no service accepts traffic before its store is reachable.

##### 3.1.2.5. Worker contracts — task queues, Celery queues, consumer groups

Names that services and workers must agree on. Changing one is a coordinated change across both sides.

| Kind | Name | Producer side | Consumer side |
| --- | --- | --- | --- |
| Temporal namespace | `smartretail` | `catalog`, `order` (start) | `temporal-worker` |
| Temporal task queue | `catalog-publishing` | `catalog` starts `ProductPublishingWorkflow` | `temporal-worker` |
| Temporal task queue | `order-lifecycle` | `order` starts `OrderWorkflow` | `temporal-worker` |
| Celery queue | `media`, `notifications`, `analytics`, `maintenance` | Activities, consumers, Beat | `celery-worker` |
| Celery queue | `assistant` | `assistant` API, Beat | `celery-worker-ai` |
| Kafka consumer group | `catalog` | — | `catalog` consumer (`identity.merchant.status_changed`) |
| Kafka consumer group | `inventory` | — | `inventory` consumer (`catalog.product.published`, `order.cancelled`) |
| Kafka consumer group | `notification` | — | `notification` consumer (`order.*`, `payment.*`, `shipment.*`, `inventory.stockout`, `identity.user.registered`) |
| Kafka consumer group | `analytics` | — | `analytics` consumer (every topic) |
| Kafka consumer group | `assistant-indexer` | — | `indexer-worker` (`catalog.product.*`) |
| Kafka consumer group | `dlq-replay` | — | `make dlq-replay` |

Which topics each group consumes is authoritative in each owner's §3.1.2.4 *Events consumed*; this table records only the names.

#### 3.1.3. Data/schema changes

The platform owns no database table. The shared `outbox` and `processed_events` tables are conventions (S12, S13) whose DDL — columns, the partial index the relay uses, and the per-database matrix of who carries which — is defined once in [RFC 0000 §3.1.3.2](0000-architecture.md#3132-shared-tables--outbox-and-processed_events). PLT-6 proposes two nullable columns for it (`traceparent`, `actor_id`) as an amendment there. Everything below is non-database state and provisioning.

##### 3.1.3.1. PostgreSQL instance, databases and roles

One PostgreSQL 16 instance (A2), image `pgvector/pgvector:pg16`, one named volume. `ops/postgres/init/` creates, idempotently:

| Database | Role(s) with access | Notes |
| --- | --- | --- |
| `identity_db` | `identity_role` (owner); `outbox_relay` on `outbox` | |
| `catalog_db` | `catalog_role`; `outbox_relay` on `outbox` | |
| `order_db` | `order_role`; `outbox_relay` on `outbox` | |
| `inventory_db` | `inventory_role`; `outbox_relay` on `outbox` | |
| `notification_db` | `notification_role` | Consumer only — no `outbox` |
| `assistant_db` | `assistant_role`; `outbox_relay` on `outbox` | `CREATE EXTENSION vector` is in `assistant`'s first migration, not the init script |
| `temporal`, `temporal_visibility` | `temporal_role` | Schema set up by Temporal's own tooling |

Each service role is granted `CONNECT` on its own database only and no cross-database grants exist — R1 fails at connection time, not at review time ([RFC 0000 §3.1.1.3](0000-architecture.md#3113-boundary-rules-r1r8)). `outbox_relay` has `CONNECT` on the five producer databases and, once each service's migration has created `outbox`, `SELECT, UPDATE (published_at), DELETE ON outbox` granted by that migration (the init script cannot grant on a table that does not exist yet). `PUBLIC` is revoked from every database. Splitting a service onto its own instance later is a connection-string change.

##### 3.1.3.2. Redis keyspaces

Every key the platform writes, with its TTL. A key pattern not in this table does not exist; a new one is an amendment here.

| Pattern | DB | TTL | Written by | Use |
| --- | --- | --- | --- | --- |
| `rl:{scope}:{identifier}:{window_start}` | 0 | The rule's window — 60 s for per-minute rules, 3600 s for per-hour rules (PLT-10) | `gateway`, `notification` (ack), `assistant` | Fixed-window rate-limit counters. `scope` is a rule name (`login_ip`, `login_email`, `orders_user`, `default_ip`, `search_ip`, `search_user`, `assistant_user`, `assistant_anon`, `generate_merchant`, `ack_client`); `identifier` is the IP hash, user id, email hash or `(user, client)` pair |
| `idem:{user_id}:{key}` | 0 | 24 h | `order` via `common.idempotency` | Idempotency fast path, ahead of the Postgres claim. Value: `{request_hash, status, body}` of a **completed** request only (PLT-19) |
| `cache:products:{hash}` | 0 | 60 s | `catalog` | Browse query results, keyed by a hash of the full filter set; invalidated by the `catalog` consumer on `product.updated` |
| `cache:inventory:{variant_id}` | 0 | 10 s | `inventory` | Availability — short, because stock is the fastest-moving fact in the system; also absorbs the assistant's live-hydration reads |
| `nt:user:{user_id}` | 0 | — (Pub/Sub channel, not a stored key) | `notification` | Fan-out channel (C17). The message body carries `origin_client_id` and `scope`; the delivering replica filters per connection ([0007](0007-notification-service.md)) |
| `nt:ticket:{ticket}` | 0 | 60 s | `notification` | Single-use SSE stream ticket, redeemed with `GETDEL` (C19). Value `{user_id, client_id, jwt_exp}` as JSON, so the stream's identity comes from the redeemed ticket, never from the URL |
| `nt:conns:{user_id}` | 0 | 60 s, refreshed by the heartbeat | `notification` | Per-user connection count for the 3-stream cap; a crashed replica's slots expire on their own |
| `nt:client:{user_id}:{client_id}` | 0 | 45 s, refreshed by the 20 s heartbeat, deleted on clean close | `notification` | Presence hint for origin-scoped delivery (C21). Value `1` — deliberately *not* a replica id |
| `budget:{scope}:{date}` | 0 | 48 h | `assistant` | Token-budget fast path (E10, PLT-11). `scope` is `user:<uuid>`, `ip:<hash>` or `global`, matching `token_budgets.scope`; a hash with `tokens_used`, `requests`, `cost_usd` incremented with `HINCRBY`/`HINCRBYFLOAT`. Reconciled into `token_budgets` by `reconcile_token_budgets`; re-seeded from Postgres when missing |
| `celery-task-meta-*` (and other `celery-*` keys) | 1 | 24 h (`result_expires`) | Celery | Results backend |

**Rejected keyspaces**, recorded so they are not reinvented: `nt:cursor:{user_id}:{client_id}` as a write-behind cache of `cursor_seq` (acks are batched at ≤ 1 per 2 s per client, so the Postgres upsert is not a hot path, and a second copy of a cursor is a second thing to be wrong); deriving `nt:conns:*` by counting `nt:client:{user_id}:*` (a `SCAN` on the connection-open path). Both from [archive/0003 §3.1.3](archive/0003-events-and-observability.md#313-dataschema-changes); the stream-cursor table itself stays in Postgres because a Redis flush must not turn every device into a cold start ([0007](0007-notification-service.md#stream_cursors)).

##### 3.1.3.3. Kafka topic configuration

Sixteen topics from the [event catalogue](0000-architecture.md#3123-event-catalogue) (fourteen catalogue rows, two of which name a pair), each with a DLQ.

| Topic | Key | Partitions | RF | `cleanup.policy` | `retention.ms` | DLQ |
| --- | --- | --- | --- | --- | --- | --- |
| `smartretail.identity.user.registered` | `user_id` | 3 | 1 | `delete` | 7 d | `….dlq` |
| `smartretail.identity.merchant.status_changed` | `user_id` | 3 | 1 | **`compact`** | — | `….dlq` |
| `smartretail.catalog.product.published` | `product_id` | 3 | 1 | **`compact`** | — | `….dlq` |
| `smartretail.catalog.product.updated` | `product_id` | 3 | 1 | `delete` | 7 d | `….dlq` |
| `smartretail.catalog.product.unpublished` | `product_id` | 3 | 1 | `delete` | 7 d | `….dlq` |
| `smartretail.order.created` | `order_id` | 3 | 1 | `delete` | 7 d | `….dlq` |
| `smartretail.order.confirmed` | `order_id` | 3 | 1 | `delete` | 7 d | `….dlq` |
| `smartretail.order.failed` | `order_id` | 3 | 1 | `delete` | 7 d | `….dlq` |
| `smartretail.order.cancelled` | `order_id` | 3 | 1 | `delete` | 7 d | `….dlq` |
| `smartretail.payment.succeeded` | `order_id` | 3 | 1 | `delete` | 7 d | `….dlq` |
| `smartretail.payment.failed` | `order_id` | 3 | 1 | `delete` | 7 d | `….dlq` |
| `smartretail.shipment.status_changed` | `order_id` | 3 | 1 | `delete` | 7 d | `….dlq` |
| `smartretail.inventory.reserved` | `variant_id` | 3 | 1 | `delete` | 7 d | `….dlq` |
| `smartretail.inventory.released` | `variant_id` | 3 | 1 | `delete` | 7 d | `….dlq` |
| `smartretail.inventory.stockout` | `variant_id` | 3 | 1 | `delete` | 7 d | `….dlq` |
| `smartretail.assistant.interaction` | `conversation_id` | 3 | 1 | `delete` | 7 d | `….dlq` |
| every `<topic>.dlq` | source key | 3 | 1 | `delete` | 30 d (PLT-22) | — |

Compaction on `identity.merchant.status_changed` and `catalog.product.published` is because the latest state per key is what a new consumer needs ([archive/0003 §3.1.3](archive/0003-events-and-observability.md#313-dataschema-changes)). Two consequences are recorded rather than discovered: a compacted topic has no time bound, so a replay from earliest can reach events older than the 30-day dedupe retention (PLT-17) — handlers of compacted topics must therefore be idempotent by state (upsert semantics), not only by `processed_events`; and `min.cleanable.dirty.ratio=0.5`, `segment.ms=1 d` keep compaction observable in a week of local use. Multi-broker is a variable change: replication factor and `min.insync.replicas` are set by the topic script from `KAFKA_REPLICATION_FACTOR`.

##### 3.1.3.4. MinIO buckets and key layout

One bucket, `smartretail-media` (PLT-16): private, versioning off, no public policy, created by `minio-init`.

| Object | Key | Written by |
| --- | --- | --- |
| Original | `products/{product_id}/original/{random}.{ext}` | `catalog` on upload; the key is `product_media.storage_key` |
| Derived size | `products/{product_id}/derived/{size}/{random}.{ext}` | `process_media`; the `size → key` map is `product_media.derived` |

`{random}` is a 128-bit random token (randomised storage keys, [archive/0002 §6.2](archive/0002-core-business-logic-and-workflows.md#62-security-considerations)), so keys are unguessable and never derived from a filename. `{ext}` comes from the validated content type, not the upload name. The sizes, the allow-listed content types and the size ceiling are `catalog`'s ([0004](0004-catalog-service.md)). Objects are never overwritten: a replaced image is a new key, and orphan cleanup is `catalog`'s concern.

##### 3.1.3.5. Temporal namespaces

| Namespace | Retention | Task queues | Search attributes |
| --- | --- | --- | --- |
| `smartretail` | 7 days of closed-workflow history | `catalog-publishing`, `order-lifecycle` | Default only; owning RFCs may register custom attributes (for example `OrderId`, `ProductId`) through the init step |

The `default` namespace is not used. Temporal's persistence is the `temporal` and `temporal_visibility` databases (§3.1.3.1).

##### 3.1.3.6. MongoDB instance

`mongo:7`, profile `events`, one node, one database `analytics_db`, one application user from `.env`, port 27017 internal only. The collections, their shapes, the 7-day TTL on `raw_events` and the `processed_events` Mongo variant with its 30-day TTL are `analytics`'s, defined in [RFC 0008 §3.1.3](0008-analytics-service.md#313-dataschema-changes). If 0008 needs multi-document transactions for its dedupe-plus-effect write, Mongo runs as a single-node replica set (`--replSet rs0` with a one-shot `rs.initiate()`), which is a Compose change here.

#### 3.1.4. Breaking changes

SmartRetail has no released API and no production data, so nothing here breaks an external client. Changes to platform behaviour that a service author will notice:

| Change | Impact | Handling |
| --- | --- | --- |
| `X-Request-ID` semantics change to the trace ID (C12) | Header name and behaviour are unchanged; only the value's provenance differs | No client depends on the format |
| Media processing moves from a Temporal activity to a Celery task | The publishing workflow's timing changes; the activity now enqueues and awaits | Internal; the workflow contract is unchanged |
| Rate limiting introduced | Clients can receive `429` | Documented per route by the owners; generous limits; `Retry-After` always present |
| Browse and inventory reads become cached | Up to 60 (or 10) seconds of staleness | Documented per endpoint; invalidation on update keeps the common case fresh |
| Outbox rows start being published and swept | A row older than 7 days after publication is gone | Nothing reads the outbox except the relay |
| PLT-6 adds `traceparent` and `actor_id` to `outbox` and `traceparent` to the envelope | An additive, nullable column and an optional Avro field with a `null` default | `FULL`-compatible; existing rows publish with `null` |
| Postgres image changes to `pgvector/pgvector:pg16` | None for other databases; the extension exists only where created | Drop-in; switched once, early, so the volume never needs recreating |
| Every migration is additive; no migration destroys or rewrites data | — | S14 |

#### 3.1.5. Action Plan

Ordered so that each platform piece exists before the first service that needs it, the event backbone is proven end to end on one topic before it is widened, and observability lands early enough to be used while debugging the rest.

| # | Task | Output | Depends on |
| --- | --- | --- | --- |
| P1 | Repository skeleton, tooling, `.env.example`, Makefile, ruff + mypy config, pytest config | `make lint` and `make test` run against an empty suite | — |
| P2 | `common` core: settings base, async DB session, JSON logging with request-ID contextvar, problem-detail errors, pagination, `DomainEnum` and `DomainEnumText` with the `render_item` hook | Importable package, unit-tested. The S4 pair has tests for strict write, tolerant read, and the R8 vocabulary scan | P1 |
| P3 | Compose `core`: Postgres 16 (pgvector image) with the database-per-service init script, roles, grants, healthchecks, named volume; `make keys` | `docker compose up postgres` healthy; databases and roles exist; a role cannot connect to another's database | P1 |
| P4 | `common` auth: RS256 verification with mounted key and JWKS refresh, `Principal`, `require_roles`, `optional_principal` | Unit tests for verify, expiry, tamper, wrong key, claim contents, dependency allow/deny/missing/malformed | P2 |
| P5 | Multi-stage Dockerfiles with the shared base layer; non-root; health checks | Every image builds; `make up-min` healthy | P3 |
| P6 | Temporal + MinIO in Compose under `workflow`; `temporal-worker` skeleton; bucket init; connectivity smoke test | Temporal UI reachable; worker registers on both task queues; bucket exists | P5 |
| P7 | `common`: outbox model and writer, idempotency mechanism, service-token client and `require_scope`, Temporal client factory, default activity options | Unit-tested, importable | P2, P6 |
| P8 | Worker binding test and PLT-1 wiring in `temporal-worker` | A worker can import each service's activities with that service's settings only | P7 |
| P9 | Kafka (KRaft) + Schema Registry under `events`; `make topics` | Broker healthy; topics and DLQs exist; console produce/consume works | P5 |
| P10 | Avro schemas for every topic in `common/events/schemas/`, the envelope template (with `client_id` and `traceparent`), registration script and CI compatibility check | Registry holds every schema; compatibility `BACKWARD`; a v1 consumer reads a v2 record and vice versa | P9 |
| P11 | `common` events: envelope, request context with `client_id`, Avro serde, producer wrapper, consumer base with dedupe, retry, pause and DLQ | Unit-tested | P10 |
| P12 | `outbox-relay`: advisory lock, poll, `SKIP LOCKED` claim, produce, mark published, poison hold, metrics — **one topic end to end** | Crash-restart test shows no loss (G1) | P11 |
| P13 | Relay widened to all five producer databases; `outbox_relay` grants in each producer's migration | Every outbox row reaches its topic | P12 |
| P14 | OTel Collector, Jaeger, Prometheus, Grafana under `observability`; provisioning from `ops/` | All four UIs reachable | P5 |
| P15 | `common` tracing: FastAPI, SQLAlchemy, `httpx`, `redis` instrumentation; OTLP exporter; trace ID in the log formatter; gateway sets `X-Request-ID` to the trace ID | A gateway → catalog call is one trace with SQL spans | P14 |
| P16 | Trace context into and out of the outbox and envelope; producer and consumer spans (C11, PLT-6) | A trace spans producer and consumer | P15, P13 |
| P17 | Temporal instrumentation — interceptor on client and worker | The trace reaches the workflow and its activities | P15, P6 |
| P18 | Metrics: Prometheus reader, `/metrics` on every component, standard instruments; worker ops port; scrape config | Prometheus scrapes every target successfully | P15 |
| P19 | Grafana dashboards: platform overview, service RED, event pipeline health, workers | Dashboards provisioned as code, loading with live data (G6) | P18 |
| P20 | RabbitMQ + `celery-worker` + embedded Beat; Celery app factory, routing table, retry defaults, trace propagation; the task inventory registered | Tasks execute; Beat schedules fire | P9 |
| P21 | Redis: client factory, `Cache`, `RateLimiter`, `PubSub` with fail-open; idempotency fast path wired | Limits enforced; cache hit and invalidation tested; Redis down → reads succeed, limits fail open | P9 |
| P22 | DLQ replay tool (`make dlq-replay`) and the poison-message test | A bad event lands in DLQ; the partition keeps moving; replay works (G4) | P11 |
| P23 | Compose profiles finalised; `up-min`, `up`, `up-events`, `up-observability`, `up-all` | A developer can run any subset (G12) | P19, P20 |
| P24 | `ai` profile: `indexer-worker` container on the consumer base; `celery-worker-ai` with the `assistant` queue; `budget:*` keyspace and reconciliation task wiring; `make reindex`, `make evaluate`, `make evaluate-answers` targets | Indexer consumes with its own group; bulk queue isolated | P11, P20 |
| P25 | End-to-end platform checks: one checkout produces one trace across the five components; replay test; two-instance relay test; the diagnosis walkthroughs written up in the README | `make test` green cold (G5, G8, G11) | P13, P16, P17, P22 |

**Delivery phasing.**

| Component | Milestone |
| --- | --- |
| Repository layout, Makefile, `.env.example`, `make keys` | M1 |
| `common` core — config, logging, errors, pagination, DB session, `DomainEnum`/`DomainEnumText`, R8 guard, auth and `require_roles` | M1 |
| PostgreSQL instance, init script, roles; Compose `core` profile; multi-stage Dockerfiles; `make up`/`migrate`/`seed`/`test`/`lint`/`fmt`/`reset` | M1 |
| Temporal server + UI, `temporal-worker`, task queues, namespace, determinism and versioning rules | M2 |
| MinIO and the media bucket | M2 |
| `common` outbox writer, idempotency mechanism (Postgres claim), service-token client and `require_scope`, Temporal client factory | M2 |
| Compose `workflow` profile | M2 |
| Kafka + Schema Registry, topics, DLQs, Avro schemas and registration flow, `make topics` | M3 |
| `common` envelope, serde, producer, consumer base with dedupe/retry/DLQ/pause | M3 |
| `outbox-relay`, `outbox_relay` role, outbox sweep | M3 |
| RabbitMQ, `celery-worker`, Beat, routing and retry defaults | M3 |
| Redis and every Part A keyspace (rate limits, idempotency fast path, caches, notification pub/sub, tickets, presence, Celery results) | M3 |
| MongoDB container | M3 |
| OTel Collector, Jaeger, Prometheus, Grafana; tracing across HTTP, Kafka, Temporal and Celery; metric conventions; dashboards; `observability` profile | M3 |
| `events` profile; `up-events`, `up-observability`, `up-all`, `up-min`, `dlq-replay`, `flower` | M3 |
| Review of the platform against G1–G13 | M4 |
| `indexer-worker` runtime, pgvector image in use, `ai` profile, `make reindex`, `make evaluate` | M5 |
| `celery-worker-ai`, `assistant` queue, `budget:*` keyspace and reconciliation, `make evaluate-answers` | M6 |
| Review of the Part B platform additions | M7 |

### 3.2. Rationale

**Why the outbox relay rather than publishing from the handler.** A handler that commits to Postgres and then produces to Kafka has a window: crash in between, and the state changed but nobody was told. There is no retry that fixes it, because the process that knew is gone. The outbox makes the event part of the same transaction as the state change, so the two cannot disagree; the relay then has the much easier job of publishing rows that are already durable, and its failure mode is a duplicate rather than a loss. The outbox tables were written before Kafka existed specifically so that adding the backbone was a new process rather than a rewrite of every handler (B11).

**Why a polling relay rather than change data capture.** One small Python process with a 500 ms poll against a partial index, versus Kafka Connect plus Debezium plus logical replication configuration plus a transform to shape rows into events (`0003-Q1`, §5 Alternative 2). Revisited only if publish latency ever matters.

**Why one relay loop per database (PLT-2).** C2's claim that multiple relay instances "can run without double-publishing" is true, and C3's per-aggregate order is a stated goal (G1). `SKIP LOCKED` alone cannot give both: two instances holding disjoint batches will interleave an aggregate's events across instances. An advisory lock per database costs one line and preserves both properties; horizontal throughput for the relay is not a requirement at this scale, and the standby takes over within a poll interval.

**Why at-least-once with idempotent consumers, not Kafka transactions.** Exactly-once in Kafka is exactly-once *within* Kafka — it does not extend to a Postgres write or an email. The end-to-end property still has to come from consumer idempotency, so transactions add coordination cost and operational complexity without removing the work they appear to remove. Recording `(consumer_group, event_id)` in the same transaction as the effect is simpler, debuggable, and gives the actual guarantee.

**Why Avro and a Schema Registry rather than JSON.** Events outlive the code that wrote them. JSON gives no compatibility checking, so a renamed field is discovered by a consumer crash. `BACKWARD` compatibility in the registry means the incompatible change fails at registration — in CI, with a clear message. [Part A](../part-a.md) names the Schema Registry explicitly, and this is what it is for. JSON Schema was the alternative; Avro is what "Schema Registry" implies and gives compact payloads with real evolution rules (C5).

**Why pausing is distinct from dead-lettering (PLT-5).** C6 exists so that one bad event cannot stop a partition. An outage of the embedding provider or a consumer's own database is not a bad event — every event would fail, and dead-lettering all of them would turn a five-minute outage into a manual replay of a whole topic. Pausing is what Kafka is for: the events wait, retained, and flow again when the dependency returns.

**Why Celery *and* Kafka *and* Temporal.** The distinction is operational, not aesthetic. Kafka broadcasts a fact to unknown consumers and retains it for replay. Celery does one job soon, with per-task retry, and is the right shape for "resize this image" or "send this email". Temporal runs a durable multi-step process with compensation. Media processing is a Celery task; the fact that a product was published is a Kafka event; the publishing sequence is a Temporal workflow (C7, A12). Using one for another's job is the anti-pattern this design avoids.

**Why workers host service code rather than calling every service over HTTP (PLT-1).** B1's seam — one business rule reachable from a router, an activity, a consumer and a CLI — is the highest-leverage structural rule in the codebase ([archive/0006 §3.2](archive/0006-service-decomposition-and-data-model.md#32-rationale)). Routing every activity and task through HTTP would keep workers credential-free but would force each service to grow an internal endpoint per task and would put a network hop inside every sweep. Binding each task to its owner's settings keeps one implementation per rule, keeps the database role per service (so R1's grant test still holds), and confines cross-service effects to APIs — which is where R4 wants them. The price, a worker process holding several roles, is stated in §3.3 and made visible by a test.

**Why trace context must cross Kafka, Temporal and Celery.** A trace that stops at the producer answers "the API was fast" and nothing about where the work actually went. [Part A](../part-a.md) §6 asks for failure diagnosis in background workers and notifications — exactly the part that lives past the producer. Carrying `traceparent` through the outbox, the envelope, Temporal headers and Celery headers turns five disconnected traces into one, and it is the difference between the observability stack being a demo and being useful.

**Why metrics are pulled rather than pushed (PLT-7).** Pull gives Prometheus's `up` as a free liveness signal for every component, keeps `/metrics` working when the collector is off, and avoids a second metrics path. C10's actual goal — no service knows which backend stores its signals — is met because services use the OTel SDK and expose a standard format.

**Why Redis now and only for these jobs.** It was deferred until a measured need existed. Several do: login brute force is a real hole with no other answer, the browse query is the hottest read path, Celery needs a result backend, and notification fan-out needs a bus that any replica can publish to. Every use is either disposable or backed by a durable record, so a Redis outage is a latency event, never a data event.

**Why the observability stack is off by default.** Four extra containers to run a unit test is a tax on every iteration. A profile costs one flag and removes it (C14).

**Why one Postgres instance with several databases.** Isolation matters; container count does not. Separate logical databases with separate roles make cross-service SQL impossible, which is the property being bought (A2). Running eight Postgres containers on a laptop that also hosts Kafka, Temporal and Grafana buys nothing further, and splitting later changes only connection strings.

**Why migrations are an explicit step.** Automatic migration on start means N replicas racing to migrate one database, and it hides a schema change inside a deploy. An explicit `make migrate` is one more command and makes every schema change a visible event.

### 3.3. Tradeoffs

| Trade being made | The cost | Why it is acceptable |
| --- | --- | --- |
| Twenty-odd containers | Real resource pressure; slow cold start; the heaviest configuration to debug | Profiles mean the full set runs only for demos and reviews. Every component traces to a Part A or Part B requirement |
| At-least-once delivery | Every consumer must implement dedupe; duplicates are normal, not exceptional | The dedupe is ~15 lines in a shared base class, and it is the honest guarantee |
| Outbox relay as a separate process | Another component to run and monitor; adds up to one poll interval of publish latency | The alternative loses events. Latency is bounded and configurable |
| One active relay loop per database (PLT-2) | Relay throughput per database is one loop | Ordering is a stated goal; throughput is not a stated problem |
| Poison rows hold their aggregate (PLT-4) | One aggregate's events stall until a human fixes the row; the hold is in memory and is rebuilt after a restart by re-failing | Reordering or dropping events is worse. Write-time schema validation makes poison rows rare |
| Avro over JSON | Harder to inspect by eye; the registry is another dependency; schema files to maintain | Compatibility enforcement is the point. `kafka-avro-console-consumer` covers the debugging need |
| Three orchestration systems | Three sets of operational concepts, three failure modes, three UIs | Required by Part A, and each has a distinct job (A12) |
| Workers hold several services' credentials (PLT-1) | A compromised `temporal-worker` or `celery-worker` reaches more than one database | Each task binds exactly one role, asserted by a test; splitting a worker per service is a queue and container change, not a code change |
| Pull metrics, push traces (PLT-7) | Two paths to understand; the collector is not the single fan-out point for every signal | Each signal uses its backend's native model; a missing collector costs traces only |
| DLQ without an automatic replay loop | A human decides when to replay | Automatic replay of a poison message is an infinite loop. The replay tool exists; the trigger is deliberate |
| Compacted topics outlive dedupe retention | A full replay can re-deliver an event whose dedupe row was swept | Handlers of compacted topics are required to be idempotent by state; the replay test exercises it |
| Single-node Kafka, Redis, Mongo, Temporal; in-memory Jaeger | No replication; restarts pause pipelines or lose trace history | Local environment by design; every one is a configuration change, none a design decision that blocks scaling |
| A shared `common` package | A change there touches every service; it can grow into a hidden monolith | Restricted by rule to non-domain concerns (R8), with a vocabulary scan and an import-direction test; any business rule that appears in it is a review failure |
| Limit/offset pagination | Deep offsets scan; concurrent inserts can shift a page | Pages are shallow and heavily filtered. Cursor pagination is additive when a path needs it |
| Explicit migrations | One more command in the bring-up | Prevents replica races and makes schema changes visible |

#### 3.3.1. Challenges and Open Questions

| # | Question | Current position | Needs a decision by |
| --- | --- | --- | --- |
| `0003-Q1` | Polling relay or logical replication (Debezium)? | Polling, 500 ms interval. One small Python process versus Kafka Connect plus Debezium plus their configuration | Revisit only if latency matters |
| `0003-Q2` | One consumer process per service, or per topic? | Per service, subscribing to several topics, with the dedupe keyed by consumer group. Fewer containers, and the ordering guarantee is per key anyway | Decided |
| `0003-Q5` | Sampling for traces? | 100% locally. A parent-based sampler is configured but set to always-on, so turning it down is a variable | Not required |
| `0003-Q7` | Do we need consumer lag alerting? | Metrics and a dashboard panel, no alerting. There is nothing to alert into | Not required |
| `0003-Q8` | Should the DLQ replay be an API or a CLI? | CLI in `make`. An API for replaying arbitrary events is a footgun with no operator to use it | Decided |
| `0003-Q9` | Partition count of 3 with one broker — meaningful? | Yes: it forces the per-key ordering design to be real, and multi-partition consumer behaviour to be tested, which a single partition would hide | Decided |
| `0003-Q11` | One Pub/Sub channel per connected user, or a fixed set of sharded channels? | Per user. Redis handles tens of thousands of channels comfortably, and a replica receives only messages it can deliver. Revisit only if `SUBSCRIBE` churn shows in Redis latency. The delivery design is [0007](0007-notification-service.md)'s | Revisit under load |
| `0000-Q9` | Should the observability stack be in the default Compose profile? | No — `observability` profile, off by default, on for demos and reviews | Decided |
| `0001-Q4` | Where do RS256 keys come from outside a laptop? | Local: a generated key pair in a gitignored directory, public key mounted into every verifier and refreshed from JWKS (PLT-9). Production key management is out of scope | Not required |
| Q-PLT-1 | Should workers be split per owning service so no process holds two roles (PLT-1)? | Not now — one `celery-worker` and one `temporal-worker`, with `celery-worker-ai` already split for the LLM key. The split is a queue binding and a Compose entry | Part A review |
| Q-PLT-2 | Should `outbox` gain `publish_attempts` and `last_error` so a poison hold is visible in SQL and survives a relay restart? | Not yet — the hold is in memory and re-established on the first failed attempt after restart; the metric and log are the visibility. Adding them is an amendment to [RFC 0000 §3.1.3.2](0000-architecture.md#3132-shared-tables--outbox-and-processed_events) | Part A review |
| Q-PLT-3 | Is PLT-6 (`outbox.traceparent`, `outbox.actor_id`, envelope `traceparent`) accepted into RFC 0000? | Proposed. Without it traces join by ID but lose parent/child structure across Kafka, and the envelope's `actor_id` has no source column | Before the relay is built |
| Q-PLT-4 | Are 30 days of `processed_events` (PLT-17) and 7 days of published outbox rows (PLT-18) the right retentions? | Yes for local use. Dedupe must outlive the longest *time-based* topic retention (7 days) with margin; compacted topics are covered by state-idempotent handlers | Part A review |
| Q-PLT-5 | Does analytics need a Mongo replica set for transactional dedupe? | Owned by [0008](0008-analytics-service.md); the Compose change is ready (§3.1.3.6) | Before the analytics consumer is built |
| Q-PLT-6 | Should each image record the `common` git SHA at build time and report it on `/health`, so version skew (PLT-27) is visible? | Not yet — `make up` rebuilds every consumer together, so skew lasts only as long as a deliberate single-service rebuild | Part A review |

---

## 4. Dependencies/blockers/known issues

**Dependencies.**

| Dependency | Version | Used for |
| --- | --- | --- |
| Python | 3.12 | Runtime |
| FastAPI | 0.115.x | HTTP layer, OpenAPI generation |
| Uvicorn (+ `uvloop`) | 0.32.x | ASGI server |
| SQLAlchemy | 2.0.x (async) | ORM |
| asyncpg | 0.30.x | Postgres driver |
| Alembic | 1.14.x | Migrations |
| Pydantic / pydantic-settings | 2.x (2.10 pinned for the S4 behaviour) | Validation, configuration |
| PyJWT | 2.10.x | RS256 sign and verify |
| httpx | 0.28.x | Service-to-service calls, gateway proxying, test client |
| pytest / pytest-asyncio | 8.x / 0.24.x | Tests |
| ruff, mypy | current | Lint, format, type-check |
| PostgreSQL (`pgvector/pgvector:pg16`) | 16 (pgvector 0.7.x) | Data store for six services and Temporal |
| Docker + Compose | v2 | Local environment |
| Temporal server + UI | 1.25.x | Workflow orchestration |
| `temporalio` (Python SDK) | 1.8.x | Workflows, activities, worker, tracing interceptor |
| MinIO + `boto3` | latest / 1.35.x | Media object storage |
| Apache Kafka (KRaft, no ZooKeeper) | 3.8 / cp-7.7 | Event backbone |
| Confluent Schema Registry | 7.7.x | Avro schema governance |
| `confluent-kafka[avro]` | 2.6.x | Python producer and consumer |
| `fastavro` | 1.9.x | Schema parsing in CI |
| RabbitMQ | 3.13-management | Celery broker |
| Celery | 5.4.x | Background tasks and Beat |
| Flower | 2.x (on demand) | Celery inspection |
| Redis | 7.4 | Cache, rate limit, Pub/Sub, tickets, budgets, Celery results |
| `redis[hiredis]` | 5.x | Async client |
| MongoDB | 7.x | Analytics store (used by [0008](0008-analytics-service.md)) |
| OpenTelemetry SDK + instrumentations | 1.28.x | Traces and metrics |
| OTel Collector (contrib) | 0.112.x | Trace pipeline |
| Jaeger | 1.62 (all-in-one) | Trace storage and UI |
| Prometheus / Grafana | 2.55 / 11.x | Metrics and dashboards |
| `sse-starlette` | 2.x | SSE responses, shared by [0007](0007-notification-service.md) and [0009](0009-assistant-service.md) |

**External dependencies.** One only: an LLM provider API key, needed by `assistant`, `indexer-worker` and `celery-worker-ai`. It should be obtained before the retrieval work starts rather than during it. Every other dependency is a public image or package.

**Blockers.** None. The fake and cassette providers owned by [0009](0009-assistant-service.md) keep development and CI unblocked without the key.

**Known issues.**

- Local resource pressure — the full stack is ~8 GB; running everything plus an IDE on 16 GB is uncomfortable. Mitigated by profiles, not eliminated.
- Single-broker Kafka: no replication, and a broker restart pauses the pipeline.
- Redis is a single node and Pub/Sub is at-most-once: a Redis restart drops in-flight frames; clients recover on reconnect rather than instantly.
- Jaeger all-in-one stores traces in memory; a restart loses history.
- Prometheus retention is local and short; Grafana dashboards are provisioned but not alerting.
- DLQ replay is manual.
- No log aggregation; correlation is by trace ID across `docker compose logs`.
- No TLS anywhere; plain HTTP and plaintext brokers inside Compose.
- Until PLT-6 is accepted, traces cross Kafka by trace ID only, without parent/child linkage.
- Worker processes hold more than one service's credentials (PLT-1, Q-PLT-1).
- A poison outbox row stalls its aggregate until fixed (PLT-4).
- The implemented Dockerfiles (`services/*/Dockerfile`) do not yet use the shared `base` stage shown in §3.1.1.12: each copies `common` into its own builder stage and installs it in the same `RUN` as the service's dependencies, so any `common` change reinstalls every third-party dependency of every image.
- The `common` version is fixed at `0.1.0`, so nothing in a running container says which copy of the library it was built with (Q-PLT-6).

---

## 5. Alternate solutions

**Alternative 1 — RabbitMQ for everything; no Kafka.** Already present for Celery; simpler operationally; adequate pub/sub via exchanges.

*Rejected.* RabbitMQ deletes a message once consumed, so a new consumer cannot read history — which breaks the indexer's backfill, any replay, and analytics' `raw_events` escape hatch. There is no offset and no schema governance. Part A names Kafka *and* RabbitMQ precisely because they are different tools, and using one for both jobs would leave the retention and replay properties undemonstrated.

**Alternative 2 — Debezium CDC instead of an application-level outbox.** Read the WAL, no outbox table, no relay to write.

*Rejected.* It requires Kafka Connect, the Debezium connector, logical replication configuration, and a transform to shape rows into events — a heavier stack than the ~150-line relay it replaces, and one that couples the event schema to the table schema unless the outbox pattern is used anyway. Debezium's own recommended approach for domain events is, in fact, an outbox table. If it were ever adopted, it would read the *same* outbox tables, so the services would not change.

**Alternative 3 — Kafka transactions for exactly-once.**

*Rejected.* The guarantee does not extend past Kafka, so consumer idempotency is needed regardless, and the transaction adds coordination cost for a property already obtained (§3.2).

**Alternative 4 — Celery for all asynchronous work; no Kafka.**

*Rejected.* Celery is point-to-point with no retention and no fan-out; every new consumer means the producer changes to enqueue another task. That is exactly the coupling events exist to remove, and Part A §4 asks for work that runs "independently from core request flows" with multiple reactions per fact.

**Alternative 5 — Kafka for everything, replacing Temporal and Celery.**

*Rejected.* Kafka has no workflow state, no compensation primitive, and no per-task retry with backoff. Building those on top is reimplementing Temporal badly ([RFC 0000 §5](0000-architecture.md#5-alternate-solutions)). The choreographed-saga variant is argued in [0005](0005-order-service.md#5-alternate-solutions).

**Alternative 6 — Celery chains with `on_error` callbacks instead of Temporal.**

*Rejected.* Celery has no workflow state, no visibility into where a chain stopped, and error callbacks that themselves fail are lost. It is the right tool for one-shot background jobs and is used for exactly that.

**Alternative 7 — Redis Streams instead of Kafka as the event backbone.** Already in the stack; consumer groups, acknowledgements and a replayable log; far lighter than a broker plus registry.

*Rejected.* Retention in Redis is bounded by memory and trimming, so history is exactly what gets thrown away under pressure; persistence is secondary to Redis's design; there is no schema registry or compatibility enforcement; and partition-by-key ordering with consumer-group rebalancing would be hand-built. It also puts the durable event log on the one store this design treats as disposable. Part A names Kafka with a Schema Registry. The same reasoning rejects Redis Streams (or a second Kafka topic) for notification fan-out, where at-most-once Pub/Sub over a durable row is sufficient ([archive/0003 §5](archive/0003-events-and-observability.md#5-alternate-solutions), Alternative 12; [0007](0007-notification-service.md#5-alternate-solutions)).

**Alternative 8 — Every notification replica joins Kafka with its own consumer group and filters locally**, so no Redis Pub/Sub is needed.

*Rejected.* Consumer groups become dynamic — created and abandoned per pod — leaving offset garbage in the broker on every deploy; every replica deserialises every event to discard most of them; and the durable, dedupe-backed persistence path becomes entangled with the disposable fan-out path. Recorded here because it is a platform consequence; the delivery argument is [0007](0007-notification-service.md)'s.

**Alternative 9 — Elastic APM or another single-vendor observability stack.**

*Rejected.* OpenTelemetry is what Part A names, and vendor-neutral instrumentation means the backend is a collector configuration rather than a code change. The three-backend split also demonstrates signal separation.

**Alternative 10 — Skip Grafana; use Prometheus' own UI.** One fewer container.

*Rejected.* "Basic monitoring" is an explicit outcome, and a dashboard is the artefact a reviewer can actually read. Grafana with provisioned dashboards-as-code is cheap.

**Alternative 11 — Log aggregation (Loki) in addition to traces.**

*Deferred.* The actual requirement is correlating logs with a request, which the trace ID in every log line already satisfies. `docker compose logs` is adequate at this scale; Loki would be a fifth observability container for marginal gain.

**Alternative 12 — Managed services** (Confluent Cloud, Temporal Cloud, a hosted vector database) to reduce local load.

*Rejected.* External accounts, credentials, and network dependency for an assignment whose target environment is Docker Compose on a laptop and whose review is a walkthrough on that laptop.

**Alternative 13 — Credential-free workers: every activity and task calls its owner over HTTP.**

*Rejected for now* (PLT-1). It would make R1 hold per process as well as per role, but it doubles the surface of every service with internal task endpoints, puts a network hop inside every sweep, and splits one rule's implementation across a task and an endpoint. Kept as the direction of travel if Q-PLT-1 is decided the other way, together with per-service workers.

**Alternative 14 — An off-the-shelf gateway (Traefik, Kong, Envoy)** for routing, rate limiting and TLS.

*Deferred, not rejected* — owned by [0002](0002-gateway-service.md#5-alternate-solutions). Its platform consequence is only that the rate-limit keyspace in §3.1.3.2 would move into the gateway product's own store.

**Alternative 15 — Several Postgres containers, one per service.**

*Rejected.* Separate logical databases with separate roles already make cross-service SQL impossible; more containers buy nothing further on a laptop that must also run Kafka, Temporal and Grafana (A2).

---

## 6. Production readiness

"Production" here means the assignment's target: Docker Compose on a developer machine, demonstrated to a mentor. The properties below are structural — designed to hold in principle, demonstrated at that scale.

### 6.1. Scalability and Fault tolerance

- **No event loss.** The outbox makes the state change and the event atomic; the relay's failure mode is duplication, which consumers absorb (G1, C4).
- **Poison messages cannot stall a partition.** Three retries with backoff, then DLQ, then offset commit (G4, C6). A dependency outage pauses instead of dead-lettering (PLT-5).
- **Poison outbox rows cannot stall a table.** They hold their own aggregate only (PLT-4).
- **Consumer lag is visible and bounded.** Exported, dashboarded; consumers scale horizontally up to the partition count.
- **Backpressure has a defined shape at every layer.** Kafka retains and consumers lag; RabbitMQ buffers per queue so media cannot starve notifications and bulk generation cannot starve chat; the gateway rate-limits; Temporal's task queues buffer when workers are saturated. This is Part A's "degrades gracefully rather than collapsing", made concrete per layer.
- **Blast radius.** A consumer being down means events accumulate in Kafka and are processed on recovery. Kafka being down means outbox rows accumulate in Postgres and publish on recovery. Neither affects checkout, because nothing synchronous is downstream of the order path.
- **Cache failure is not request failure.** A Redis outage degrades to uncached reads; rate limiting fails open; idempotency falls through to Postgres.
- **Idempotent everywhere.** Producers may duplicate, consumers dedupe, activities carry deterministic tokens (B3), tasks are re-runnable, Beat sweeps are safe to run twice.
- **Workflow durability.** Temporal persists every workflow event; a worker killed mid-order resumes from the last completed activity. Timeouts exist at every level so nothing hangs.
- **Local token verification.** `identity` being down does not stop any service serving authenticated traffic; the mounted key covers a cold start with `identity` absent (PLT-9, R7).
- **Connection pooling.** `pool_size=10`, `max_overflow=20` per service, so a scaled-out service does not exhaust Postgres connections.
- **Graceful shutdown.** SIGTERM drains in-flight requests; consumers finish the current message and commit before exiting; Celery workers finish or requeue (late ack).
- **Health and readiness** on every component, with Compose dependency ordering, so nothing accepts traffic before its store is reachable.
- **Known ceilings.** One Postgres instance, one broker, one Redis, one Mongo node, single-node Temporal, in-memory Jaeger. All are single-node by choice and none is a design decision that blocks scaling.

### 6.2. Security considerations

- **Infrastructure is not exposed.** Kafka, Schema Registry, RabbitMQ, Redis, Mongo, Postgres, Temporal's gRPC port, the collector, and every service other than the gateway are reachable only on the Compose network. UIs (Temporal, MinIO console, RabbitMQ management, Jaeger, Prometheus, Grafana) are bound to `127.0.0.1` for the demo. Broker ports are never published.
- **R1 is enforced by grants.** One role per service database, `CONNECT` on its own database only, `PUBLIC` revoked. The relay's role reaches `outbox` and nothing else (PLT-3). Worker processes bind one role per task (PLT-1).
- **Tokens.** RS256; the private key exists only in `identity`; verifiers hold the public key. Service tokens are short-lived, scoped, rejected on user routes; user tokens are rejected on internal routes, so a stolen customer token cannot call `/internal/v1/inventory/reservations`.
- **Event payloads carry identifiers and the minimum needed to render.** No password hashes, no card data, no full addresses beyond what a shipment notification requires. Kafka is unauthenticated locally, so payload minimalism is the actual control.
- **Traces and logs.** Request and response bodies are never span attributes; headers are allow-listed and the collector strips `Authorization` and `Cookie` as defence in depth. Logs never carry passwords, tokens, stream tickets, API keys or bodies. Traces may contain URLs and user ids.
- **Secrets.** Environment only; `.env` gitignored; `.env.example` has placeholders; keys generated into a gitignored directory and mounted, never baked into images. The LLM API key is confined to `assistant`, `indexer-worker` and `celery-worker-ai` (PLT-12), never logged, never a span attribute, never in an error body.
- **Media.** Private bucket, randomised keys, no execution path over uploaded content; validation is `catalog`'s ([0004](0004-catalog-service.md)).
- **Workflow inputs carry identifiers, not secrets.** Temporal history is readable in its UI.
- **Error hygiene.** Problem details never include stack traces, SQL or exception text.
- **Grafana** runs with a non-default admin password from `.env` and anonymous access disabled.
- **Containers run as a non-root user.**
- **Accepted gaps** — no TLS or SASL on Kafka; no mTLS between services; Redis without auth on the internal network; Mongo with a single local credential; MinIO with credentials from `.env`; plain HTTP everywhere. All are local-environment decisions, listed so they read as choices.

### 6.3. Deployment strategy

- **One `docker-compose.yml`** at the repository root with the five profiles of §3.1.1.12. `make up` runs `core` + `workflow` + `events`; `make up-all` runs every profile (PLT-20).
- **One Makefile** as the single entry point (§3.1.1.12).
- **Multi-stage Dockerfiles**, non-root, one per service and worker, sharing a base layer for `common`.
- **`common` changes** — reach a service only through a rebuild of its image; rebuild every consumer together with `make up`, and change contract-bearing modules backward-compatibly first (PLT-27, §3.1.1.1).
- **Configuration** is twelve-factor, environment-only, validated at startup; a missing variable fails the container immediately.
- **Database provisioning** — the Postgres init script creates every database with a dedicated role, granted only on its own database, plus Temporal's two databases and the relay role.
- **Migrations** — per service, explicit, never on startup, every one with a working `downgrade`; `alembic check` in CI.
- **Topic and schema provisioning** — `make topics`, an idempotent init script, so a cold clone reproduces the pipeline exactly.
- **Temporal schema** — set up by Temporal's init before the server starts; namespace registered in the same step.
- **MinIO bucket** — created idempotently at start-up by `minio-init`, versioning off, private by default.
- **Dashboards and datasources** — provisioned as code from `ops/grafana/`, surviving `make reset`.
- **Startup ordering** — health-gated `depends_on`; Kafka before the relay and consumers; consumers retry broker connection with backoff rather than crash-looping.
- **Independent deployability.** Consumers deploy independently of their service's API process; a consumer restart never interrupts an API. Workers deploy separately from the services they call, so workflow and task code can be redeployed without restarting an API.
- **Workflow deploys** — `workflow.patched()` for sequence changes; drain-and-restart locally; the replay test gates a deploy.
- **Rollback.** Per-service Alembic `downgrade`; registry schema versions are additive and never deleted; a bad consumer is rolled back and its offsets rewound if needed; `make reset` for a broken local environment. Because histories are per service, rolling back one never requires rolling back another.
- **CI** — lint, type-check, the full test suite against ephemeral Postgres, Kafka, Schema Registry and Redis service containers, the schema compatibility check, and the R8 scan.

---

## 7. Test Plan

**Unit** — no I/O.

- `common` auth: sign and verify, expiry rejection, signature-tamper rejection, wrong-key rejection, unknown `kid` triggers a JWKS refresh then rejects, claim contents mapped into `Principal`.
- `require_roles`: allow, deny, missing token, malformed token; service token rejected on a user route. `require_scope`: user token rejected on an internal route; wrong scope → `403`.
- Problem-detail serialisation for every `DomainError` subclass, validation `violations[]`, and the generic `500` carrying no exception text.
- Pagination: default, limit ceiling clamped at 100, negative values rejected.
- `DomainEnum`/`DomainEnumText`: strict write (unknown value raises, surfacing as `StatementError` with `UnknownEnumValueError` on `.orig`, and no row is written); tolerant read for `passthrough`, `fallback` and `raise`; `render_item` renders `sa.Text()`.
- **R8**: the vocabulary scan over `common` returns nothing; no module imports `services.*`.
- Idempotency: key canonicalisation, request hashing (key order and whitespace do not change the hash), same-key-different-body detection, fast-path replay only for completed entries.
- Envelope construction, correlation propagation, and Avro round-trip for every topic schema.
- Outbox helper: unknown `event_type` rejected; payload invalid against its schema rejected at write; `client_id`, `actor_id`, `traceparent` taken from context.
- Consumer base: dedupe hit and miss; retry counting; DLQ routing with headers; offset commit on skip; `DependencyUnavailable` pauses and does not count an attempt; `after_commit` not called on rollback.
- Rate limiter: window boundaries, per-scope isolation, TTL equals the window, fail-open on a Redis error.
- Trace context injection into and extraction from the outbox row, the envelope, Kafka headers and Celery headers.
- Celery routing table: every registered task maps to exactly one queue; every Beat entry names a registered task.

**Integration** — real Postgres, Kafka, Schema Registry, Redis and RabbitMQ (Compose services or testcontainers).

*relay*
- A committed outbox row reaches its topic with the right key, schema and headers.
- **Crash between produce and mark-published → the row republishes; the consumer dedupes; the effect happens once** (G1).
- Two relay instances do not double-publish, and only one holds each database's advisory lock; killing the holder hands over within one poll interval (C2, PLT-2).
- Ordering: three events for one aggregate arrive on one partition in order, including across a relay hand-over (C3).
- A poison row holds its aggregate; other aggregates keep flowing; fixing the row releases the hold without a restart (PLT-4).
- A rolled-back state change leaves no outbox row, and a committed one always has one.
- `sweep_published_outbox` deletes only published rows older than 7 days.

*consumers*
- **Replay: reset offsets to earliest, re-consume everything, assert every counter and record is unchanged** (G3) — run by each owning service against its consumers, on the shared harness provided here.
- A malformed event goes to DLQ; the next event is processed normally (G4).
- A consumer down during publication catches up fully on restart.
- A dependency outage pauses the partition; on recovery every paused event is processed and none is in the DLQ (PLT-5).
- `make dlq-replay` re-produces a DLQ message; the consumer processes it once.

*schemas*
- A deliberately incompatible change is rejected by the registry checker in CI (G2).
- A v1-schema consumer reads a v2 record (unknown field ignored); a v2 consumer reads a v1 record (`client_id` and `traceparent` resolve to null); the registry accepts the v2 registration under `BACKWARD`.
- A producer cannot produce with an unregistered schema.

*Redis*
- Cache hit, miss, and invalidation by prefix.
- Rate limit returns `429` with `Retry-After` at the threshold and recovers after the window.
- Redis down → reads still succeed, limits fail open, idempotency falls through to Postgres.

*workers*
- Temporal replay test: recorded histories of both workflows replay against the current code without non-determinism errors.
- Worker killed mid-workflow resumes and reaches a terminal state (the scenario itself is owned by [0005](0005-order-service.md#7-test-plan)).
- **Worker binding**: every activity and task module imports only its owning service's settings and opens connections only with that service's role; a task that tries another role fails the test (PLT-1).
- A Celery task killed mid-execution is redelivered (late ack); Beat fires every scheduled task.

*provisioning and boundaries*
- A connection as each service role to every other service's database is refused ([RFC 0000 §7](0000-architecture.md#7-test-plan) boundary tests run on this provisioning).
- `outbox_relay` can read and mark `outbox` and cannot read any other table.
- `make topics` twice is a no-op; `make seed` twice is a no-op; `make migrate` from empty reaches head in every database and `alembic check` reports no diff.

**Observability tests**

- **A checkout produces exactly one trace containing spans from gateway, order, inventory, the Temporal workflow, the relay, and the notification consumer** (G5). Asserted by querying the Jaeger API for the trace ID and checking the service set — not by looking at the UI.
- Every service's `/metrics` exposes request count, error count and the latency histogram, and Prometheus scrapes every target successfully (G6).
- Every log line emitted during one request carries the same trace ID, equal to `X-Request-ID` (C12).
- Consumer lag, DLQ depth, outbox backlog and Celery queue depth are exported and non-null.
- No metric carries a forbidden label (a scrape of every target is checked against the PLT-8 deny-list).
- A streaming response's server span closes at stream end, not at first byte.

**End-to-end** — through the gateway, cold start, full stack.

1. `make reset && make up-all && make migrate && make seed && make test` from a clean clone on a machine that has never run it (G11).
2. Each `make up-*` target starts a healthy subset (G12).
3. Place an order → assert its events are on their topics, consumed, and joined in one trace (G1, G5).
4. Inject a malformed event → DLQ receives it, the pipeline keeps running, the DLQ depth metric moves (G4).
5. Stop Kafka for a minute during checkouts → checkouts succeed; outbox backlog rises then drains to zero after restart; every event is delivered once.
6. Stop Redis → browse and checkout still succeed; rate limits fail open.

**Diagnosis walkthroughs** — G8 requires demonstrating diagnosis, not just instrumentation. One per [Part A](../part-a.md) §6 flow, each starting from a symptom, written up in the README:

| Symptom | Path to the cause |
| --- | --- |
| "A product never published" | `publish-status` → workflow id → Temporal UI → failing activity → the trace for that activity → the exception |
| "Checkout is slow" | Grafana latency panel → the slow endpoint → an exemplar trace → the slow span (catalog call, or reservation lock wait) |
| "Stock looks wrong" | Ledger reconciliation query → the movement rows → `reference_id` → the order → its trace |
| "A background job didn't run" | Flower / Celery metrics → queue depth → task failure → the task's trace and exception |
| "A customer got no notification" | `orders.notified_at` → the `order.confirmed` event id → consumer dedupe table → DLQ → delivery attempts |
| "A customer got it late, or only on refresh" | Active-connection metric → fan-out publish→frame latency → reconnect rate → catch-up rows per reconnect. Distinguishes "never pushed" from "pushed to nobody" from "delivered on reconnect" |
| "An event never arrived" | Outbox backlog and oldest-unpublished age for the producing database → poison-row count → relay logs for the row → registry error |

**Explicitly not tested** — load and throughput beyond functional concurrency; Kafka failover (single broker); alerting.

---

## 8. Impact Analysis

**On every service.** Each service imports `common` for auth, config, logging, errors, pagination and its DB session, so its own code is its domain. Each producer gains an `outbox` and grants the relay on it; each consumer gains `processed_events` and a class on the consumer base; each gains a `/metrics` endpoint and an OTLP exporter by calling two setup functions. Behavioural changes a service author must know about are in §3.1.4.

**On other RFCs.**

| RFC | Change | Why it cannot live only there |
| --- | --- | --- |
| [0000](0000-architecture.md) | PLT-6: `outbox.traceparent`, `outbox.actor_id`, envelope `traceparent` (proposed amendment) | The shared DDL and the envelope are defined once, there |
| [0004 catalog](0004-catalog-service.md), [0005 order](0005-order-service.md) | Workflows and activities live in the service package and are hosted by `temporal-worker` on `catalog-publishing` and `order-lifecycle` (PLT-1, PLT-14) | Hosting is platform; the steps are theirs |
| [0005](0005-order-service.md), [0006](0006-inventory-service.md), [0007](0007-notification-service.md), [0008](0008-analytics-service.md), [0009](0009-assistant-service.md) | Celery tasks and Beat schedules are routed by the table in §3.1.1.4 | One routing table for every queue |
| Every producer's migration | Grants `SELECT, UPDATE (published_at), DELETE ON outbox TO outbox_relay` (PLT-3) | The init script cannot grant on a table that does not yet exist |
| Every consumer | Handlers of compacted topics are idempotent by state (§3.1.3.3) | Dedupe retention is shorter than compaction's horizon |
| [0009 assistant](0009-assistant-service.md) | `budget:*` keyspace, `celery-worker-ai`, `assistant` queue | The keyspace and worker are platform; the budget rules are 0009's |

**On effort.** The platform is front-loaded: the `common` core and the local environment before the first service, Temporal and MinIO before the sagas, and the event backbone with the observability stack as the heaviest single block. The action plan's ordering is the mitigation — P12 proves the whole event path on one topic before P13 widens it, and observability (P14–P19) lands early enough to debug everything after it.

**Risks.**

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| The event backbone and observability stack do not fit their slot | **High** | High | Priority order if time runs short: event pipeline (P9–P13) > tracing (P14–P17) > metrics and dashboards (P18–P19) > Celery (P20) > Redis extras (P21). The first two are the named outcomes; the rest can be thinner |
| Local resources exhausted | **High** | High | Profiles; `make up-min`; documented memory expectations; Jaeger in-memory; one Postgres instance |
| Kafka + Avro + Registry learning curve | High | Medium | P9–P12 are a single-topic spike before anything is widened |
| Temporal's learning curve | High | High | P6 is a connectivity spike; the simpler publishing workflow is built first by [0004](0004-catalog-service.md) as the learning vehicle |
| Consumer idempotency implemented shallowly and only failing on replay | Medium | High | The replay test (G3) is an acceptance criterion, not an optional extra |
| Trace context silently dropped across Kafka, Temporal or Celery, leaving fragmented traces | Medium | Medium | G5 is asserted programmatically against the Jaeger API; PLT-6 persists context across the outbox gap |
| Relay reordering an aggregate's events across instances | Medium | High | PLT-2 advisory lock; the ordering test runs across a hand-over |
| `common` accumulating domain logic and becoming a hidden monolith | Medium | Medium | R8 scan and import-direction test in CI; review check at every design review |
| Event schema churn breaking consumers | Medium | Medium | Registry with `BACKWARD` compatibility; additive-only changes; producers cannot auto-register |
| A worker task reaching another service's database | Low | High | PLT-1 binding test |
| Gateway buffering silently degrading SSE into delayed delivery | Medium | Medium | Owned by [0002](0002-gateway-service.md), which tests it by measurement; the platform's part is that streaming spans stay open |
| Laptop cannot run the full stack for the review | Medium | High | Profiles from the start; observability off by default; the review runs the profiles each walkthrough needs |

**On reviewers.** A review of the platform should be able to check: an event survives a relay crash; replaying a topic changes nothing; a poison message does not stop the pipeline; one checkout is one trace across five components; every component has `/metrics` and a dashboard panel; `common` contains no domain vocabulary; each of Part A §6's flows can be diagnosed from a symptom; and the whole stack comes up from a cold clone.

---

## 9. Useful Links/References

**Project documents**

- [RFC 0000 — Architecture](0000-architecture.md) — boundary rules, decisions, event catalogue and envelope, schema conventions, shared tables
- Service RFCs: [0002 gateway](0002-gateway-service.md) · [0003 identity](0003-identity-service.md) · [0004 catalog](0004-catalog-service.md) · [0005 order](0005-order-service.md) · [0006 inventory](0006-inventory-service.md) · [0007 notification](0007-notification-service.md) · [0008 analytics](0008-analytics-service.md) · [0009 assistant](0009-assistant-service.md)
- [Part A](../part-a.md) §4, §5, §6 and the tech stack · [Part B](../part-b.md) · [Milestones](../milestones.md) · [Execution Guidelines](../execution-guidelines.md)
- Archived sources: [archive/0000](archive/0000-system-architecture.md) · [archive/0001](archive/0001-foundation-and-core-services.md) · [archive/0002](archive/0002-core-business-logic-and-workflows.md) · [archive/0003](archive/0003-events-and-observability.md) · [archive/0004](archive/0004-retrieval-layer.md) · [archive/0005](archive/0005-ai-assistant-and-streaming.md) · [archive/0006](archive/0006-service-decomposition-and-data-model.md)

**Technical references**

- Transactional outbox — https://microservices.io/patterns/data/transactional-outbox.html
- PostgreSQL `SKIP LOCKED` — https://www.postgresql.org/docs/16/sql-select.html#SQL-FOR-UPDATE-SHARE
- PostgreSQL advisory locks — https://www.postgresql.org/docs/16/explicit-locking.html#ADVISORY-LOCKS
- Confluent Schema Registry compatibility — https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html
- `confluent-kafka-python` — https://docs.confluent.io/kafka-clients/python/current/overview.html
- Kafka idempotent producer and delivery semantics — https://kafka.apache.org/documentation/#semantics
- Kafka log compaction — https://kafka.apache.org/documentation/#compaction
- Celery routing and task design — https://docs.celeryq.dev/en/stable/userguide/routing.html
- Temporal Python SDK — https://docs.temporal.io/develop/python
- Temporal versioning (patching) — https://docs.temporal.io/develop/python/versioning
- Temporal testing and time-skipping — https://docs.temporal.io/develop/python/testing-suite
- OpenTelemetry Python — https://opentelemetry.io/docs/languages/python/
- OTel Collector configuration — https://opentelemetry.io/docs/collector/configuration/
- OpenTelemetry messaging semantic conventions — https://opentelemetry.io/docs/specs/semconv/messaging/
- W3C Trace Context — https://www.w3.org/TR/trace-context/
- RED method — https://grafana.com/blog/2018/08/02/the-red-method-how-to-instrument-your-services/
- Prometheus naming practices — https://prometheus.io/docs/practices/naming/
- Redis Pub/Sub — https://redis.io/docs/latest/develop/interact/pubsub/
- MinIO Python SDK — https://min.io/docs/minio/linux/developers/python/minio-py.html
- RFC 9457, Problem Details for HTTP APIs — https://www.rfc-editor.org/rfc/rfc9457
- RFC 7519, JSON Web Token — https://www.rfc-editor.org/rfc/rfc7519
- IETF draft, The Idempotency-Key HTTP Header Field — https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/
- The Twelve-Factor App — https://12factor.net/
- SQLAlchemy 2.0 async ORM — https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html
- Alembic — https://alembic.sqlalchemy.org/

---

## 10. Related tickets/issues

| ID | Title | Maps to |
| --- | --- | --- |
| *(to be created)* | PLT-P1 … PLT-P25 | The tasks in §3.1.5, one ticket each |
| — | [RFC 0000](0000-architecture.md) amendment for PLT-6 | Q-PLT-3 |
| — | archive/0001 §4 — login rate limiting | Closed by the Redis rate limiter here and the policy in [0002](0002-gateway-service.md) |
| — | archive/0002 `0002-Q7` — media processing in Celery | Closed: `process_media` on the `media` queue (§3.1.1.4) |
| — | archive/0002 known issue — outbox rows accumulating unread | Closed by `outbox-relay` and `sweep_published_outbox` |
| — | archive/0001 known issue — no distributed tracing | Closed by §3.1.1.11 |

---

## 11. Notes

- **On the relay being the most important piece.** The single-topic, crash-tested relay (P12) is the task that most reduces risk in the whole platform. Everything after it is repetition of a proven pattern; widening to every topic before the first one survives a restart would mean debugging sixteen things at once.
- **On what "basic monitoring" means.** RED metrics with dashboards for every component, one trace across every hop, and the diagnosis walkthroughs — not a subset. What is deliberately not included is alerting, SLOs and log aggregation (§2.2).
- **On decision IDs from archive/0001.** That RFC numbered its own decisions A1–A8, which overlap the platform-wide A1–A14 of archive/0000 with different meanings. This RFC uses the archive/0000 numbering throughout. For traceability: archive/0001 A1 = A1, A2 = A2, A3 = A3, A4 = A4, A6 ("the gateway verifies; each service re-verifies") = A5, A7 (async) = A6, A8 (per-service Alembic) = S14; archive/0001 A5 (stateless access, stateful refresh tokens) is `identity`'s ([0003](0003-identity-service.md)).
- **On counts.** The archive refers to "fourteen topics"; that is the number of rows in the event catalogue, two of which name a pair (`payment.succeeded` / `payment.failed`, `inventory.reserved` / `inventory.released`), so there are sixteen topics and sixteen DLQs (§3.1.3.3). The archive's container table (4 / 9 / 21 / 24) is replaced by §3.1.1.12's, which counts one-shot init containers and `celery-worker-ai`; the archive's infrastructure figures did not reconcile with the containers it named.
- **On things this RFC decided that the archive left open.** Every `PLT-` decision fills a gap — a name, a TTL, a retention, a hosting rule — or resolves a conflict between archive sources. Each names what it resolves, so a reviewer who disagrees can disagree with one decision rather than with the document.
- **Definition of done.** Goals G1–G13 verified by their stated checks; `make reset && make up-all && make migrate && make seed && make test` green from a clean clone; the replay, poison-message, relay hand-over, single-trace and worker-binding tests green; Grafana dashboards loading with live data; the diagnosis walkthroughs written up; this RFC moved to **DONE** with review outcomes folded in.

---

## 12. Comments

*Reviewer comments go here. Please raise objections against a numbered decision (A*, B*, C*, D*, E*, R8, S4/S12–S14 as implemented here, or PLT-1 … PLT-26), an open question, a keyspace, a topic configuration, or a rejected alternative in §5, so the discussion attaches to something specific.*

| Reviewer | Section | Comment | Resolution |
| --- | --- | --- | --- |
| | | | |
