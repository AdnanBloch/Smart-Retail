# Mentor reviews

This page records every mentor review, newest last. Each review lists what the
mentor asked for, then a section per point. The section gives the options we
weighed, the choice made, and the reason. The RFCs carry the binding version of
each decision; this page explains where each one came from. RFC rows and
amendment logs cite a point as *"mentor review DATE §n"*, where §n is the
section number inside that date's review.

| Date | Review | Outcome |
| --- | --- | --- |
| 2026-10-06 | [Review of 2026-10-06](#review-of-2026-10-06) | `common/` package, entity-first layout with versioned API folders, a decision register in every RFC, Mermaid diagrams, coverage gate, pre-commit and CI |
| 2026-10-12 | [Review of 2026-10-12](#review-of-2026-10-12) | Why FastAPI and the rest of the stack (ARC-D28–ARC-D35); everything an entity uses under its folder; review-changed decisions traced in every RFC; diagram index per service |
| 2026-10-13 | [Review of 2026-10-13](#review-of-2026-10-13) · [full record](2026-10-13-mentor-review.md) | Version-first layout (`app/v1/<entity>/`, ARC-D23 changed); Kafka partitioning, commit, retry and dedupe explained and built; outbox relay, inventory consumer and Celery in Milestone 3; why Kafka, RabbitMQ/Celery and Temporal each exist; end-to-end business flows; one diagrams page |
| 2026-10-20 | [Review of 2026-10-20](#review-of-2026-10-20) | Reserved counter kept and proven (INV-D32, reconciliation built); where the system is asynchronous; internal calls, retries and the saga explained; 32 failure cases; ranked bottlenecks; gateway rate limit (GW-7), explicit worker concurrency (ORD-D37), shared payment dedupe (ORD-D36); race and full-flow tests across every package |

## Review of 2026-10-06

The first review, after Milestone 1.

| # | Mentor's request | Where it lands |
| --- | --- | --- |
| 1 | Turn `libs/common` into a simple package folder, because `libs/` holds only one package | [§1](#1-shared-package-folder) · RFC 0001 |
| 2, 4 | Every decision must state why it was taken, in the RFCs | [§2](#2-a-reason-for-every-decision) · §3.2.1 of every RFC |
| 3 | Organise each service by entity. Use one global route file that only references each entity's routes, with versions handled in the folder structure | [§3](#3-entity-first-service-layout-with-versions) · RFC 0000 §3.1.1.7 |
| 5 | More diagrams in each RFC: flow, sequence, relational/ER, and others | [§4](#4-diagrams) · every RFC |
| 6 | Follow standard practice even where the assignment doesn't require it, including unit tests and coverage | [§5](#5-standard-engineering-practice) · RFC 0001 |

The mentor also asked that, before each decision, we check whether there is a
better way to handle the case. Each section below does this.

### 1. Shared package folder

**Options considered**

| Option | For | Against |
| --- | --- | --- |
| **`common/` at the repo root, import name `smartretail_common` (chosen)** | One level fewer. The folder name says what it is | — |
| `packages/common/` | Leaves room for more shared packages | Same depth as today. Builds structure for packages that don't exist |
| `common/` with import name `common` | Shortest imports | `common` is a PyPI package name and a common local module name, so imports could resolve to the wrong one. Grepping for shared-library usage gets noisy |

**Why.** A `libs/` folder containing one package adds a level without meaning
anything. The distinct import name stays because it is how a reader, and the R8
grep test, can tell platform code from service code.

### 2. A reason for every decision

**Options considered**

| Option | For | Against |
| --- | --- | --- |
| Separate ADR files (`docs/adr/NNNN-*.md`) | The industry-standard format | Splits a service's design across two places again. That is the problem the per-service RFC split solved |
| **Decision register table in each RFC §3.2.1 (chosen)** | One table per RFC; *Why* is a required column, so a missing reason shows in review | Rows must be kept in step with the spec text |
| Prose rationale only (status quo) | Reads well | Covers the big decisions only. Nothing shows when a reason is missing |

**Format.** `| ID | Decision | Why | Alternatives rejected (and why) | Revisit if |`.
IDs use the RFC's prefix (`ARC-D1`, `PLT-D1`, `GW-D1`, `IDN-D1`, `CAT-D1`, …). The
existing prose rationale stays, and rows link to it rather than repeat it. The
PR template asks which decision IDs a change adds or alters.

### 3. Entity-first service layout with versions

**Options considered**

| Option | For | Against |
| --- | --- | --- |
| **Entity-first, version folder inside each entity (chosen)** | Everything about an entity sits in one folder. Only the parts that change between versions are versioned | An entity's API is one level deeper |
| Version-first: `app/v1/<entity>/` holds everything | The literal reading; simple to scan | v2 would copy the ORM models and business rules. Two versions would then define the same table twice and drift apart |
| Hybrid: `app/<entity>/` for the domain, `app/api/v1/<entity>/` for the API | Versioning is clearly separated | Each entity is split across two trees, which is the cohesion problem the mentor raised |

**The deciding argument.** A database table has one shape at a time, so the ORM
model and the business rules that guard it cannot be versioned. Only the HTTP
contract can: paths, request bodies and response bodies. So each entity has
`v1/routes.py` and `v1/schemas.py`. Its `models.py`, `repo.py` and `utils.py` sit
beside them, unversioned. v2 is a new `v2/` folder plus one line in the global
router, and both versions call the same repo.

**The global router.** `app/router.py` only includes the entity routers. It sets
the version and entity prefixes (`/api/v1` + `/products`), so every URL the
service serves can be read from one file. Handlers now use decorators. Before,
they were registered centrally because routes and handlers lived in different
files. Now they share a file, so central registration added indirection and
nothing else.

Binding version: RFC 0000 §3.1.1.7 and [file-structure.md](../file-structure.md) §4.

*Superseded by [2026-10-13 §9](#9-version-first-layout): the version folder now
comes first (`app/v1/<entity>/`), and a later version holds only the files it
changes.*

### 4. Diagrams

**Options considered**

| Option | For | Against |
| --- | --- | --- |
| **Mermaid, existing ASCII converted (chosen)** | Text in the Markdown, so it diffs and gets reviewed. Renders in GitHub, GitLab and VS Code. Supports flowcharts, sequence, ER, state and C4 diagrams | Less control over layout |
| PlantUML | Fuller C4 and layout control | Needs a render server. Doesn't render inline on GitHub |
| Keep ASCII, add Mermaid only for new diagrams | Less churn | Two styles. ASCII ER and sequence diagrams are hard to maintain |

**Minimum set.**

- **0000:** system context; service map; cross-service ER; login, publish and checkout sequences; event flow.
- **0001:** deployment topology; outbox relay sequence; consumer idempotency flowchart; CI pipeline.
- **Each service RFC:** component diagram; `erDiagram` of its tables; a `stateDiagram-v2` per state machine; a sequence per workflow and per critical endpoint; a flowchart for decision-heavy logic.

### 5. Standard engineering practice

These are not part of the assignment requirements. We adopt them because a
production codebase would have them.

| Practice | What | Why |
| --- | --- | --- |
| Unit tests + coverage gate | `pytest-cov`, branch coverage, `fail_under = 85` per package; `make cov` for the HTML report | Shows which code no test exercises, and stops coverage from shrinking quietly |
| Git + pre-commit | Git repository; hooks for ruff, ruff-format, mypy, whitespace and EOF, YAML/TOML checks, private-key detection | Catches problems before commit rather than in review |
| CI | GitHub Actions: lint → type check → tests per package against Postgres → coverage gate → `pip-audit` | Every change gets the same checks, which no longer depend on one machine |
| Repo hygiene | PR template with a **Why** section, `CONTRIBUTING.md`, `.editorconfig`, `make audit` | Asks for the reason behind each change (point 2) at the point of change |

## Review of 2026-10-12

The Milestone 2 review. Each section also starts with what was missing before
the change.

| # | Mentor's request | Where it lands |
| --- | --- | --- |
| 1 | Explain why FastAPI was chosen | [§1](#1-why-fastapi-and-the-rest-of-the-stack) · RFC 0000 ARC-D28–ARC-D35 |
| 2 | Every model, and every other file, in its entity folder | [§2](#2-every-file-in-its-entity-folder) · RFC 0000 ARC-D27 §3.1.1.7, RFC 0004 CAT-D32, RFC 0005 ORD-D34 |
| 3 | Every decision explained in the RFCs, including how and on what basis a decision changed | [§3](#3-every-change-traced-to-its-review) · RFC 0000 ARC-D36, the amendment log in §11 of every RFC |
| 4 | Flow, sequence and data diagrams for each service | [§4](#4-diagram-coverage-per-service) · a diagram index in each service RFC, RFC 0002 GW-D29 |

### 1. Why FastAPI and the rest of the stack

**What was missing.** RFC 0000 and RFC 0001 said only that Part A names the
stack. No decision row gave the reason for FastAPI, or for the libraries around
it, and none listed the alternatives.

**Options considered for the HTTP framework**

| Option | For | Against |
| --- | --- | --- |
| **FastAPI (chosen)** | Native async on ASGI (A6). Pydantic validation is the same models as the v1 schemas. OpenAPI generated from code, which `/docs` and the gateway's aggregated view use. `Depends` for session, caller and repos. Named in Part A | Smaller built-in feature set than Django. Fine for an API-only backend |
| Django + DRF | Mature; admin, auth and ORM included | Sync-first ORM and request cycle. Its ORM would compete with SQLAlchemy. Admin and templates go unused |
| Flask | Small and well known | No native async or validation; both are add-ons |
| Litestar | Similar design to FastAPI, fast | Smaller ecosystem, fewer OpenTelemetry and Temporal examples. Outside the brief |

**Why.** The platform is async all the way down (A6), validates every request
with Pydantic, and publishes OpenAPI per service. FastAPI does all three without
extra libraries. The same reasoning, with its own rejected alternatives, is
recorded for Python 3.12, SQLAlchemy 2.0 with `asyncpg`, Pydantic v2, Alembic,
`httpx`, Uvicorn and Docker Compose.

Binding version: RFC 0000 [ARC-D28–ARC-D35](../rfcs/0000-architecture.md#321-decision-register).

### 2. Every file in its entity folder

**What was missing.** Every ORM model except one was already in its entity
folder. The exceptions:

- `IdempotencyKey` was in `services/order/app/core/idempotency.py`.
- `workflows/` (catalog and order), `clients/` (catalog and order) and
  `providers/` (order) sat at `app/` level, beside the entities.

**Options considered**

| Option | For | Against |
| --- | --- | --- |
| Models only | Least churn | Leaves a second, layer-first tree (`app/workflows/`, `app/clients/`) beside the entity tree — the split ARC-D23 rejected |
| Models and workflows | Each workflow sits with the entity it changes | Clients and providers still outside |
| **Everything one entity uses, under it (chosen)** | One folder answers "what does this entity do and what does it call". Same rule everywhere | More files move once: imports, the worker start commands, tests |

**Where `IdempotencyKey` went.** Only `POST /orders` uses it, so it joined the
`orders` entity. The model is in `orders/models.py`, the claim logic in
`orders/idempotency_repo.py` and its states in `orders/enums.py`. A separate
`app/idempotency/` entity was rejected because it would be an entity with one
user. If a second endpoint needs it, it becomes its own entity.

**What stays in `core/`.** Only what two or more entities share: config, db,
logger, errors, dependencies and shared schemas. `CallerRole` stays as one small
copy per service, because a role claim is a vocabulary and PLT-D24 keeps
vocabularies out of `common`.

**Result.**

| Service | Moved from | Moved to |
| --- | --- | --- |
| order | `app/core/idempotency.py` | `app/orders/models.py` + `app/orders/idempotency_repo.py` |
| order | `app/workflows/`, `app/clients/`, `app/providers/` | `app/orders/workflows/`, `app/orders/clients/`, `app/orders/providers/` |
| catalog | `app/workflows/`, `app/clients/` | `app/products/workflows/`, `app/products/clients/` |

The Temporal workflow and activity names were already set explicitly with
`name=`, so moving the modules did not rename anything Temporal stores. The
order migration `0002_idempotency_state_comment` updates a column comment that
pointed at the old path.

Binding version: RFC 0000 [ARC-D27](../rfcs/0000-architecture.md#321-decision-register) and [§3.1.1.7](../rfcs/0000-architecture.md#3117-module-structure-inside-a-service).

### 3. Every change traced to its review

**What was missing.** Every RFC had a decision register with a *Why* column
(from the [2026-10-06 review §2](#2-a-reason-for-every-decision)).
But a reader could not tell which decisions had changed after a review, what
they were before, or on what basis they changed. Only five RFCs linked the
earlier review at all, and only RFC 0000 had an amendment log.

**Options considered**

| Option | For | Against |
| --- | --- | --- |
| Git history only | No extra writing | The reason is lost in commit messages; a reviewer has to dig |
| A separate change-log file | One place for all changes | A third place to read, beside the RFC and the review |
| **Mark the row and log the change in the RFC (chosen)** | The basis is next to the decision it changed | Rows grow a sentence longer |

**The rule.** When a review changes a decision:

1. The register row's *Why* ends with *"Changed after [mentor review DATE §n](…)
   — previously …"*. A decision that is new because of a review says *"Added
   after …"*.
2. The RFC's §11 amendment log gets a row: date, section, change, and the review
   it is based on.
3. The review record in `docs/reviews/` explains the options and the choice.

Every RFC now has an amendment log, back-filled with the 2026-10-06 changes.

Binding version: RFC 0000 [ARC-D36](../rfcs/0000-architecture.md#321-decision-register).

### 4. Diagram coverage per service

**What was found.** The 2026-10-06 minimum set (§4 there) was met for identity,
catalog, order and inventory: each has flowcharts, sequence diagrams, state
diagrams and an `erDiagram`. The gateway had flow and sequence diagrams but no
data diagram, because it owns no tables.

**What changed.**

- Each service RFC (0002–0009) gets a **diagram index** at the start of §3.1, so
  a reviewer can check flow, sequence, state and data coverage at a glance.
- RFC 0002 gets a data diagram: the request and token data the gateway reads and
  writes, and its in-memory and Redis state. It is stated there that the gateway
  owns no tables.

Binding version: the diagram index in each service RFC; RFC 0002 [GW-D29](../rfcs/0002-gateway-service.md#321-decision-register).

## Review of 2026-10-13

A review of the messaging design, ahead of Milestone 3. The mentor asked for
answers that can be defended in a discussion, not only RFC rows. The answers are
gathered on three new pages. The RFCs stay the binding version.

| # | Mentor's request | Where it lands |
| --- | --- | --- |
| 1 | Kafka partitioning: which method we use | [§1](#1-kafka-partitioning-method) · [messaging §1](../messaging-and-consistency.md#1-partitioning) |
| 2 | Kafka commit and retry: on what basis | [§2](#2-offset-commit-and-retry-basis) · [messaging §2](../messaging-and-consistency.md#2-offset-commit-and-retry) |
| 3 | A duplicate event caused by a retry: how it is handled | [§3](#3-duplicate-events) · [messaging §3](../messaging-and-consistency.md#3-duplicates) |
| 4 | How atomicity is maintained | [§4](#4-atomicity-across-database-broker-and-services) · [messaging §4](../messaging-and-consistency.md#4-atomicity) |
| 5 | Understand the complete business logic | [§5](#5-complete-business-logic) · [business-flows.md](../business-flows.md) |
| 6 | A top-level diagram of how services communicate, and a diagram per service of how its components work together | [§6](#6-top-level-and-per-service-architecture-diagrams) · [architecture-diagrams.md](../architecture-diagrams.md) |
| 7 | Why both RabbitMQ and Kafka, what Celery is for, and the basis for using each | [§7](#7-why-kafka-rabbitmq-celery-and-temporal) · [messaging §6](../messaging-and-consistency.md#6-why-each-tool) |
| 8 | Queue methodology: how it works | [§8](#8-queue-methodology) · [messaging §5](../messaging-and-consistency.md#5-how-each-queue-works) |
| 9 | Versioning at the top level, before the entity; a later version holds only the files it changes and imports the rest, such as models, from v1 | [§9](#9-version-first-layout) · [2026-10-13-mentor-review.md §1](2026-10-13-mentor-review.md#1-version-first-layout) · RFC 0000 ARC-D23 |

**What was missing.** Most of the answers already existed as decisions in RFC
0000 (A10–A12, ARC-D11–D13) and RFC 0001 (PLT-D1, D5, D6, D8, D10), but they
were spread over two long documents. None of Kafka, RabbitMQ or Celery was built
yet: outbox rows were written, but nothing published them. The answers could be
read, but not shown working.

**What changed.**

- Three pages that answer the review directly:
  - [messaging-and-consistency.md](../messaging-and-consistency.md)
  - [business-flows.md](../business-flows.md)
  - [architecture-diagrams.md](../architecture-diagrams.md)
- The Milestone 3 messaging layer is built rather than only described:
  - the Kafka + Schema Registry topics;
  - the `outbox-relay`;
  - the `EventConsumer` base with `processed_events` dedupe;
  - the `inventory` consumer;
  - RabbitMQ + Celery with the reservation sweep and `resume_unstarted_orders`.

  Where the code departs from RFC 0001, the RFC is amended and the amendment cites
  this review.

### 1. Kafka partitioning method

**Options considered**

| Option | For | Against |
| --- | --- | --- |
| **Key hash, key = aggregate id (chosen)** | Every event for one product, order or variant is on one partition, so it is consumed in order. Different entities run in parallel | A very hot single entity cannot be spread. No such entity exists here |
| Round-robin, no key | Even load | No ordering: `order.cancelled` could be handled before `order.created` |
| Custom partitioner by merchant | Groups one merchant's events together | One large merchant becomes a hot partition. No rule needs per-merchant order |

**Why.** Every ordering rule in the domain is per entity. Three partitions per
topic make the multi-partition path real, even with one local broker. Binding:
RFC 0000 C3 and ARC-D13; RFC 0001 PLT-D8.

### 2. Offset commit and retry basis

**Options considered**

| Option | For | Against |
| --- | --- | --- |
| Auto-commit on a timer | Nothing to write | A crash after the timer fires but before the database commit loses the event |
| **Manual commit after the effect's transaction commits (chosen)** | A crash can only cause a redelivery, and dedupe absorbs that | One commit call per message. Acceptable at this volume |
| Commit before processing (at-most-once) | Never a duplicate | Loses events on any crash |

**Retry basis.** The decision depends on what failed, not on a fixed count alone:

- A message that cannot be read goes to the DLQ at once.
- `DependencyUnavailable` pauses the partition and retries without counting an
  attempt.
- Any other error is retried 3 times with exponential backoff, then goes to the
  DLQ, and the offset is committed.

Binding: RFC 0001 PLT-D5 and PLT-D6.

### 3. Duplicate events

**Options considered**

| Option | For | Against |
| --- | --- | --- |
| **At-least-once + `processed_events (consumer_group, event_id)` in the effect's transaction (chosen)** | Covers every source of duplicates: relay restart, consumer restart, DLQ replay | One extra row per event, swept after 30 days |
| Kafka exactly-once (transactions) | Built in | Covers Kafka → Kafka only. Our effects are Postgres writes |
| Rely on each handler being naturally idempotent | No table | Easy to get wrong, and impossible to check in review |

**Why.** The dedupe row and the effect commit together, so "done" and "recorded
as done" can never disagree. The other layers that already run are listed in
[messaging §3](../messaging-and-consistency.md#3-duplicates): Idempotency-Key,
`REJECT_DUPLICATE`, provider keys and unique reservations. Binding: RFC 0000 A11
and ARC-D12.

### 4. Atomicity across database, broker and services

**Options considered**

| Option | For | Against |
| --- | --- | --- |
| **Transactional outbox, plus Temporal sagas across services (chosen)** | The state change and its event are one local transaction. Cross-service steps compensate on failure | Event latency of one relay poll, about 500 ms |
| Dual write (commit, then publish) | Simple | Loses the event on a crash between the two |
| Two-phase commit | Atomic in theory | Kafka and Temporal do not take part; it couples every service's availability |
| CDC (Debezium) | No polling | A connector cluster to run. Kept as the upgrade path |

**Why.** See [messaging §4](../messaging-and-consistency.md#4-atomicity). The one
real gap was an order committed without its workflow started. It is closed by
`resume_unstarted_orders` on Celery Beat (RFC 0005 ORD-9). Binding: RFC 0000 A10
and ARC-D12; RFC 0001 PLT-D1.

### 5. Complete business logic

[business-flows.md](../business-flows.md) walks through each flow from start to
finish. Each step names its owning service, the state change, the event, and
every failure and compensation branch:

- merchant onboarding;
- product publishing;
- browse and assistant;
- the checkout saga;
- cancellation;
- reservation expiry;
- post-order events.

The failure table is taken from the branches in `order_workflow.py`, so it
matches the code.

### 6. Top-level and per-service architecture diagrams

**What was found.** A top-level diagram existed (RFC 0000 and RFC 0001), and
every service RFC had a component diagram. But the top-level diagram did not
label protocols, and the per-service diagrams were spread over eight RFCs.

**What changed.**
[architecture-diagrams.md](../architecture-diagrams.md) holds the following:

- one top-level diagram with the protocol on every edge (HTTP, SSE, gRPC, Kafka,
  AMQP, SQL, S3);
- the shared internal layout;
- one component diagram per service, drawn from the real modules.

RFC 0000's target-topology diagram gained the same protocol labels.

### 7. Why Kafka, RabbitMQ, Celery and Temporal

**Options considered**

| Option | Against |
| --- | --- |
| RabbitMQ for everything | A consumed event is deleted: no replay, no new consumer reading history, no schema registry, and the producer must know its consumers |
| Kafka for everything | No per-task ack, retry or time limit. One slow job blocks its partition. No scheduler |
| Celery chains instead of Temporal | No durable state. A worker crash mid-saga leaves a charged order with nothing to refund it |
| **Each tool for its own question (chosen)** | Three systems to run. Each has a job the other two do badly |

**The basis.**

- A **fact** other services may react to goes to Kafka.
- A **multi-step process** that must complete or compensate goes to Temporal.
- A **single job** off the request path, or on a schedule, goes to Celery over
  RabbitMQ.

Binding: RFC 0000 A12 and ARC-D11; RFC 0001 PLT-D10.

### 8. Queue methodology

- **Kafka is a log.** Messages are kept and each consumer group keeps its own
  offset, so fan-out and replay come at no cost, with order kept per partition.
- **RabbitMQ is a work queue.** Each message goes to one worker and is deleted
  on ack. Tasks are acked late, so a killed worker's task is redelivered.
  Prefetch is 1, and each workload has its own durable queue.
- **Temporal task queues** dispatch the next step of a durable workflow.

The diagrams are in
[messaging §5](../messaging-and-consistency.md#5-how-each-queue-works).

### 9. Version-first layout

The full record, with the mentor's words, the options and the rules that come
with the layout, is in
[2026-10-13-mentor-review.md §1](2026-10-13-mentor-review.md#1-version-first-layout).

In short, every entity folder moved to `app/v1/<entity>/`, and the old
`<entity>/v1/{routes,schemas}.py` became `app/v1/<entity>/{routes,schemas}.py`.
A later version is a folder holding only the files it changes: the handlers,
the schemas, and a repo when the business rules change. It imports the rest
from v1 by name, so models are never copied.

This reverses the [2026-10-06 §3](#3-entity-first-service-layout-with-versions)
choice of version-inside-entity. That choice could not hold a per-version
business rule, which is the case ARC-D23 had named as its reason to revisit.

Binding: RFC 0000 ARC-D23 and §3.1.1.7;
[file-structure.md §4](../file-structure.md#4-service-layout-every-service).

## Review of 2026-10-20

A review of distributed-systems behaviour after Milestone 3. The mentor's
concerns:

- the stock row and the reservation table look like two sources of truth;
- the system *feels* synchronous, especially orders;
- what happens when many orders arrive at once.

The mentor asked for failure cases to be thought through in advance, and for
much deeper testing. The long-form answers are on one new page,
[distributed-systems-and-failure-modes.md](../distributed-systems-and-failure-modes.md).
This section records the options and choices.

| # | Mentor's request | Where it lands |
| --- | --- | --- |
| 1 | `stock_items.reserved` and the `reservations` table both hold reservations: a duplicate source of truth? Explore and review the decision | [§1](#1-reserved-counter-versus-reservation-rows) · [DS §7](../distributed-systems-and-failure-modes.md#7-the-reserved-counter-and-the-reservation-rows) · RFC 0006 INV-D32, INV-D23 |
| 2 | Where is the distributed and asynchronous logic? Orders feel synchronous, with no queue when many arrive at once; future bottlenecks | [§2](#2-where-the-system-is-asynchronous) · [DS §1](../distributed-systems-and-failure-modes.md#1-where-the-system-is-synchronous-and-where-it-is-not) · RFC 0005 ORD-D37, RFC 0002 GW-7 |
| 3 | Internal endpoint calls: asynchronous or not? | [§3](#3-internal-endpoint-calls) · [DS §2](../distributed-systems-and-failure-modes.md#2-how-services-call-each-other) |
| 4 | How retries are handled within the system | [§4](#4-retry-handling-across-layers) · [DS §3](../distributed-systems-and-failure-modes.md#3-retries-layer-by-layer) |
| 5 | Understand the saga pattern in detail | [§5](#5-the-saga-pattern-explained) · [DS §4](../distributed-systems-and-failure-modes.md#4-the-saga-pattern) |
| 6 | Possible failure cases in this kind of system, and how ours handles each | [§6](#6-failure-case-questions) · [DS §5](../distributed-systems-and-failure-modes.md#5-failure-cases-and-how-each-is-handled) |
| 7 | Scalability bottlenecks | [§7](#7-scalability-bottlenecks-and-the-fixes-made) · [DS §6](../distributed-systems-and-failure-modes.md#6-scalability-bottlenecks) · RFC 0005 ORD-D36, ORD-D37 |
| 8 | Unit tests that run smoothly, code coverage, complete-flow tests, race-condition tests, edge cases and the other complex cases | [§8](#8-test-depth-race-conditions-and-full-flows) |

**What was missing.**

- **Point 1.** The design held, but its reason did not. No decision row
  explained why the counter is kept, and the reconciliation job that is meant
  to prove I5 was designed in RFC 0006 but never built.
- **Point 2.** The asynchrony was real: checkout returns `202` and the saga
  runs on a Temporal task queue. But it was not explained in one place.
- **Point 7.** Three gaps were real:
  - the gateway's designed rate limit (GW-7) was not built;
  - the order worker ran on unexamined SDK concurrency defaults;
  - the fake payment provider deduped charges in process memory, so a second
    worker replica, or a restart, could charge twice. Scaling the worker out,
    the obvious answer to bursts, was unsafe.

**What changed.**

- **Code:**
  - `reconcile_inventory_ledger` is built and scheduled daily (INV-D23).
  - The gateway enforces GW-7 rate limits, including `order_create` at 30 per
    user per minute; it fails open.
  - Worker concurrency is explicit and configurable (ORD-D37).
  - Payment dedupe lives in a shared `payment_attempts` table (ORD-D36), so
    `--scale order-worker=N` is safe.
- **Tests:**
  - race tests for every conflicting pair of stock operations;
  - replica races for payments;
  - burst and fail-open tests for the rate limit;
  - workflow branch tests;
  - a pass over every package (§8).
- **Docs:**
  - the new [distributed-systems-and-failure-modes.md](../distributed-systems-and-failure-modes.md);
  - RFC rows INV-D32, ORD-D36, ORD-D37 and GW-7's status;
  - `business-flows.md` corrected (checkout returns `202`, not `201`).

### 1. Reserved counter versus reservation rows

**Options considered**

| Option | For | Against |
| --- | --- | --- |
| **Keep `stock_items.reserved` as a counter of held reservations, updated in the same locked transaction, and prove it with a daily reconciliation (chosen)** | `available = on_hand − reserved` stays a stored, indexable column. The database `CHECK (on_hand >= reserved)` stops oversell even if application code is wrong. Reads never scan reservations | Two places change on every write. Closed by one repository owning every write, one transaction, I5 checked in tests, and the reconciliation job |
| Reservation rows only; compute `SUM(held)` when needed | One source; nothing can drift | The stock-row lock is still needed to stop two reserves both seeing room, so the main cost stays. The `CHECK` cannot cover a sum over another table, so the oversell guard moves into code. Every availability read becomes an aggregate |
| A trigger keeps the counter | Code cannot forget the counter | Hidden logic; a second lock path outside the canonical lock order (INV-D7) |
| Ledger-only event sourcing | One source | Every read folds history (already rejected as INV-D2's alternative) |

**Why.** The two are not equal sources. The reservation rows say *who* holds
*what*. The counter is their total, kept for a database-enforced guarantee.
Dropping the counter keeps the hard part, the row lock, and loses the strongest
guard, the `CHECK`. The risk the mentor saw is drift, and drift is now detected
daily by `reconcile_inventory_ledger`. It never auto-corrects (INV-D23). It
was designed but not built; it is built now (`services/inventory/app/v1/stock/reconcile.py`).
Every race test also ends by asserting there is no drift.

Binding: RFC 0006 INV-D32 (new), INV-D23, I5.

### 2. Where the system is asynchronous

**Options considered**

| Option | For | Against |
| --- | --- | --- |
| Run the saga inside the request and return `201 confirmed` | Simple for the client | The request waits on stock, payment and shipping. A burst holds hundreds of open requests and DB connections. A crash mid-request loses the order's progress |
| Put orders on a Kafka or RabbitMQ queue and process them with consumers | A visible queue | We would rebuild Temporal: per-order state, timers, retries, compensation and resumption after a crash |
| **Accept with `202 pending`, then schedule a Temporal workflow; the task queue `order-lifecycle` is the order queue (chosen; this is what was built)** | Accepting is cheap and bounded. Work is queued durably and drained at a configured concurrency. A crashed worker resumes from history | A dependency on Temporal (single node here; [DS §6](../distributed-systems-and-failure-modes.md#6-scalability-bottlenecks) B2) |

**Why.** The queue the mentor looked for exists; it is Temporal's. A burst
becomes many accepted rows and scheduled workflows. They wait on
`order-lifecycle` and are pulled at each replica's limit. Two things in this
review make that controllable:

- **ORD-D37**: explicit `max_concurrent_activities` and
  `max_concurrent_workflow_tasks` per worker replica, set from settings.
- **GW-7**: a per-user `order_create` limit at the edge, so one client cannot
  fill the queue.

The walk-through of 500 simultaneous orders is in
[DS §1.3](../distributed-systems-and-failure-modes.md#13-what-a-burst-looks-like).

### 3. Internal endpoint calls

`/internal/v1/…` routes are plain HTTP. They are asynchronous with respect to
the *user*: only Temporal activities and workflows call them, never a request
a person is waiting on. Inside the activity the call is synchronous, because
the saga needs the answer (reservation IDs, or "no stock") before it charges
a card.

| Option | For | Against |
| --- | --- | --- |
| **Synchronous HTTP from an activity, a service token, idempotent targets (chosen)** | The answer is in hand for the next step. Temporal owns retry and backoff. Inventory dedupes by `(order_id, variant_id)` | Inventory must be reachable at that moment; if not, the activity waits and retries |
| Request/reply over Kafka | Fully decoupled | Needs a correlation ID, reply topic, timeout and matching state, which is HTTP rebuilt badly. Kafka is kept for facts, not commands (A12) |
| Fire-and-forget command | No waiting | The saga could not know whether stock was reserved before charging |

The table of every caller, its auth, timeout and retry is in
[DS §2](../distributed-systems-and-failure-modes.md#2-how-services-call-each-other).

### 4. Retry handling across layers

Retries happen at six layers:

1. the client (with an `Idempotency-Key`);
2. inventory's own lock-conflict retry;
3. Temporal activity policies, per step;
4. Kafka consumers (3 attempts, then DLQ);
5. the outbox relay (until acknowledged);
6. Celery autoretry.

Every layer retries only an operation whose target dedupes it, so a retry can
never apply something twice. The per-step Temporal table and the rules behind
it are in [DS §3](../distributed-systems-and-failure-modes.md#3-retries-layer-by-layer):

- business refusals are never retried;
- the forward path fits inside the reservation TTL;
- compensations retry for an hour.

One RFC sentence was wrong: RFC 0005 §6.1 said a network blip is retried "up
to three" times. Attempts vary by step (5 for reserve, unlimited for commit),
and the RFC now points to the table.

### 5. The saga pattern explained

[DS §4](../distributed-systems-and-failure-modes.md#4-the-saga-pattern) covers:

- what a saga is (local transactions plus compensations instead of 2PC);
- compensatable, pivot and retriable steps;
- orchestration versus choreography, and why we orchestrate on Temporal;
- our forward steps and compensations, as a diagram and a table;
- the hard cases: the ambiguous charge timeout, cancel racing the pivot,
  operator termination, a refund that keeps failing;
- the isolation anomalies a saga allows, and our countermeasure for each.

Binding: ARC-D11, ORD-D1, ORD-10.

### 6. Failure-case questions

[DS §5](../distributed-systems-and-failure-modes.md#5-failure-cases-and-how-each-is-handled)
lists 32 failure cases (F1–F32). Each gives what happens, the mechanism, and
the test that proves it. Examples:

- a double click;
- the last-unit race;
- inventory, Temporal, Kafka, Redis, identity, catalog or Postgres being down;
- a worker crash after the charge;
- two replicas running one charge;
- relay and consumer crashes;
- poison messages;
- sweep-versus-commit and cancel-versus-commit races;
- counter drift;
- a flash sale on one product;
- a network split mid-reserve.

### 7. Scalability bottlenecks and the fixes made

The ranked list (B1–B11) is in
[DS §6](../distributed-systems-and-failure-modes.md#6-scalability-bottlenecks).
For each bottleneck it gives the current mitigation, the next step, and the
metric that would trigger it. Three were fixed now:

| Gap | Options | Chosen | Binding |
| --- | --- | --- | --- |
| A second order-worker replica, or a restart, could charge twice: the fake provider deduped in memory | A sticky single worker (no scale-out); Redis dedupe (not durable, RFC 0001 §3.1.1.8); **a `payment_attempts` table in `order_db`, first writer wins (chosen)** | Shared and durable, in the service's own DB; `INSERT … ON CONFLICT` counts attempts atomically | ORD-D36, migration `0006_payment_attempts` |
| Worker concurrency was the SDK default, unexamined | Leave the defaults; **explicit settings per replica, scale out by replicas (chosen)** | Throughput is configuration; each replica's DB pool stays within its limit | ORD-D37 |
| No edge rate limit, so one client could flood checkout | Per-service limits (cannot span replicas or services); **GW-7 fixed window in Redis, fail open (chosen, as designed)** | Shared by gateway replicas; a Redis outage never blocks orders | GW-7, GW-D11–D13 |

`login_email` (GW-8) is the one GW-7 class still not built; it needs the login
body read before forwarding. `login_ip` bounds login attempts in the meantime.

### 8. Test depth, race conditions and full flows

**What was added**

| Kind | Where |
| --- | --- |
| Race conditions on one row | inventory `test_races.py`: commit vs release, commit vs expiry sweep, saga release vs `order.cancelled` consumer, reverse vs reverse, adjust and set vs reserve, the last unit among 20 orders, mixed line order over 5 SKUs. Each ends with no ledger drift |
| Reconciliation | inventory `test_reconcile.py`: clean after real traffic; injected drift reported and not corrected; the Celery task |
| Replicas and restarts | order `test_payment_store.py`: two replicas charging at once, flaky attempts counted across replicas, a restart sees the earlier charge, concurrent refunds |
| Bursts at the edge | gateway `test_ratelimit.py`: 20 concurrent orders from one user (3 admitted); per-subject isolation, window reset, per-IP before auth, stream exemption, fail-open |
| Saga branches | order workflow tests with stub activities: every compensation path and the cancel window |
| Every other package | Unit, edge and race tests for the files the coverage report showed as least covered |

**Bugs the new tests found, and fixed.** All seven are concurrency or retry
bugs. None showed up in the single-request tests that existed before.

| Service | Bug | Fix |
| --- | --- | --- |
| order | `OrderRepo.advance` rolled back on an idempotent no-op, which expired the loaded order. A retried status write whose first attempt had committed then crashed on every retry. After 300 s the saga compensated, so a **successful order could be refunded and its stock reversed** | Commit (release the lock) instead of rolling back; two regression tests |
| identity | Replaying an already-rotated refresh token while its successor was being rotated left the new session alive, so **reuse detection could be bypassed** (13 of 15 probe runs) | Lock the user row before the session row, so one user's refreshes run one at a time |
| identity | Two merchants registering the same business name at once got a 500 (slug clash). A slug containing "email" was misreported as email-taken | Match on constraint names; retry the slug |
| identity | Two admins demoting each other at once deadlocked; the loser got 500 instead of `409 last-admin` | Lock the admin rows in id order before the target |
| catalog | Deleting a category while a product was created in it, or while a child category was created under it, returned 500 (raw foreign-key error) | Map the foreign key to `409 category-in-use` / `422` unknown reference |
| notification | Releasing a stream slot decremented and deleted the counter in two Redis commands, so a claim landing between them was wiped and the **3-streams-per-user cap drifted open** (198 of 200 rounds) | One atomic Lua script |
| assistant | A reindex read `index_state` before locking it, so a newer catalog event could be overwritten by stale chunks, and an unpublished product could be re-indexed | Lock the row on read; skip removed products |

**Found and left open, for a decision.**

- The assistant token budget is checked and charged in separate steps. Concurrent
  requests from one user can overspend by the in-flight requests' cost. No
  charge is ever lost, but the docstring promises at most "one request's
  spend". The fix is either to correct the docstring or to use a conditional
  charge (AST-D34).
- When duplicate carrier updates arrive at once, later ones can report
  `applied: true` although they changed nothing. History and events still
  record the change once; only the response flag is wrong.

**Coverage (per package, branch coverage; the gate is 85 %)**

| Package | Tests before | Tests after | Coverage before | Coverage after |
| --- | --- | --- | --- | --- |
| common | 138 | 234 | 95.0 % | 99.4 % |
| identity | 53 | 115 | 90.8 % | 98.4 % |
| catalog | 114 | 204 | 93.5 % | 98.8 % |
| gateway | 61 | 77 | 93.0 % | 93.6 % (new rate-limit code included) |
| inventory | 63 | 79 | 99.6 % | 99.6 % (new reconcile code included) |
| order | 133 | 223 | 93.4 % | 98.5 % (`order_workflow.py` 88 % → 99 %) |
| notification | 143 | 176 | 97.5 % | 99.8 % |
| analytics | 123 | 127 | 99.8 % | 99.9 % |
| assistant | 111 | 162 | 97.3 % | 99.0 % |
| **e2e** (`tests/e2e`, full stack) | 24 | 28 | — | all pass |

Every new race test was run at least five times in a loop without a failure.
The lines still uncovered are process entry points, wiring that needs a live
Temporal, and defensive branches the schema makes unreachable; the agents'
notes on each are in the PR description.

**How to run**

- `make test-db test-mongo && make test` runs every package. `make cov` adds
  the coverage report.
- `make up migrate seed && make test-e2e` runs the full flows through the
  real stack.
