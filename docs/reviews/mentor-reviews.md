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
