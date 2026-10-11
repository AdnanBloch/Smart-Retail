# SmartRetail — Milestones

> Path framing: [Learning Path](learning-path.md) · Weekly plan and evaluation: [Execution Guidelines](execution-guidelines.md)
> What gets built — [Part A](part-a.md) · [Part B](part-b.md)

## Milestone Overview

| # | Milestone | Phase | Week |
| --- | --- | --- | --- |
| 1 | Foundation & Core Services | Part A | Week 1 |
| 2 | Core Business Logic | Part A | Week 2 |
| 3 | Events & Observability | Part A | Week 3 |
| 4 | Part A Review | Part A | After Week 3 |
| 5 | Retrieval Layer | Part B | Week 4 |
| 6 | AI Assistant | Part B | Week 5 |
| 7 | Part B Review | Part B | After Week 5 |

> **Note on terminology.** The source milestone list was written with course/student/instructor wording. It has been mapped to SmartRetail's domain: course → product, student/instructor → customer/merchant, instructor content generation → merchant content generation. Milestone structure and expected outcomes are unchanged.

---

## Milestone 1 — Foundation & Core Services

**Status:** delivered — gateway, identity, catalog, `common`, the local stack.

### Scope

- Initial system design and setup
- User & product management (basic CRUD)
- RBAC handling (customer / merchant)

### Expected Outcome

- Service structure defined
- Database schema created
- Basic APIs working
- Local setup ready

---

## Milestone 2 — Core Business Logic

**Status:** delivered — the product publishing workflow and the order saga on
Temporal, inventory reservation, idempotent checkout, the state machines, and
the `workflow` Compose profile. What was built and where it departs from the
RFCs: [README](../README.md#milestone-2-decisions-and-deviations).

### Scope

- Core business logic of the platform
- End-to-end implementation of the assignment

> The source text for this milestone is broad. The concrete scope was taken
> from Week 2 of the [Execution Guidelines](execution-guidelines.md) — product
> publishing workflow using Temporal, order creation, inventory reservation,
> duplicate order prevention, and state transitions — and from sections 2 and 3
> of [Part A](part-a.md).

### Expected Outcome

- Implementation of core business logic
- Workflow orchestration integrated

---

## Milestone 3 — Events & Observability

### Scope

- Kafka integration for events
- Background workers (Celery)
- Basic analytics metrics
- Observability setup (logs, tracing, metrics)

### Expected Outcome

- Event-driven flows working
- Analytics pipeline initialized
- Basic monitoring and tracing available

---

## Milestone 4 — Part A Review

### Scope

- Detailed review of design, architecture, and implementation completed in Weeks 1, 2, and 3
- Documentation / artifacts review

---

## Milestone 5 — Retrieval Layer

### Scope

- Content chunking
- Embedding generation
- Vector database integration
- Retrieval pipeline

### Expected Outcome

- Content indexed for semantic search
- Relevant data retrieval working

---

## Milestone 6 — AI Assistant

### Scope

- Contextual Q&A system
- Merchant content generation (summaries, promotional copy)
- Streaming or non-blocking responses

### Expected Outcome

- Functional AI assistant
- Context-aware responses
- Smooth user interaction flow

---

## Milestone 7 — Part B Review

### Scope

- Detailed review of implementation completed in Weeks 4 and 5

---

## Milestone Tracking Table

| # | Milestone | Expected Outcome | Done |
| --- | --- | --- | --- |
| 1 | Foundation & Core Services | Service structure, schema, basic APIs, local setup | ☑ |
| 2 | Core Business Logic | Core logic implemented, workflow orchestration integrated | ☐ |
| 3 | Events & Observability | Event-driven flows, analytics pipeline, monitoring and tracing | ☐ |
| 4 | Part A Review | Design, architecture, implementation and docs reviewed | ☐ |
| 5 | Retrieval Layer | Content indexed, retrieval working | ☐ |
| 6 | AI Assistant | Functional assistant, context-aware responses | ☐ |
| 7 | Part B Review | Weeks 4–5 implementation reviewed | ☐ |
