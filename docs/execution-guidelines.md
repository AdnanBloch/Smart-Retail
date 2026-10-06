# SmartRetail — Execution Guidelines

## Contents

1. [Objective of This Assignment](#1-objective-of-this-assignment)
2. [Execution Approach](#2-execution-approach)
   - Module-Based Progression
   - Weekly Milestone Flow
   - Mentor Collaboration
3. [Submission Guidelines](#3-submission-guidelines)
   - GitHub Repository
   - README
   - Technical Documentation
4. [Learning Expectations](#4-learning-expectations)
   - Part A (Core Platform)
   - Part B (GenAI Layer)
5. [Part A — Execution Plan (3 Weeks)](#5-part-a--execution-plan-3-weeks)
   - 🟢 Week 1 — Foundation + Core Services
   - 🟡 Week 2 — Orders + Publishing Workflow
   - 🔴 Week 3 — Event-Driven System + Observability
   - Part A — Weekly Tracking Table
6. [Part B — Execution Plan (2 Weeks)](#6-part-b--execution-plan-2-weeks)
   - 🟣 Week 4 — Data Preparation + Retrieval
   - 🔵 Week 5 — AI Assistant + Streaming
   - Part B — Weekly Tracking Table
7. [Final Deliverables Checklist](#7-final-deliverables-checklist)
8. [Evaluation Approach](#8-evaluation-approach)

---

## 1. Objective of This Assignment

This assignment is designed as a structured learning journey for engineers. The intention is not only to build features, but to:

- Understand distributed commerce systems
- Learn event-driven architecture patterns
- Design for reliability and scalability
- Gain hands-on workflow orchestration experience
- Apply GenAI in real-world business use-cases
- Improve engineering decision-making skills

---

## 2. Execution Approach

### Module-Based Progression

The assignment is divided into weekly learning modules. To gain maximum value:

- Complete each module in sequence
- Share progress regularly with your mentor
- Incorporate feedback before the next milestone
- Strengthen foundations before moving to advanced modules

### Weekly Milestone Flow

Each week should ideally follow this cycle:

1. Work on the assigned scope
2. Submit the implementation for review
3. Discuss design choices
4. Improve based on feedback
5. Continue to the next milestone

### Mentor Collaboration

During reviews:

- Walk through architecture decisions
- Explain tradeoffs
- Highlight blockers faced
- Ask questions openly

Reviews should focus on:

- System thinking
- Design clarity
- Tool understanding
- Quality of implementation

---

## 3. Submission Guidelines

### GitHub Repository

Maintain:

- Clean project structure
- Logical service separation
- Meaningful commits
- Good code organization

### README

Include:

- Project overview
- Architecture diagram
- Setup instructions
- API overview
- Technology stack used

### Technical Documentation

Include:

- Service/module breakdown
- Data flows
- Event flows
- Key design decisions
- Assumptions and tradeoffs

---

## 4. Learning Expectations

### Part A (Core Platform)

Focus on:

- Event-driven architecture
- Idempotency
- Inventory consistency
- Distributed workflows
- Async systems
- Observability basics

### Part B (GenAI Layer)

Focus on:

- Embeddings and vector search
- Retrieval-Augmented patterns
- Prompt engineering
- Response quality evaluation
- Streaming and latency handling

---

## 5. Part A — Execution Plan (3 Weeks)

**Focus:** Commerce Core + Distributed Systems

### 🟢 Week 1 — Foundation + Core Services

#### Scope

- Initial architecture and project setup
- Product management APIs
- User roles and auth basics
- Database schema design
- Local environment setup

#### Deliverables

- Working APIs
- Database schema ready
- Docker local setup running
- Clean service structure established

### 🟡 Week 2 — Orders + Publishing Workflow

#### Scope

- Product publishing workflow using Temporal
- Order creation flow
- Inventory reservation logic
- Duplicate order prevention
- State transitions and validations

#### Deliverables

- Stable order workflow
- Product publishing orchestration ready
- Reliable inventory reservation flow

### 🔴 Week 3 — Event-Driven System + Observability

#### Scope

- Kafka event integration
- Celery workers for background tasks
- Notifications flow
- Basic analytics metrics
- Logs, tracing, dashboards

#### Deliverables

- Event-driven workflows operational
- Monitoring dashboards ready
- Distributed tracing enabled

### Part A — Weekly Tracking Table

| Week | Focus Area | What to Aim For | Done |
| --- | --- | --- | --- |
| Week 1 | Foundation & Core Services | Working APIs, schema, setup | ☐ |
| Week 2 | Orders & Workflows | Stable ordering + publish workflow | ☐ |
| Week 3 | Events & Observability | Async processing + visibility | ☐ |

---

## 6. Part B — Execution Plan (2 Weeks)

**Focus:** GenAI + Intelligent Commerce Layer

### 🟣 Week 4 — Data Preparation + Retrieval

#### Scope

- Product chunking pipeline
- Embedding generation
- Vector database integration
- Semantic retrieval system

#### Deliverables

- Searchable product embeddings
- Relevant retrieval pipeline working

### 🔵 Week 5 — AI Assistant + Streaming

#### Scope

- Customer shopping assistant
- Product compare assistant
- Merchant content generator
- Streaming responses
- AI metrics and monitoring

#### Deliverables

- Functional AI assistant
- Context-aware responses
- Smooth user experience flow

### Part B — Weekly Tracking Table

| Week | Focus Area | What to Aim For | Done |
| --- | --- | --- | --- |
| Week 4 | Retrieval Layer | Indexed catalog + semantic retrieval | ☐ |
| Week 5 | AI Commerce Layer | Assistant + generated content tools | ☐ |

---

## 7. Final Deliverables Checklist

Before completion, ensure:

- [ ] All milestones completed and reviewed
- [ ] Codebase is clean and maintainable
- [ ] README and docs complete
- [ ] Key workflows tested end-to-end
- [ ] You can clearly explain architecture decisions

---

## 8. Evaluation Approach

Evaluation will be based on:

- Architecture clarity
- Code quality and maintainability
- Correct use of the defined tech stack
- Reliability and failure handling
- Observability maturity
- Depth of engineering understanding
