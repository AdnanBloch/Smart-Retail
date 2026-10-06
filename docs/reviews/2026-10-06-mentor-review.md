# Mentor review — week of 2026-10-06

This records the changes the mentor asked for in the review of 2026-10-06. For
each point it gives the options we weighed, the choice made, and the reason.
The RFCs carry the binding version of each decision. This page explains where
each one came from.

| # | Mentor's request | Where it lands |
| --- | --- | --- |
| 1 | Turn `libs/common` into a simple package folder, because `libs/` holds only one package | [§1](#1-shared-package-folder) · RFC 0001 |
| 2, 4 | Every decision must state why it was taken, in the RFCs | [§2](#2-a-reason-for-every-decision) · §3.2.1 of every RFC |
| 3 | Organise each service by entity. Use one global route file that only references each entity's routes, with versions handled in the folder structure | [§3](#3-entity-first-service-layout-with-versions) · RFC 0000 §3.1.1.7 |
| 5 | More diagrams in each RFC: flow, sequence, relational/ER, and others | [§4](#4-diagrams) · every RFC |
| 6 | Follow standard practice even where the assignment doesn't require it, including unit tests and coverage | [§5](#5-standard-engineering-practice) · RFC 0001 |

The mentor also asked that, before each decision, we check whether there is a
better way to handle the case. Each section below does this.

## 1. Shared package folder

**Options considered**

| Option | For | Against |
| --- | --- | --- |
| **`common/` at the repo root, import name `smartretail_common` (chosen)** | One level fewer. The folder name says what it is | — |
| `packages/common/` | Leaves room for more shared packages | Same depth as today. Builds structure for packages that don't exist |
| `common/` with import name `common` | Shortest imports | `common` is a PyPI package name and a common local module name, so imports could resolve to the wrong one. Grepping for shared-library usage gets noisy |

**Why.** A `libs/` folder containing one package adds a level without meaning
anything. The distinct import name stays because it is how a reader, and the R8
grep test, can tell platform code from service code.

## 2. A reason for every decision

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

## 3. Entity-first service layout with versions

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

## 4. Diagrams

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

## 5. Standard engineering practice

These are not part of the assignment requirements. We adopt them because a
production codebase would have them.

| Practice | What | Why |
| --- | --- | --- |
| Unit tests + coverage gate | `pytest-cov`, branch coverage, `fail_under = 85` per package; `make cov` for the HTML report | Shows which code no test exercises, and stops coverage from shrinking quietly |
| Git + pre-commit | Git repository; hooks for ruff, ruff-format, mypy, whitespace and EOF, YAML/TOML checks, private-key detection | Catches problems before commit rather than in review |
| CI | GitHub Actions: lint → type check → tests per package against Postgres → coverage gate → `pip-audit` | Every change gets the same checks, which no longer depend on one machine |
| Repo hygiene | PR template with a **Why** section, `CONTRIBUTING.md`, `.editorconfig`, `make audit` | Asks for the reason behind each change (point 2) at the point of change |
