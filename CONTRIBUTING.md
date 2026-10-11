# Contributing

## Set up

```bash
python3.12 -m venv .venv
make dev-install   # every package plus pytest, ruff, mypy, pip-audit, pre-commit
make hooks         # git pre-commit hooks
```

`make help` lists every target.

## Before you open a pull request

```bash
make fmt     # ruff format and auto-fix
make lint    # ruff and mypy
make cov     # tests with branch coverage; fails below 85% per package
make audit   # known vulnerabilities in dependencies
```

CI runs the same checks on every pull request (`.github/workflows/ci.yml`).

## Rules

- **Every decision has a reason.** A design decision goes in the owning RFC's
  §3.2.1 decision register (`docs/rfcs/`) with its *Why*, the alternatives
  rejected and when to revisit it. The PR template asks which IDs you changed.
- **Code follows the entity-first layout** in
  [docs/file-structure.md](docs/file-structure.md) §4: `app/<entity>/` for
  models, repo and helpers; `app/<entity>/v1/` for routes and schemas;
  `app/router.py` only includes routers.
- **New code comes with tests.** Unit tests for pure logic, integration tests
  against Postgres for repos and routes. Don't lower the coverage gate; add
  tests.
- **Diagrams are Mermaid**, inside the Markdown they illustrate.
- **Commits** use Conventional Commit prefixes (`feat`, `fix`, `refactor`,
  `docs`, `test`, `chore`) with the area in parentheses, e.g.
  `refactor(catalog): …`.
