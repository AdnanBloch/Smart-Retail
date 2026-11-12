"""Build the review explorer's data file from the docs and the code.

Reads every Markdown page under docs/, the decision register in each RFC, the
business-rule tables, the failure catalogue and the mentor reviews. Then scans
the code for decision IDs and resolves the tests the failure catalogue names.
Writes site/data.js, which site/index.html loads. Standard library only, so it
runs without the project venv:

    python3 tools/review_explorer/build.py
"""

from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
SITE = Path(__file__).resolve().parent / "site"

PREFIXES = ("ARC", "PLT", "GW", "IDN", "CAT", "ORD", "INV", "NTF", "ANL", "AST")
ID_RE = re.compile(r"\b(?:" + "|".join(PREFIXES) + r")-D\d+\b")
FAILURE_RE = re.compile(r"\bF(?:[1-9]|[1-9]\d)\b")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
ALIAS_RE = re.compile(r"\(=\s*([^)]*)\)")
TEST_NAME_RE = re.compile(r"\btest_\w+(?:\.py)?")

CODE_DIRS = ("services", "common", "workers", "tests", "scripts", "ops")
CODE_FILES = ("docker-compose.yml", "Makefile", ".env.example")
CODE_SUFFIXES = {".py", ".yml", ".yaml", ".toml", ".sql", ".js", ".sh", ".avsc", ".md"}
SKIP_DIRS = {".venv", "venv", "node_modules", "__pycache__", ".pytest_cache", ".mypy_cache"}
MAX_REFS_PER_ID = 20
SNIPPET_CONTEXT = 4
TEST_BODY_LINES = 45

SPEC_DOCS = {"learning-path", "milestones", "execution-guidelines", "part-a", "part-b"}


# --- Markdown helpers -------------------------------------------------------


def plain(text: str) -> str:
    """Heading or cell text as a reader sees it: no links, code ticks or emphasis."""
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = text.replace("`", "").replace("**", "").replace("\\", "")
    return re.sub(r"(?<!\w)\*(?!\s)|(?<!\s)\*(?!\w)", "", text).strip()


def split_row(line: str) -> list[str]:
    """Cells of a GFM table row. A pipe escaped as \\| stays in its cell."""
    body = line.strip()
    body = body[1:] if body.startswith("|") else body
    body = body[:-1] if body.endswith("|") and not body.endswith("\\|") else body
    cells = re.split(r"(?<!\\)\|", body)
    return [cell.replace("\\|", "|").strip() for cell in cells]


def is_row(line: str) -> bool:
    return line.lstrip().startswith("|")


def is_delimiter(line: str) -> bool:
    return bool(re.fullmatch(r"\s*\|?[\s:|-]+\|?\s*", line)) and "-" in line


def outside_fences(lines: list[str]) -> list[bool]:
    """For each line, True when it is ordinary Markdown rather than a code block."""
    flags, fence = [], ""
    for line in lines:
        stripped = line.lstrip()
        marker = stripped[:3]
        if marker in ("```", "~~~"):
            if not fence:
                fence = marker
                flags.append(False)
                continue
            if marker == fence:
                fence = ""
                flags.append(False)
                continue
        flags.append(not fence)
    return flags


def parse_sections(lines: list[str], ordinary: list[bool]) -> list[dict[str, Any]]:
    """Every heading, in order, with the line span its section covers (1-based, inclusive)."""
    sections: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        match = HEADING_RE.match(line) if ordinary[index] else None
        if not match:
            continue
        if sections:
            sections[-1]["end"] = index
        sections.append(
            {
                "n": len(sections),
                "level": len(match.group(1)),
                "title": plain(match.group(2)),
                "start": index + 1,
                "end": len(lines),
            }
        )
    for section in sections:
        body = "\n".join(lines[section["start"] : section["end"]])
        section["ids"] = sorted(set(ID_RE.findall(body)))
        section["failures"] = sorted(set(FAILURE_RE.findall(body)), key=lambda f: int(f[1:]))
    return sections


def parse_tables(lines: list[str], ordinary: list[bool]) -> list[dict[str, Any]]:
    """Every table: its header cells and its rows, each row with its 1-based line number."""
    tables: list[dict[str, Any]] = []
    index = 0
    while index < len(lines) - 1:
        if (
            ordinary[index]
            and is_row(lines[index])
            and is_delimiter(lines[index + 1])
            and (index == 0 or not is_row(lines[index - 1]))
        ):
            header = split_row(lines[index])
            table: dict[str, Any] = {"header": header, "rows": [], "line": index + 1}
            index += 2
            while index < len(lines) and is_row(lines[index]):
                table["rows"].append((index + 1, split_row(lines[index])))
                index += 1
            tables.append(table)
            continue
        index += 1
    return tables


def section_at(sections: list[dict[str, Any]], line: int, max_level: int = 6) -> int | None:
    """Ordinal of the innermost heading at or above `line` whose level is <= max_level."""
    found = None
    for section in sections:
        if section["start"] > line:
            break
        if section["level"] <= max_level:
            found = section["n"]
    return found


# --- Documents --------------------------------------------------------------


def doc_id(path: Path) -> str:
    if path == ROOT / "README.md":
        return "repo-readme"
    rel = path.relative_to(DOCS)
    return ("archive-" if "archive" in rel.parts else "") + path.stem


def doc_group(path: Path) -> str:
    rel = path.relative_to(ROOT).as_posix()
    if "/archive/" in rel:
        return "Archived RFCs"
    if "/rfcs/" in rel:
        return "RFCs"
    if "/reviews/" in rel:
        return "Mentor reviews"
    if path.stem in SPEC_DOCS:
        return "Specification (as supplied)"
    return "Guides"


def load_docs() -> list[dict[str, Any]]:
    paths = [ROOT / "README.md", *sorted(DOCS.rglob("*.md"))]
    docs = []
    for path in paths:
        md = path.read_text(encoding="utf-8")
        lines = md.splitlines()
        ordinary = outside_fences(lines)
        sections = parse_sections(lines, ordinary)
        title = next((s["title"] for s in sections if s["level"] == 1), path.stem)
        docs.append(
            {
                "id": doc_id(path),
                "path": path.relative_to(ROOT).as_posix(),
                "title": title,
                "group": doc_group(path),
                "md": md,
                "sections": sections,
                "_lines": lines,
                "_ordinary": ordinary,
            }
        )
    ids = [d["id"] for d in docs]
    duplicates = {i for i in ids if ids.count(i) > 1}
    if duplicates:
        raise SystemExit(f"duplicate doc ids: {sorted(duplicates)}")
    return docs


# --- Decisions, rules, failures, reviews, flows ------------------------------


def parse_decisions(
    docs: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, list[str]], list[dict[str, Any]]]:
    decisions: list[dict[str, Any]] = []
    aliases: dict[str, list[str]] = defaultdict(list)
    services: list[dict[str, Any]] = []
    for doc in docs:
        if doc["group"] != "RFCs":
            continue
        rows = []
        for table in parse_tables(doc["_lines"], doc["_ordinary"]):
            if len(table["header"]) >= 5 and table["header"][1] == "Decision":
                rows.extend(table["rows"])
        for line, cells in rows:
            if len(cells) < 5 or not ID_RE.fullmatch(cells[0]):
                continue
            decision_id = cells[0]
            legacy: list[str] = []
            for group in ALIAS_RE.findall(cells[1]):
                legacy.extend(token.strip() for token in group.split(",") if token.strip())
            for token in legacy:
                aliases[token].append(decision_id)
            decisions.append(
                {
                    "id": decision_id,
                    "prefix": decision_id.split("-")[0],
                    "doc": doc["id"],
                    "line": line,
                    "section": section_at(doc["sections"], line),
                    "decision": cells[1],
                    "why": cells[2],
                    "alternatives": cells[3],
                    "revisit": cells[4],
                    "legacy": legacy,
                }
            )
        if rows:
            prefix = rows[0][1][0].split("-")[0]
            services.append(
                {
                    "prefix": prefix,
                    "doc": doc["id"],
                    "title": doc["title"],
                    "name": service_name(doc["title"]),
                    "count": sum(1 for d in decisions if d["prefix"] == prefix),
                }
            )
    return decisions, dict(aliases), services


def service_name(rfc_title: str) -> str:
    """'RFC 0005 — order Service' -> 'order'; 'RFC 0000 — System Architecture' -> 'Architecture'."""
    name = rfc_title.split("—", 1)[-1].strip()
    name = re.sub(r"\s+Service$", "", name)
    return {"System Architecture": "Architecture"}.get(name, name)


def resolve_ids(raw: str, aliases: dict[str, list[str]], known: set[str]) -> list[str]:
    """IDs named in a cell like 'A13, A14, ORD-D9, D10 ✅', resolved to register IDs."""
    found: list[str] = []
    prefix = ""
    for token in re.findall(r"[A-Z]+(?:-[A-Z]*)?\d+(?:-[A-Z]?\d+)?", raw):
        full = re.fullmatch(r"([A-Z]+)-D\d+", token)
        if full:
            prefix = full.group(1)
            candidates = [token]
        elif re.fullmatch(r"D\d+", token) and prefix:
            candidates = [f"{prefix}-{token}"]
        else:
            candidates = aliases.get(token, [])
        for candidate in candidates:
            if candidate in known and candidate not in found:
                found.append(candidate)
    return found


def status_of(raw: str) -> str:
    for mark, status in (("⏳", "designed"), ("🟡", "deviation"), ("✅", "implemented")):
        if mark in raw:
            return status
    return ""


def parse_rules(
    doc: dict[str, Any], aliases: dict[str, list[str]], known: set[str]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rules, silent = [], []
    for table in parse_tables(doc["_lines"], doc["_ordinary"]):
        header = table["header"]
        if header[:2] == ["#", "Rule"]:
            for line, cells in table["rows"]:
                cells = cells + [""] * (6 - len(cells))
                rules.append(
                    {
                        "num": cells[0],
                        "line": line,
                        "section": section_at(doc["sections"], line),
                        "area": doc["sections"][section_at(doc["sections"], line, 2) or 0]["title"],
                        "rule": cells[1],
                        "why": cells[2],
                        "source": cells[3],
                        "alternatives": cells[4],
                        "ids_raw": cells[5],
                        "ids": resolve_ids(cells[5], aliases, known),
                        "status": status_of(cells[5]),
                        "silent": "Silent" in cells[3],
                    }
                )
        elif header[:2] == ["Topic", "Our decision"]:
            for line, cells in table["rows"]:
                silent.append(
                    {
                        "topic": cells[0],
                        "decision": cells[1],
                        "alternative": cells[2] if len(cells) > 2 else "",
                        "line": line,
                    }
                )
    return rules, silent


def index_tests() -> dict[str, list[dict[str, Any]]]:
    """Every test function by name, with its body as a snippet; every test module by file name."""
    tests: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for path in code_files():
        if path.suffix != ".py" or "test" not in path.name:
            continue
        rel = path.relative_to(ROOT).as_posix()
        tests[path.name].append({"file": rel, "line": 1, "from": 1, "code": "", "module": True})
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        for index, line in enumerate(lines):
            match = re.match(r"^(\s*)(?:async\s+)?def\s+(test_\w+)", line)
            if not match:
                continue
            indent = len(match.group(1))
            start = index
            while start > 0 and lines[start - 1].strip().startswith("@"):
                start -= 1
            end = index + 1
            while end < len(lines) and end - index < TEST_BODY_LINES:
                text = lines[end]
                if text.strip() and len(text) - len(text.lstrip()) <= indent:
                    break
                end += 1
            tests[match.group(2)].append(
                {
                    "file": rel,
                    "line": index + 1,
                    "from": start + 1,
                    "code": "\n".join(lines[start:end]).rstrip(),
                }
            )
    return dict(tests)


def parse_failures(
    doc: dict[str, Any], tests: dict[str, list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    failures = []
    for table in parse_tables(doc["_lines"], doc["_ordinary"]):
        if table["header"][:2] != ["#", "What if …"]:
            continue
        for line, cells in table["rows"]:
            cells = cells + [""] * (5 - len(cells))
            names = list(dict.fromkeys(TEST_NAME_RE.findall(cells[4])))
            failures.append(
                {
                    "id": cells[0],
                    "line": line,
                    "section": section_at(doc["sections"], line),
                    "scenario": cells[1],
                    "outcome": cells[2],
                    "mechanism": cells[3],
                    "proof": cells[4],
                    "ids": sorted(set(ID_RE.findall(" ".join(cells)))),
                    "tests": [{"name": n, "found": tests.get(n, [])} for n in names],
                }
            )
    return failures


def parse_reviews(doc: dict[str, Any]) -> list[dict[str, Any]]:
    reviews, date = [], ""
    for section in doc["sections"]:
        match = re.match(r"Review of (\d{4}-\d{2}-\d{2})", section["title"])
        if section["level"] == 2:
            date = match.group(1) if match else ""
            if date:
                reviews.append({"date": date, "title": "Overview", "section": section["n"]})
        elif section["level"] == 3 and date:
            reviews.append({"date": date, "title": section["title"], "section": section["n"]})
    return reviews


def parse_flows(doc: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"title": s["title"], "section": s["n"]}
        for s in doc["sections"]
        if s["level"] == 2 and re.match(r"\d+\.", s["title"])
    ]


# --- Code -------------------------------------------------------------------


def code_files() -> list[Path]:
    found = [ROOT / name for name in CODE_FILES if (ROOT / name).exists()]
    for top in CODE_DIRS:
        for dirpath, dirnames, filenames in os.walk(ROOT / top):
            dirnames[:] = sorted(
                d for d in dirnames if d not in SKIP_DIRS and not d.startswith(".")
            )
            for filename in sorted(filenames):
                path = Path(dirpath) / filename
                if path.suffix in CODE_SUFFIXES or filename == "Dockerfile":
                    found.append(path)
    return found


def scan_code_refs() -> dict[str, list[dict[str, Any]]]:
    """Every line of code, config or test that names a decision ID, with context."""
    refs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for path in code_files():
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        rel = path.relative_to(ROOT).as_posix()
        is_test = "/tests/" in f"/{rel}" or path.name.startswith("test_")
        for index, line in enumerate(lines):
            for decision_id in dict.fromkeys(ID_RE.findall(line)):
                if len(refs[decision_id]) >= MAX_REFS_PER_ID:
                    continue
                start = max(0, index - SNIPPET_CONTEXT)
                refs[decision_id].append(
                    {
                        "file": rel,
                        "line": index + 1,
                        "from": start + 1,
                        "code": "\n".join(lines[start : index + SNIPPET_CONTEXT + 1]),
                        "test": is_test,
                    }
                )
    return dict(refs)


# --- Live stack and git ----------------------------------------------------


def env_value(name: str, default: str) -> str:
    for candidate in (ROOT / ".env", ROOT / ".env.example"):
        if candidate.exists():
            for line in candidate.read_text(encoding="utf-8").splitlines():
                if line.startswith(f"{name}="):
                    return line.split("=", 1)[1].strip() or default
    return default


def live_links() -> list[dict[str, str]]:
    temporal = env_value("TEMPORAL_UI_PORT", "8080")
    links = [
        ("Gateway API docs", "http://localhost:8000/docs", "Every route, through the gateway"),
        ("Temporal UI", f"http://localhost:{temporal}", "Order saga histories and retries"),
        ("RabbitMQ", "http://localhost:15672", "Celery queues and consumers"),
        ("MinIO console", "http://localhost:9001", "Product media buckets"),
    ]
    return [{"name": name, "url": url, "what": what} for name, url, what in links]


def git_head() -> dict[str, str]:
    git = ROOT / ".git"
    try:
        head = (git / "HEAD").read_text().strip()
        if not head.startswith("ref: "):
            return {"branch": "(detached)", "commit": head[:7]}
        ref = head[5:]
        ref_file = git / ref
        if ref_file.exists():
            commit = ref_file.read_text().strip()
        else:
            packed = (git / "packed-refs").read_text().splitlines()
            commit = next(line.split()[0] for line in packed if line.endswith(f" {ref}"))
        return {"branch": ref.removeprefix("refs/heads/"), "commit": commit[:7]}
    except (OSError, StopIteration):
        return {"branch": "", "commit": ""}


# --- Main -------------------------------------------------------------------


def find(docs: list[dict[str, Any]], identifier: str) -> dict[str, Any]:
    return next(d for d in docs if d["id"] == identifier)


def main() -> None:
    docs = load_docs()
    decisions, aliases, services = parse_decisions(docs)
    known = {d["id"] for d in decisions}
    rules, silent = parse_rules(find(docs, "business-logic-and-decisions"), aliases, known)
    tests = index_tests()
    failures = parse_failures(find(docs, "distributed-systems-and-failure-modes"), tests)
    data = {
        "built": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "git": git_head(),
        "root": ROOT.as_posix(),
        "docs": [{k: v for k, v in d.items() if not k.startswith("_")} for d in docs],
        "services": services,
        "decisions": decisions,
        "aliases": aliases,
        "rules": rules,
        "silent": silent,
        "failures": failures,
        "reviews": parse_reviews(find(docs, "mentor-reviews")),
        "flows": parse_flows(find(docs, "business-flows")),
        "codeRefs": scan_code_refs(),
        "live": live_links(),
    }
    SITE.mkdir(exist_ok=True)
    out = SITE / "data.js"
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    out.write_text(f"window.REVIEW_DATA = {payload};\n", encoding="utf-8")
    linked = sum(1 for d in decisions if d["id"] in data["codeRefs"])
    print(
        f"wrote {out.relative_to(ROOT)}: {len(docs)} docs, {len(decisions)} decisions "
        f"({linked} with code references), {len(rules)} business rules, "
        f"{len(failures)} failure cases, {len(data['reviews'])} review points, "
        f"{len(data['flows'])} flows"
    )
    unresolved = sum(1 for f in failures for t in f["tests"] if not t["found"])
    if unresolved:
        print(f"note: {unresolved} test names in the failure catalogue were not found in code")


if __name__ == "__main__":
    main()
