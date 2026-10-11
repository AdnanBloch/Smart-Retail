"""R8: common holds no domain vocabulary and never imports a service."""

import ast
from pathlib import Path

LIB = Path(__file__).resolve().parents[1] / "smartretail_common"
REPO = Path(__file__).resolve().parents[2]

# Values allowed to appear in common despite matching a service enum,
# each with a reason. Keep this empty unless a reviewer agrees.
ALLOW_LIST: dict[str, str] = {
    # RFC 0000 I4: merchant_id is the subject of a merchant token; the
    # verifier needs to recognise that one role value to derive it.
    "merchant": "auth.py derives merchant_id from the merchant role (I4)",
    # Generic words that M2's service enums happen to share. Each is part of an
    # M1 contract in common, not a domain value (flagged for review, M2).
    "expired": "auth.py: the InvalidToken detail for an expired JWT, not ReservationStatus",
    "ready": "health.py: the readiness endpoint's status, not MediaProcessingStatus",
    "failed": "health.py: a readiness check's result, not an order, payment or media status",
}


def _service_enum_values() -> set[str]:
    values: set[str] = set()
    for path in REPO.glob("services/*/app/**/enums.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and any(
                getattr(b, "id", None) == "DomainEnum" for b in node.bases
            ):
                for stmt in node.body:
                    if (
                        isinstance(stmt, ast.Assign)
                        and isinstance(stmt.value, ast.Constant)
                        and isinstance(stmt.value.value, str)
                    ):
                        values.add(stmt.value.value)
    return values


def _lib_modules() -> list[Path]:
    return sorted(p for p in LIB.rglob("*.py") if p.name != "testing.py")


def test_no_service_vocabulary_in_common() -> None:
    vocabulary = _service_enum_values() - set(ALLOW_LIST)
    hits = []
    for path in _lib_modules():
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and node.value in vocabulary:
                hits.append(f"{path.name}:{node.lineno} {node.value!r}")
    assert not hits, f"domain vocabulary found in common: {hits}"


def test_common_never_imports_services() -> None:
    offenders = []
    for path in LIB.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            offenders += [
                f"{path.name}: {n}" for n in names if n.split(".")[0] in {"app", "services"}
            ]
    assert not offenders, offenders


def test_guard_finds_service_vocabularies() -> None:
    # If the enums glob stops matching, the guard above passes vacuously.
    assert _service_enum_values(), "no service enums found; is the glob in sync with the layout?"
