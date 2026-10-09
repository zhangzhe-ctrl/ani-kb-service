#!/usr/bin/env python3
"""KB-SPLIT-P1E (issue-054, ADR 0001 §五-阶段1-6) SQL centralization gate.

kb-service self-contained copy of the monorepo gate (KB-SPLIT-P3B #062): the
Makefile in this repository must not depend on the monorepo root, so the gate
lives here and resolves paths relative to the kb-service repository root.

Enforces two invariants for kb-service:

1. Query centralization — SQL against kb tables may only live under
   ``app/repositories/``. Other layers (api/services/consumers/outbox/
   session/core/rag_engine, main.py) must call repositories
   instead of embedding raw SQL.
2. DDL centralization — ``migrations/`` is the ONLY source of DDL. No
   Python file under ``app/`` may issue CREATE/ALTER/DROP TABLE/INDEX/
   POLICY or RLS statements, and the ``app.current_tenant_id`` GUC must
   stay dead (issue-054 replaced RLS with explicit tenant_id filters).

Docstrings and comments are stripped before scanning, so prose that
merely *mentions* the retired RLS/GUC machinery does not trip the gate;
SQL in live string literals does.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "app"
REPOSITORIES = APP / "repositories"

# Raw SQL markers: any SQL verb against kb-owned tables. JOIN is included
# so a join smuggled outside app/repositories/ is caught too (the 4 legit
# JOIN sites all live in message.py repositories).
SQL_TABLE_RE = re.compile(
    r"\b(?:"
    r"FROM\s+knowledge_bases|INTO\s+knowledge_bases|UPDATE\s+knowledge_bases"
    r"|DELETE\s+FROM\s+knowledge_bases|JOIN\s+knowledge_bases"
    r"|FROM\s+kb_\w+|INTO\s+kb_\w+|UPDATE\s+kb_\w+|DELETE\s+FROM\s+kb_\w+"
    r"|JOIN\s+kb_\w+"
    r")\b",
    re.IGNORECASE,
)

# DDL / RLS / GUC markers: migrations/ is the only DDL source, and the
# RLS GUC was removed in issue-054 (must never come back).
DDL_RE = re.compile(
    r"\b(?:"
    r"CREATE\s+TABLE|ALTER\s+TABLE|DROP\s+TABLE"
    r"|CREATE\s+(?:UNIQUE\s+)?INDEX|DROP\s+INDEX"
    r"|CREATE\s+POLICY|DROP\s+POLICY"
    r"|ROW\s+LEVEL\s+SECURITY"
    r"|app\.current_tenant_id"
    r")\b",
    re.IGNORECASE,
)

GENERATED_PREFIX = "app/generated/"  # protobuf artifacts — never hand-audited


def fail(message: str) -> None:
    raise SystemExit(f"kb SQL centralization invalid: {message}")


def relative(path: Path) -> str:
    return path.resolve().relative_to(ROOT.resolve()).as_posix()


def docstring_line_spans(source: str) -> list[tuple[int, int]]:
    """Return (start_line, end_line) spans of module/class/def docstrings."""
    spans: list[tuple[int, int]] = []
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return spans
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        body = node.body
        if not body:
            continue
        first = body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
            and first.end_lineno is not None
        ):
            spans.append((first.lineno, first.end_lineno))
    return spans


def strip_line_comment(line: str) -> str:
    """Cut a line at the first # that is outside a string literal."""
    quote: str | None = None
    i = 0
    while i < len(line):
        ch = line[i]
        if quote:
            if ch == "\\":
                i += 2
                continue
            if ch == quote:
                quote = None
        elif ch in "'\"":
            quote = ch
        elif ch == "#":
            return line[:i]
        i += 1
    return line


def code_only(path: Path) -> str:
    """Return source minus docstring lines and # comments (strings kept)."""
    source = path.read_text(encoding="utf-8")
    skip: set[int] = set()
    for start, end in docstring_line_spans(source):
        skip.update(range(start, end + 1))
    kept = [
        strip_line_comment(line)
        for idx, line in enumerate(source.splitlines(), start=1)
        if idx not in skip
    ]
    return "\n".join(kept)


def iter_app_python_files() -> list[Path]:
    return sorted(p for p in APP.rglob("*.py") if p.is_file())


def in_repositories(path: Path) -> bool:
    return path.resolve().is_relative_to(REPOSITORIES.resolve())


def validate_sql_only_in_repositories() -> None:
    offenders: list[str] = []
    for path in iter_app_python_files() + [ROOT / "main.py"]:
        if not path.exists() or in_repositories(path):
            continue
        rel = relative(path)
        if rel.startswith(GENERATED_PREFIX):
            continue
        if SQL_TABLE_RE.search(code_only(path)):
            offenders.append(rel)
    if offenders:
        fail(
            "raw kb-table SQL outside app/repositories/ "
            "(move it into a repository module): " + ", ".join(offenders)
        )


def validate_no_ddl_or_guc_outside_migrations() -> None:
    offenders: list[str] = []
    for path in iter_app_python_files():
        rel = relative(path)
        if rel.startswith(GENERATED_PREFIX):
            continue
        if DDL_RE.search(code_only(path)):
            offenders.append(rel)
    if offenders:
        fail(
            "DDL/RLS/GUC markers in app/ code "
            "(migrations/ is the only DDL source; the RLS GUC is retired): "
            + ", ".join(offenders)
        )


def main() -> None:
    validate_sql_only_in_repositories()
    validate_no_ddl_or_guc_outside_migrations()
    print("kb SQL centralization gate passed")


if __name__ == "__main__":
    main()
