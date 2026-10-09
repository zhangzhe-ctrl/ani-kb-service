#!/usr/bin/env python3
"""KB-SPLIT-P1F (issue-055, ADR 0001 §五-阶段1-3) init/migration copy gate.

kb-service self-contained copy of the monorepo gate (KB-SPLIT-P3B #062): the
Makefile in this repository must not depend on the monorepo root, so both the
init-script copy and this gate ship inside the kb-service repository.

The dedicated-PG role bootstrap needs the roles to exist BEFORE the first app
connection (and before migrations replay), so a compose init script runs the
same baseline at container bootstrap. Hand-maintaining two copies invites
silent drift — one side gains a grant or loses a NOBYPASSRLS and the
least-privilege story diverges between dev-compose and migration-replay
deployments.

This gate compares the LIVE SQL of the migration and its init-script copy
(comment lines and blank lines stripped) and fails the build on drift. When
editing the role baseline, change migrations/000_*, then mirror the SQL into
deploy/init-scripts/kb-pg/00_* (headers/comments may differ freely; statements
may not).
"""

from __future__ import annotations

import difflib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

MIGRATION = ROOT / "migrations" / "000_kb_dedicated_roles.sql"
INIT_SCRIPT = ROOT / "deploy" / "init-scripts" / "kb-pg" / "00_kb_dedicated_roles.sql"


def fail(message: str) -> None:
    raise SystemExit(f"kb pg init consistency invalid: {message}")


def relative(path: Path) -> str:
    return path.resolve().relative_to(ROOT.resolve()).as_posix()


def live_sql_lines(path: Path) -> list[str]:
    """SQL body: comment-only lines (`--` prefixed) and blank lines removed,
    trailing whitespace trimmed. Adequate for this file pair — their string
    literals never start a line with `--`."""
    return [
        line.rstrip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("--")
    ]


def main() -> None:
    for path in (MIGRATION, INIT_SCRIPT):
        if not path.exists():
            fail(f"missing file: {relative(path)}")

    migration_sql = live_sql_lines(MIGRATION)
    init_sql = live_sql_lines(INIT_SCRIPT)

    if migration_sql != init_sql:
        diff = "\n".join(
            difflib.unified_diff(
                migration_sql,
                init_sql,
                fromfile=f"a/{relative(MIGRATION)} (live SQL)",
                tofile=f"b/{relative(INIT_SCRIPT)} (live SQL)",
                lineterm="",
            )
        )
        fail(
            "dedicated-PG role baseline drifted between the migration and "
            "its init-script copy — update BOTH to the same statements "
            "(comments may differ, SQL may not):\n" + diff
        )

    print("kb pg init/migration role baseline consistent")


if __name__ == "__main__":
    main()
