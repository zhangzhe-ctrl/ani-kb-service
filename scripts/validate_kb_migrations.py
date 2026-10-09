#!/usr/bin/env python3
"""KB-SPLIT-P3B (issue-062): migration baseline sanity gate.

kb-service owns its DDL exclusively (``migrations/`` is the only DDL source,
ADR 0001 §五-阶段1-6). This gate keeps the baseline replayable and reviewable:

1. every migration file matches ``NNN_<name>.sql`` with a zero-padded,
   monotonically increasing 3-digit prefix starting at 000 (no gaps, no
   duplicates) — the replayer orders by filename;
2. every migration file is non-empty (a stray empty file would silently drop
   a step from the baseline).

It does not execute SQL (that is the replay's job at deploy/integration time);
it is a cheap, dependency-free structural check that runs in CI before the
image build.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import NoReturn

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "migrations"

MIGRATION_RE = re.compile(r"^(\d{3})_[A-Za-z0-9_]+\.sql$")


def fail(message: str) -> NoReturn:
    raise SystemExit(f"kb migration baseline invalid: {message}")


def main() -> None:
    if not MIGRATIONS.is_dir():
        fail(f"missing migrations directory: {MIGRATIONS}")

    files = sorted(p for p in MIGRATIONS.iterdir() if p.is_file())
    if not files:
        fail("migrations directory is empty")

    numbered: list[tuple[int, str]] = []
    for path in files:
        if path.suffix != ".sql":
            fail(f"unexpected non-SQL file in migrations/: {path.name}")
        match = MIGRATION_RE.match(path.name)
        if match is None:
            fail(
                f"migration filename must match NNN_<name>.sql: {path.name}"
            )
        # match is non-None past the guard above (mypy can't narrow fail()).
        assert match is not None
        if path.stat().st_size == 0:
            fail(f"empty migration file: {path.name}")
        numbered.append((int(match.group(1)), path.name))

    prefixes = [number for number, _ in numbered]
    duplicates = sorted({n for n in prefixes if prefixes.count(n) > 1})
    if duplicates:
        fail(f"duplicate migration prefixes: {duplicates}")
    expected = list(range(prefixes[0], prefixes[0] + len(prefixes)))
    if prefixes != expected:
        fail(
            "migration prefixes must be a gap-free sequence from "
            f"{prefixes[0]:03d}: found {[f'{n:03d}' for n in prefixes]}"
        )
    if prefixes[0] != 0:
        fail(f"migration baseline must start at 000, found {prefixes[0]:03d}")

    print(f"kb migration baseline valid ({len(numbered)} migrations, 000..{prefixes[-1]:03d})")


if __name__ == "__main__":
    main()
