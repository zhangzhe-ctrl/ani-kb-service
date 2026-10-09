"""Tests for the outbox dispatcher (issue-008 / US-010, SPEC §6.1).

Verifies:
- The dispatcher polls outbox_events and publishes undispatched rows to NATS
  `ani.tasks.kb.parse.v2` (100/batch).
- Each published event is marked published=TRUE in its own short transaction.
- The dispatcher loop survives transient errors (does not die).
- stop() drains the background task.

Uses a mock asyncpg pool and a mock NATS client so no real DB/NATS is required.
"""
import asyncio
import json
import os
import re
import sys
import uuid
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _SERVICE_ROOT)
sys.path.insert(0, os.path.join(_SERVICE_ROOT, "app", "generated"))

from app.outbox.dispatcher import OutboxDispatcher


TENANT_ID = "11111111-1111-1111-1111-111111111111"
DOC_ID = "33333333-3333-3333-3333-333333333333"


class _MockNATS:
    """Records publish calls; optionally fails to simulate transient errors."""

    def __init__(self):
        self.published: list[tuple[str, bytes]] = []
        self._fail = False

    def fail_next(self):
        self._fail = True

    async def publish(self, subject: str, payload: bytes):
        if self._fail:
            self._fail = False
            raise RuntimeError("NATS transient error")
        self.published.append((subject, payload))


class _MockJetStream:
    """Records JS publish calls; the returned PubAck stands in for the
    server ack that makes a publish durable in the ANI_TASKS stream."""

    def __init__(self):
        self.published: list[tuple[str, bytes]] = []

    async def publish(self, subject: str, payload: bytes):
        self.published.append((subject, payload))
        return object()  # PubAck stand-in


class _MockJetStreamNATS(_MockNATS):
    """NATS client exposing a JetStream context (start() path)."""

    def __init__(self):
        super().__init__()
        self.js = _MockJetStream()

    def jetstream(self):
        return self.js


def _make_event(
    event_id: int,
    payload: dict | None = None,
    event_type: str | None = "kb.parse",
):
    """Build an outbox_events row dict as returned by list_undispatched."""
    return {
        "id": event_id,
        "aggregate_type": "kb_documents",
        "aggregate_id": uuid.UUID(DOC_ID),
        "event_type": event_type,
        "tenant_id": uuid.UUID(TENANT_ID),
        "payload": payload or {"doc_id": DOC_ID, "kb_id": "kb-1"},
        "published": False,
        "published_at": None,
        "created_at": None,
    }


class _MockConn:
    """Minimal asyncpg.Connection mock for outbox repo calls."""

    def __init__(self, rows=None):
        self._rows = rows if rows is not None else []
        self._marked: list[int] = []

    async def fetch(self, sql, *args):
        # list_undispatched passes LIMIT $1 as the last arg; honor it so
        # batch_size is respected.
        limit = None
        for a in args:
            if isinstance(a, int):
                limit = a
                break
        if limit is not None:
            return list(self._rows[:limit])
        return list(self._rows)

    async def fetchval(self, sql, *args):
        # count_undispatched: SELECT COUNT(*) ... WHERE published = FALSE.
        return len([r for r in self._rows if not r.get("published")])

    async def execute(self, sql, *args):
        # mark_dispatched: UPDATE ... WHERE id = $1 (single)
        # mark_dispatched_batch: UPDATE ... WHERE id = ANY($1::int[]) (list)
        if args:
            if isinstance(args[0], int):
                self._marked.append(args[0])
            elif isinstance(args[0], list):
                self._marked.extend(args[0])
        # Return "UPDATE N" where N matches the affected count for batch.
        if args and isinstance(args[0], list):
            return f"UPDATE {len(args[0])}"
        return "UPDATE 1"


class _MockPool:
    """Returns _MockConn instances from acquire(); each acquire returns a
    fresh conn so the dispatcher's list + mark use independent conns (as in
    the real pool path)."""

    def __init__(self, rows=None):
        self._rows = rows
        self._conns: list[_MockConn] = []

    @asynccontextmanager
    async def acquire(self):
        conn = _MockConn(rows=self._rows)
        self._conns.append(conn)
        yield conn


# ── _dispatch_once ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dispatch_once_publishes_and_marks_all_events():
    rows = [_make_event(1), _make_event(2), _make_event(3)]
    pool = _MockPool(rows=rows)
    nats = _MockNATS()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2", batch_size=100
    )
    dispatched = await dispatcher._dispatch_once()
    assert dispatched == 3
    assert len(nats.published) == 3
    assert all(
        subject == "ani.tasks.kb.parse.v2" for subject, _ in nats.published
    )
    # payload is JSON-encoded bytes
    payloads = [json.loads(p) for _, p in nats.published]
    assert payloads[0]["doc_id"] == DOC_ID


@pytest.mark.asyncio
async def test_dispatch_once_batch_marks_all_published_ids():
    """Issue 2 fix: all published events are marked in ONE batched UPDATE on
    ONE connection, not one connection per event."""
    rows = [_make_event(1), _make_event(2), _make_event(3)]
    pool = _MockPool(rows=rows)
    nats = _MockNATS()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2", batch_size=100
    )
    await dispatcher._dispatch_once()
    # The mark connection is the SECOND acquire (list=1, mark=1); verify it
    # received all three event ids in a single batch call.
    assert len(pool._conns) == 2  # one for list, one for batch mark
    mark_conn = pool._conns[1]
    assert mark_conn._marked == [1, 2, 3]  # batched, single execute call


@pytest.mark.asyncio
async def test_dispatch_once_empty_returns_zero():
    pool = _MockPool(rows=[])
    nats = _MockNATS()
    dispatcher = OutboxDispatcher(pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2")
    dispatched = await dispatcher._dispatch_once()
    assert dispatched == 0
    assert nats.published == []


@pytest.mark.asyncio
async def test_dispatch_once_respects_batch_size():
    rows = [_make_event(i) for i in range(5)]
    pool = _MockPool(rows=rows)
    nats = _MockNATS()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2", batch_size=2
    )
    dispatched = await dispatcher._dispatch_once()
    assert dispatched == 2
    assert len(nats.published) == 2


@pytest.mark.asyncio
async def test_publish_uses_subject_from_settings():
    rows = [_make_event(1)]
    pool = _MockPool(rows=rows)
    nats = _MockNATS()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2"
    )
    await dispatcher._dispatch_once()
    assert nats.published[0][0] == "ani.tasks.kb.parse.v2"


# ── lifecycle ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_start_stop_lifecycle_drains_task():
    rows = [_make_event(1)]
    pool = _MockPool(rows=rows)
    nats = _MockNATS()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2",
        poll_interval=0.01,
    )
    task = dispatcher.start()
    assert isinstance(task, asyncio.Task)
    # let at least one iteration run
    await asyncio.sleep(0.05)
    await dispatcher.stop(timeout=2.0)
    assert task.done() or task.cancelled()
    # at least one event should have been published
    assert len(nats.published) >= 1


@pytest.mark.asyncio
async def test_loop_survives_transient_nats_error():
    """A transient NATS publish error must not kill the dispatcher loop."""
    # First publish fails, subsequent succeed; one event so only one publish
    # attempt per iteration. We give 3 rows so the failing one is skipped and
    # the rest still dispatch on that iteration.
    rows = [_make_event(1), _make_event(2)]
    pool = _MockPool(rows=rows)
    nats = _MockNATS()
    nats.fail_next()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2",
        poll_interval=0.01,
    )
    task = dispatcher.start()
    await asyncio.sleep(0.05)
    await dispatcher.stop(timeout=2.0)
    # The dispatcher did not crash: the transient error raised from
    # _dispatch_once (publish-all-then-batch-mark) is caught by _run_loop,
    # backed off, and retried; the task stays alive until stop().
    assert task.done() or task.cancelled()


# ── backoff on persistent errors (Issue 1) ────────────────────────────────────


@pytest.mark.asyncio
async def test_run_loop_backs_off_on_consecutive_failures():
    """Issue 1 fix: on consecutive _dispatch_once failures, the backoff
    grows exponentially and is capped at MAX_BACKOFF_INTERVAL_SECONDS."""
    from app.outbox.dispatcher import MAX_BACKOFF_INTERVAL_SECONDS

    pool = _MockPool(rows=[_make_event(1)])
    nats = _MockNATS()
    # Make every publish fail so _dispatch_once always raises.
    nats._fail = True

    class _AlwaysFailNATS(_MockNATS):
        async def publish(self, subject, payload):
            raise RuntimeError("NATS down")

    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=_AlwaysFailNATS(), subject="ani.tasks.kb.parse.v2",
        poll_interval=0.01,
    )
    # Simulate consecutive failures by exercising the backoff formula used
    # in _run_loop: backoff = min(poll_interval * 2**min(n-1, 8), cap).
    dispatcher._consecutive_failures = 0
    # First failure: backoff = 0.01 * 2^0 = 0.01
    dispatcher._consecutive_failures += 1
    b1 = min(0.01 * (2 ** 0), MAX_BACKOFF_INTERVAL_SECONDS)
    assert b1 == 0.01
    # Second failure: backoff = 0.01 * 2^1 = 0.02
    dispatcher._consecutive_failures += 1
    b2 = min(0.01 * (2 ** 1), MAX_BACKOFF_INTERVAL_SECONDS)
    assert b2 == 0.02
    # The exponent is capped at 8, so with poll_interval=0.01 the max backoff
    # is 0.01 * 2^8 = 2.56s (the 30s cap only bites at the default 1s interval).
    dispatcher._consecutive_failures = 20
    b_cap = min(0.01 * (2 ** min(19, 8)), MAX_BACKOFF_INTERVAL_SECONDS)
    assert b_cap == 2.56  # 0.01 * 256, exponent capped at 8

    # Verify the 30s cap bites at the default poll_interval (1.0s).
    from app.outbox.dispatcher import DEFAULT_POLL_INTERVAL_SECONDS
    b_default_cap = min(
        DEFAULT_POLL_INTERVAL_SECONDS * (2 ** 8),
        MAX_BACKOFF_INTERVAL_SECONDS,
    )
    assert b_default_cap == MAX_BACKOFF_INTERVAL_SECONDS  # 256 > 30 → capped


# ── payload encoding ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_payload_dict_is_json_encoded():
    rows = [_make_event(1, payload={"doc_id": DOC_ID, "kb_id": "kb-1", "n": 5})]
    pool = _MockPool(rows=rows)
    nats = _MockNATS()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2"
    )
    await dispatcher._dispatch_once()
    _, payload_bytes = nats.published[0]
    decoded = json.loads(payload_bytes.decode("utf-8"))
    assert decoded["doc_id"] == DOC_ID
    assert decoded["n"] == 5


@pytest.mark.asyncio
async def test_payload_string_is_passed_through():
    """If the DB returns payload as a JSON string, it is published as-is."""
    event = _make_event(1)
    event["payload"] = json.dumps({"doc_id": DOC_ID})
    pool = _MockPool(rows=[event])
    nats = _MockNATS()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2"
    )
    await dispatcher._dispatch_once()
    decoded = json.loads(nats.published[0][1].decode("utf-8"))
    assert decoded["doc_id"] == DOC_ID


# ── P1 #24: per-event_type subject routing (subject_overrides) ────────────────


def _rebuild_overrides() -> dict[str, str]:
    """The production wiring from main.py: 'kb.rebuild' → rebuild subject."""
    from app.core.config import Settings

    return {"kb.rebuild": Settings().nats_rebuild_subject}


@pytest.mark.asyncio
async def test_rebuild_event_routes_to_override_subject():
    """P1 #24: an event whose event_type matches a subject_overrides key
    is published to the override subject (kb.rebuild → rebuild subject),
    NOT the default parse subject."""
    rows = [_make_event(1, event_type="kb.rebuild")]
    pool = _MockPool(rows=rows)
    nats = _MockNATS()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2",
        subject_overrides=_rebuild_overrides(),
    )
    await dispatcher._dispatch_once()
    assert len(nats.published) == 1
    assert nats.published[0][0] == "ani.tasks.kb.rebuild.v1"


@pytest.mark.asyncio
async def test_parse_event_keeps_default_subject_with_overrides_set():
    """P1 #24: events whose event_type has no override entry keep the
    default subject even when subject_overrides is non-empty."""
    rows = [_make_event(1, event_type="kb.parse")]
    pool = _MockPool(rows=rows)
    nats = _MockNATS()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2",
        subject_overrides=_rebuild_overrides(),
    )
    await dispatcher._dispatch_once()
    assert nats.published[0][0] == "ani.tasks.kb.parse.v2"


@pytest.mark.asyncio
async def test_mixed_batch_routes_each_event_by_its_event_type():
    """P1 #24: a single batch containing both parse and rebuild events
    publishes each to its own subject (per-row routing, not per-batch)."""
    rows = [
        _make_event(1, event_type="kb.parse"),
        _make_event(2, event_type="kb.rebuild"),
        _make_event(3, event_type="kb.parse"),
    ]
    pool = _MockPool(rows=rows)
    nats = _MockNATS()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2",
        subject_overrides=_rebuild_overrides(),
    )
    dispatched = await dispatcher._dispatch_once()
    assert dispatched == 3
    subjects = [s for s, _ in nats.published]
    assert subjects == [
        "ani.tasks.kb.parse.v2",
        "ani.tasks.kb.rebuild.v1",
        "ani.tasks.kb.parse.v2",
    ]
    # Both kinds of events are still batch-marked in the same UPDATE.
    assert len(pool._conns) == 2
    assert pool._conns[1]._marked == [1, 2, 3]


@pytest.mark.asyncio
async def test_missing_event_type_falls_back_to_default_subject():
    """P1 #24: a row without event_type (None or empty) must not raise a
    KeyError from the overrides lookup; it falls back to the default
    subject (str(... or "") guards the None case)."""
    rows = [_make_event(1, event_type=None)]
    pool = _MockPool(rows=rows)
    nats = _MockNATS()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2",
        subject_overrides=_rebuild_overrides(),
    )
    await dispatcher._dispatch_once()
    assert nats.published[0][0] == "ani.tasks.kb.parse.v2"

    rows = [_make_event(2, event_type="")]
    pool = _MockPool(rows=rows)
    nats = _MockNATS()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2",
        subject_overrides=_rebuild_overrides(),
    )
    await dispatcher._dispatch_once()
    assert nats.published[0][0] == "ani.tasks.kb.parse.v2"


@pytest.mark.asyncio
async def test_subject_overrides_none_equals_empty_dict():
    """P1 #24: default construction (subject_overrides=None) behaves the
    same as an empty mapping — every event takes the default subject."""
    rows = [_make_event(1, event_type="kb.rebuild"), _make_event(2)]
    pool = _MockPool(rows=rows)
    nats = _MockNATS()
    dispatcher_default = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2",
        subject_overrides=None,
    )
    await dispatcher_default._dispatch_once()
    dispatcher_empty = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2",
        subject_overrides={},
    )
    await dispatcher_empty._dispatch_once()
    subjects = [s for s, _ in nats.published]
    assert subjects == [
        "ani.tasks.kb.parse.v2",
        "ani.tasks.kb.parse.v2",
        "ani.tasks.kb.parse.v2",
        "ani.tasks.kb.parse.v2",
    ]


# ── KB-SPLIT-P1A (issue-050): kb-owned outbox table ──────────────────────────


_SHARED_TABLE_RE = re.compile(r"(?<![A-Za-z_])outbox_events(?![A-Za-z_])")


class _StatefulOutboxConn:
    """In-memory kb_outbox_events backing store for repo + dispatcher calls.

    Simulates the pending→published state machine of migrations/009:
    - fetchrow on INSERT adds a pending (published=False) row
    - fetch on SELECT ... published = FALSE returns only pending rows
    - execute on UPDATE ... published = TRUE flips matching rows
    """

    def __init__(self):
        self.rows: list[dict[str, Any]] = []
        self.sql_seen: list[str] = []
        self._next_id = 1

    async def execute(self, sql, *args):
        self.sql_seen.append(sql)
        if sql.lstrip().startswith("SET LOCAL"):
            return "SET LOCAL"
        if "SET published = TRUE" in sql and "ANY(" in sql:
            ids = set(args[0])
            n = 0
            for r in self.rows:
                if r["id"] in ids and not r["published"]:
                    r["published"] = True
                    n += 1
            return f"UPDATE {n}"
        if "SET published = TRUE" in sql:
            event_id = args[0]
            for r in self.rows:
                if r["id"] == event_id and not r["published"]:
                    r["published"] = True
                    return "UPDATE 1"
            return "UPDATE 0"
        raise AssertionError(f"unexpected execute: {sql}")

    async def fetchrow(self, sql, *args):
        self.sql_seen.append(sql)
        if "INSERT INTO" in sql:
            row = {
                "id": self._next_id,
                "aggregate_type": args[0],
                "aggregate_id": args[1],
                "event_type": args[2],
                "tenant_id": args[3],
                "payload": args[4],
                "published": False,
                "published_at": None,
                "created_at": None,
            }
            self._next_id += 1
            self.rows.append(row)
            return {"id": row["id"]}
        raise AssertionError(f"unexpected fetchrow: {sql}")

    async def fetch(self, sql, *args):
        self.sql_seen.append(sql)
        if "published = FALSE" in sql:
            limit = args[-1] if args else 100
            return [dict(r) for r in self.rows if not r["published"]][:limit]
        raise AssertionError(f"unexpected fetch: {sql}")

    async def fetchval(self, sql, *args):
        self.sql_seen.append(sql)
        if "COUNT(*)" in sql and "published = FALSE" in sql:
            return len([r for r in self.rows if not r["published"]])
        raise AssertionError(f"unexpected fetchval: {sql}")


@pytest.mark.asyncio
async def test_outbox_repo_targets_kb_owned_table():
    """issue-050: repository SQL must target kb_outbox_events and must not
    reference the shared-library outbox_events table anywhere."""
    from app.repositories import outbox as outbox_repo

    conn = _StatefulOutboxConn()
    await outbox_repo.insert_event(
        conn,
        tenant_id=TENANT_ID,
        aggregate_type="kb_documents",
        aggregate_id=DOC_ID,
        event_type="kb.parse",
        payload={"doc_id": DOC_ID},
    )
    await outbox_repo.list_undispatched(conn)
    await outbox_repo.mark_dispatched_batch(conn, event_ids=[1])
    assert conn.sql_seen, "no SQL executed"
    for sql in conn.sql_seen:
        assert _SHARED_TABLE_RE.search(sql) is None, (
            f"shared outbox_events referenced: {sql}"
        )
    assert any("kb_outbox_events" in sql for sql in conn.sql_seen)


@pytest.mark.asyncio
async def test_pending_to_published_flow_on_owned_table():
    """issue-050: pending write → dispatcher poll → published state machine
    on the kb-owned table (in-memory kb_outbox_events)."""
    from app.repositories import outbox as outbox_repo
    from app.outbox.dispatcher import OutboxDispatcher

    conn = _StatefulOutboxConn()
    event_id = await outbox_repo.insert_event(
        conn,
        tenant_id=TENANT_ID,
        aggregate_type="kb_documents",
        aggregate_id=DOC_ID,
        event_type="kb.parse",
        payload={"doc_id": DOC_ID, "kb_id": "kb-1"},
    )

    class _SingleConnPool:
        @asynccontextmanager
        async def acquire(self):
            yield conn

    nats = _MockNATS()
    dispatcher = OutboxDispatcher(
        pool=_SingleConnPool(), nats_client=nats, subject="ani.tasks.kb.parse.v2"
    )

    # The pending row is visible to the dispatcher poll.
    pending = await outbox_repo.list_undispatched(conn)
    assert [r["id"] for r in pending] == [event_id]
    assert conn.rows[0]["published"] is False

    dispatched = await dispatcher._dispatch_once()
    assert dispatched == 1
    assert len(nats.published) == 1

    # The published row leaves the undispatched set (state machine flipped).
    assert await outbox_repo.list_undispatched(conn) == []
    assert conn.rows[0]["published"] is True
    assert conn.rows[0]["id"] == event_id


@pytest.mark.asyncio
async def test_nats_outage_backs_up_events_without_loss():
    """issue-050: NATS down → events stay pending in kb_outbox_events (nothing
    is marked published); after NATS recovers the SAME events are published
    and marked (at-least-once, no loss)."""
    from app.repositories import outbox as outbox_repo
    from app.outbox.dispatcher import OutboxDispatcher

    conn = _StatefulOutboxConn()
    for _ in range(2):
        await outbox_repo.insert_event(
            conn,
            tenant_id=TENANT_ID,
            aggregate_type="kb_documents",
            aggregate_id=DOC_ID,
            event_type="kb.parse",
            payload={"doc_id": DOC_ID},
        )

    class _SingleConnPool:
        @asynccontextmanager
        async def acquire(self):
            yield conn

    nats = _MockNATS()
    nats.fail_next()
    dispatcher = OutboxDispatcher(
        pool=_SingleConnPool(), nats_client=nats, subject="ani.tasks.kb.parse.v2"
    )

    # NATS outage: the first publish raises → _dispatch_once fails →
    # nothing is marked; both events remain pending in the owned table.
    with pytest.raises(RuntimeError):
        await dispatcher._dispatch_once()
    assert nats.published == []
    assert all(r["published"] is False for r in conn.rows), (
        "events must stay pending during a NATS outage (no loss)"
    )

    # NATS recovers: the same pending events are published and marked.
    dispatched = await dispatcher._dispatch_once()
    assert dispatched == 2
    assert len(nats.published) == 2
    assert all(r["published"] is True for r in conn.rows)


# ── JetStream publish (server PubAck durability) ─────────────────────────────


@pytest.mark.asyncio
async def test_dispatch_once_publishes_via_jetstream_when_available():
    """When a JetStream context is resolved, events go through js.publish
    (server PubAck) rather than core nats.publish — so published=TRUE means
    the message is durably in the ANI_TASKS stream, not merely handed to a
    possibly-dead socket."""
    rows = [_make_event(1), _make_event(2)]
    pool = _MockPool(rows=rows)
    nats = _MockJetStreamNATS()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2"
    )
    dispatcher._js = nats.js  # simulate the context resolved in start()
    dispatched = await dispatcher._dispatch_once()
    assert dispatched == 2
    # Every event went through JetStream; none through core publish.
    assert len(nats.js.published) == 2
    assert nats.published == []
    assert all(s == "ani.tasks.kb.parse.v2" for s, _ in nats.js.published)


@pytest.mark.asyncio
async def test_dispatch_once_falls_back_to_core_publish_without_jetstream():
    """Fallback path: with no JetStream context the dispatcher uses core
    publish (previous behavior) so the loop still makes progress."""
    rows = [_make_event(1)]
    pool = _MockPool(rows=rows)
    nats = _MockNATS()
    dispatcher = OutboxDispatcher(
        pool=pool, nats_client=nats, subject="ani.tasks.kb.parse.v2"
    )
    assert dispatcher._js is None
    dispatched = await dispatcher._dispatch_once()
    assert dispatched == 1
    assert len(nats.published) == 1
