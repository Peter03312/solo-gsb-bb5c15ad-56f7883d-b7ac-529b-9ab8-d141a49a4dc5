"""End-to-end tests for per-target delivery pacing."""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx

from app import db
from tests.test_webhooks import _get_event, _parse_iso, _post_event_obj
from .conftest import wait_for


def _put_pace(base_url: str, target: str, interval_seconds) -> httpx.Response:
    return httpx.put(
        f"{base_url}/targets/pace",
        json={"target": target, "interval_seconds": interval_seconds},
    )


def _get_paces(base_url: str, **params) -> dict:
    return httpx.get(f"{base_url}/targets/paces", params=params).json()


def _wait_event(base_url: str, event_id: str, status: str, timeout: float = 15.0) -> dict:
    return wait_for(
        lambda: (lambda e: e if e["status"] == status else None)(
            _get_event(base_url, event_id)
        ),
        timeout=timeout,
    )


# -------------------------------------------------- API shape and normalisation

def test_pace_api_normalises_idempotent_and_lists(server):
    # A target never configured has no row and is never paced.
    target = f"{server.base_url}/t/record?pace-shape={uuid.uuid4().hex}"
    assert _get_paces(server.base_url, target=target)["paces"] == []

    # Uppercase scheme/host is normalised exactly as event ingest normalises
    # targets (str(HttpUrl)), so this matches stored targets byte-for-byte.
    upper = target.replace("http://", "HTTP://")
    resp = _put_pace(server.base_url, upper, 2)
    assert resp.status_code == 200
    row = resp.json()
    assert set(row) == {"target", "interval_seconds", "last_claimed_at", "updated_at"}
    assert row["target"] == target  # normalised form
    assert row["interval_seconds"] == 2
    assert row["last_claimed_at"] is None  # nothing claimed yet
    datetime.fromisoformat(row["updated_at"])  # parseable RFC 3339 stamp

    # Listing the same normalised target (exact match) finds the one row.
    listed = _get_paces(server.base_url, target=target)["paces"]
    assert [r["target"] for r in listed] == [target]

    # Repeating the same setting is idempotent: updated_at does not move.
    time.sleep(0.01)
    again = _put_pace(server.base_url, target, 2).json()
    assert again == row

    # Changing the interval refreshes updated_at; 0 switches pacing off but
    # keeps the row listed.
    off = _put_pace(server.base_url, target, 0).json()
    assert off["interval_seconds"] == 0
    assert off["updated_at"] != row["updated_at"]
    assert [r["target"] for r in _get_paces(server.base_url, target=target)["paces"]] == [target]

    # List envelope carries the paging fields; invalid input is a 422.
    body = _get_paces(server.base_url)
    assert set(body) == {"paces", "limit", "offset"}
    assert httpx.put(
        f"{server.base_url}/targets/pace", json={"target": target}
    ).status_code == 422
    assert httpx.put(
        f"{server.base_url}/targets/pace",
        json={"target": target, "interval_seconds": -1},
    ).status_code == 422
    assert httpx.put(
        f"{server.base_url}/targets/pace",
        json={"target": target, "interval_seconds": 3601},
    ).status_code == 422
    assert httpx.put(
        f"{server.base_url}/targets/pace",
        json={"target": target, "interval_seconds": "fast"},
    ).status_code == 422
    assert httpx.put(
        f"{server.base_url}/targets/pace",
        json={"target": "not-a-url", "interval_seconds": 1},
    ).status_code == 422
    # The upper bound itself is accepted.
    assert _put_pace(server.base_url, target, 3600).status_code == 200
    _put_pace(server.base_url, target, 0)


# ------------------------------- paced head is spaced, other targets run free

def test_paced_target_deliveries_are_spaced_and_others_unaffected(server):
    base = server.base_url
    target = f"{base}/t/record?paced={uuid.uuid4().hex}"
    other = f"{base}/t/record?unpaced={uuid.uuid4().hex}"
    try:
        assert _put_pace(base, target, 2).status_code == 200

        first = _post_event_obj(base, {
            "idempotency_key": uuid.uuid4().hex, "target": target, "payload": {},
        }).json()
        second = _post_event_obj(base, {
            "idempotency_key": uuid.uuid4().hex, "target": target, "payload": {},
        }).json()
        healthy = _post_event_obj(base, {
            "idempotency_key": uuid.uuid4().hex, "target": other, "payload": {},
        }).json()

        # The first claim is immediate (nothing claimed before); the other
        # target is completely unaffected by the pace row.
        f1 = _wait_event(base, first["id"], "delivered")
        _wait_event(base, healthy["id"], "delivered")
        assert f1["attempts_count"] == 1

        # The second event is the paced head: it goes out only once
        # last_claim + interval has passed.
        f2 = _wait_event(base, second["id"], "delivered")
        assert f2["attempts_count"] == 1
        gap = _parse_iso(f2["delivered_at"]) - _parse_iso(f1["delivered_at"])
        assert gap >= timedelta(seconds=1.5)
        assert gap < timedelta(seconds=10)

        # The pace row recorded the claims.
        row = _get_paces(base, target=target)["paces"][0]
        assert row["last_claimed_at"] is not None
    finally:
        _put_pace(base, target, 0)


# --------------------------------------- retries occupy the pacing interval

def test_retries_occupy_the_pacing_interval(server):
    base = server.base_url
    # Always fails: the 1s backoff would allow a quick second attempt, but
    # the 3s pace must dominate between claims.
    target = f"{base}/t/probe?fail_times=99&pace-retry={uuid.uuid4().hex}"
    try:
        assert _put_pace(base, target, 3).status_code == 200
        event = _post_event_obj(base, {
            "idempotency_key": uuid.uuid4().hex, "target": target, "payload": {},
        }).json()
        wait_for(
            lambda: _get_event(base, event["id"])["attempts_count"] >= 2,
            timeout=15,
        )
        attempts = _get_event(base, event["id"])["attempts"]
        t1 = _parse_iso(attempts[0]["attempted_at"])
        t2 = _parse_iso(attempts[1]["attempted_at"])
        assert (t2 - t1).total_seconds() >= 2.5
    finally:
        _put_pace(base, target, 0)


# ------------------- adjusting the interval recomputes; zero lifts the limit

def test_adjusting_interval_recomputes_and_zero_disables(server):
    base = server.base_url
    target = f"{base}/t/record?pace-adjust={uuid.uuid4().hex}"
    try:
        assert _put_pace(base, target, 30).status_code == 200
        first = _post_event_obj(base, {
            "idempotency_key": uuid.uuid4().hex, "target": target, "payload": {},
        }).json()
        _wait_event(base, first["id"], "delivered")

        # 30s from the first claim: the second head stays queued...
        second = _post_event_obj(base, {
            "idempotency_key": uuid.uuid4().hex, "target": target, "payload": {},
        }).json()
        time.sleep(0.5)
        assert _get_event(base, second["id"])["status"] == "queued"
        assert _get_event(base, second["id"])["attempts_count"] == 0

        # ...until the interval is shrunk: the next claimable moment is
        # recomputed as last_claim + 1s, which is already in the past.
        assert _put_pace(base, target, 1).status_code == 200
        f2 = _wait_event(base, second["id"], "delivered", timeout=5)
        assert f2["attempts_count"] == 1

        # Tighten again: the next head is blocked by the fresh interval...
        assert _put_pace(base, target, 30).status_code == 200
        third = _post_event_obj(base, {
            "idempotency_key": uuid.uuid4().hex, "target": target, "payload": {},
        }).json()
        time.sleep(0.5)
        assert _get_event(base, third["id"])["status"] == "queued"

        # ...and setting 0 lifts the limit immediately.
        assert _put_pace(base, target, 0).status_code == 200
        f3 = _wait_event(base, third["id"], "delivered", timeout=5)
        assert f3["attempts_count"] == 1
    finally:
        _put_pace(base, target, 0)


# ------------------------------------ queued expiry sweep continues while paced

def test_expired_queued_events_are_swept_while_paced(server):
    base = server.base_url
    target = f"{base}/t/record?pace-expiry={uuid.uuid4().hex}"
    try:
        assert _put_pace(base, target, 3).status_code == 200
        first = _post_event_obj(base, {
            "idempotency_key": uuid.uuid4().hex, "target": target, "payload": {},
        }).json()
        _wait_event(base, first["id"], "delivered")

        # The head is now pace-blocked (3s from the first claim) and crosses
        # its deadline while waiting: swept to expired with zero attempts.
        head = _post_event_obj(base, {
            "idempotency_key": uuid.uuid4().hex,
            "target": target,
            "payload": {},
            "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat(),
        }).json()
        tail = _post_event_obj(base, {
            "idempotency_key": uuid.uuid4().hex, "target": target, "payload": {},
        }).json()

        expired = _wait_event(base, head["id"], "expired", timeout=10)
        assert expired["attempts_count"] == 0
        assert expired["attempts"] == []

        # The tail becomes the head and is delivered once the interval from
        # the first claim has elapsed.
        delivered = _wait_event(base, tail["id"], "delivered", timeout=10)
        assert delivered["attempts_count"] == 1
    finally:
        _put_pace(base, target, 0)


# ------------------------------- pace write and claim are serialised by the DB

def test_claim_and_pace_are_serialized_in_database_transactions(server):
    target = f"{server.base_url}/dev/sink?pace-serial={uuid.uuid4().hex}"
    committed = threading.Event()

    def configure():
        db.set_target_pace(target, 30)
        committed.set()

    first_id = uuid.uuid4().hex
    with db.db_lock:
        conn = db.get_conn()
        conn.execute(
            "INSERT INTO events (id, idempotency_key, target, payload, status, "
            "attempts_count, created_at, next_attempt_at, deliver_after) "
            "VALUES (?, ?, ?, '{}', 'queued', 0, 'x', ?, 'x')",
            (first_id, uuid.uuid4().hex, target, time.time() - 1),
        )
        conn.commit()

        writer = threading.Thread(target=configure)
        writer.start()
        assert not committed.wait(0.3), "pace commit must wait for the claim critical section"

        # Same-thread re-entry into the RLock: this claim is part of one
        # serialised sequence with the pending pace write, and commits first.
        claimed = db.claim_due_events()
        assert [c["id"] for c in claimed] == [first_id]

    assert committed.wait(2.0)
    writer.join(timeout=2)

    # The pace row was created after that claim committed, so it carries no
    # last-claim stamp yet...
    assert db.list_target_paces(target=target)[0]["last_claimed_at"] is None

    claimed_ids = [first_id]
    with db.db_lock:
        conn = db.get_conn()
        # Finalise the hand-claimed head so the queue can advance.
        conn.execute(
            "UPDATE events SET status = 'delivered', next_attempt_at = NULL "
            "WHERE id = ?",
            (first_id,),
        )
        # ...so the next due head is still claimable, and that claim stamps
        # the row inside the same claim transaction.
        second_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO events (id, idempotency_key, target, payload, status, "
            "attempts_count, created_at, next_attempt_at, deliver_after) "
            "VALUES (?, ?, ?, '{}', 'queued', 0, 'x', ?, 'x')",
            (second_id, uuid.uuid4().hex, target, time.time() - 1),
        )
        conn.commit()
        claimed = db.claim_due_events()
        assert [c["id"] for c in claimed] == [second_id]
        claimed_ids.append(second_id)
        assert db.list_target_paces(target=target)[0]["last_claimed_at"] is not None

        # Now the committed interval gates the target: a new due event for
        # it cannot be claimed (30s from the second claim).
        conn.execute(
            "UPDATE events SET status = 'delivered', next_attempt_at = NULL "
            "WHERE id = ?",
            (second_id,),
        )
        third_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO events (id, idempotency_key, target, payload, status, "
            "attempts_count, created_at, next_attempt_at, deliver_after) "
            "VALUES (?, ?, ?, '{}', 'queued', 0, 'x', ?, 'x')",
            (third_id, uuid.uuid4().hex, target, time.time() - 1),
        )
        conn.commit()
        assert db.claim_due_events() == []

        # Tidy up: finalise the gated row too so the live worker leaves the
        # target alone, then disable pacing.
        conn.execute(
            "UPDATE events SET status = 'delivered', next_attempt_at = NULL "
            "WHERE id = ?",
            (third_id,),
        )
        conn.commit()
    db.set_target_pace(target, 0)


# -------------------------------------- old databases auto-init; state survives

def test_pace_table_auto_initialises_on_old_database_and_survives_restart():
    db_path = tempfile.mktemp(prefix="webhook-pace-restart-", suffix=".db")
    try:
        # A "legacy" database with only the original events table, no
        # target_paces table at all.
        legacy = sqlite3.connect(db_path)
        legacy.execute(
            "CREATE TABLE events (id TEXT PRIMARY KEY, idempotency_key TEXT, "
            "target TEXT, payload TEXT, status TEXT, attempts_count INTEGER, "
            "created_at TEXT, next_attempt_at REAL, delivered_at TEXT)"
        )
        legacy.commit()
        legacy.close()

        common_env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "WEBHOOK_SECRET": "x",
            "WEBHOOK_DB_PATH": db_path,
        }
        insert_event = (
            "conn = db.get_conn(); "
            "conn.execute(\"INSERT INTO events (id, idempotency_key, target, "
            "payload, status, attempts_count, created_at, next_attempt_at, "
            "deliver_after) VALUES (?, ?, 'http://example.com/legacy', '{}', "
            "'queued', 0, 'x', ?, 'x')\", (%r, %r, time.time() - 1)); "
            "conn.commit()"
        )
        first = subprocess.run(
            [sys.executable, "-c",
             "import json, time; from app import db; db.init_db(); "
             "print(json.dumps(db.list_target_paces())); "
             "db.set_target_pace('http://example.com/legacy', 5); "
             "print(json.dumps(db.list_target_paces(target='http://example.com/legacy'))); "
             + insert_event % ("e1", "k1") + "; "
             "print(json.dumps([c['id'] for c in db.claim_due_events()])); "
             "print(json.dumps(db.list_target_paces(target='http://example.com/legacy')))"],
            cwd="/workspace", env=common_env, capture_output=True, text=True,
        )
        assert first.returncode == 0, first.stderr
        lines = [json.loads(line) for line in first.stdout.splitlines()]
        assert lines[0] == []  # table was created empty on the old database
        assert len(lines[1]) == 1
        assert lines[1][0]["target"] == "http://example.com/legacy"
        assert lines[1][0]["interval_seconds"] == 5
        assert lines[1][0]["last_claimed_at"] is None
        assert lines[1][0]["updated_at"]
        # The first claim goes through (nothing claimed before) and stamps
        # the row inside the claim transaction.
        assert lines[2] == ["e1"]
        assert lines[3][0]["last_claimed_at"] is not None

        # A fresh process ("restart") re-runs init on the same file and sees
        # both the interval and the last-claim stamp persisted; the next due
        # head stays gated by the interval until it is switched off.
        second = subprocess.run(
            [sys.executable, "-c",
             "import json, time; from app import db; db.init_db(); "
             "print(json.dumps(db.list_target_paces(target='http://example.com/legacy'))); "
             "conn = db.get_conn(); "
             "conn.execute(\"UPDATE events SET status = 'delivered' WHERE id = 'e1'\"); "
             "conn.commit(); "
             + insert_event % ("e2", "k2") + "; "
             "print(json.dumps([c['id'] for c in db.claim_due_events()])); "
             "db.set_target_pace('http://example.com/legacy', 0); "
             "print(json.dumps([c['id'] for c in db.claim_due_events()]))"],
            cwd="/workspace", env=common_env, capture_output=True, text=True,
        )
        assert second.returncode == 0, second.stderr
        out = [json.loads(line) for line in second.stdout.splitlines()]
        assert len(out[0]) == 1
        assert out[0][0]["interval_seconds"] == 5
        assert out[0][0]["updated_at"] == lines[1][0]["updated_at"]
        assert out[0][0]["last_claimed_at"] == lines[3][0]["last_claimed_at"]
        assert out[1] == []        # still gated by the persisted last claim
        assert out[2] == ["e2"]    # interval 0 lifts the limit immediately
    finally:
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except OSError:
                pass
