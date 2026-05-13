"""
Smoke-test for the outbox helpers. No MQTT or Supabase touched.
Run: SUPABASE_URL=x SECRET_KEY=x python test_outbox.py
"""
import os
import tempfile
import time

# Module reads env at import — set placeholders before importing.
os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SECRET_KEY", "test")
os.environ.setdefault("BACKOFF_BASE_S", "0.01")
os.environ.setdefault("BACKOFF_CAP_S", "0.1")

import threading
from bridge_mqtt_to_supabase import (
    open_outbox, outbox_enqueue, outbox_pick_ready,
    outbox_delete, outbox_bump, outbox_dead_letter, outbox_stats,
    parse, is_retryable,
)
import requests


def fresh_conn():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.remove(path)
    return open_outbox(path), threading.Lock(), path


def test_enqueue_and_dedupe():
    conn, lock, _ = fresh_conn()
    raw = b'{"event":"tap","card_id":"AA","ts":"2026-05-13T05:00:00Z"}'
    assert outbox_enqueue(conn, lock, "carrwood/timecard", raw) == 1
    assert outbox_enqueue(conn, lock, "carrwood/timecard", raw) == 0, "dedup failed"
    # different topic -> not a dup
    assert outbox_enqueue(conn, lock, "foxwood/timecard", raw) == 1
    pending, _, _ = outbox_stats(conn, lock)
    assert pending == 2, pending
    print("test_enqueue_and_dedupe ok")


def test_pick_fifo_and_delete():
    conn, lock, _ = fresh_conn()
    outbox_enqueue(conn, lock, "carrwood/timecard", b'{"card_id":"A","ts":"2026-01-01T00:00:00Z"}')
    outbox_enqueue(conn, lock, "carrwood/timecard", b'{"card_id":"B","ts":"2026-01-01T00:00:01Z"}')

    row1 = outbox_pick_ready(conn, lock)
    assert row1 is not None
    outbox_delete(conn, lock, row1[0])

    row2 = outbox_pick_ready(conn, lock)
    assert row2 is not None and row2[0] != row1[0]
    outbox_delete(conn, lock, row2[0])

    assert outbox_pick_ready(conn, lock) is None
    print("test_pick_fifo_and_delete ok")


def test_bump_delays_next_pick():
    conn, lock, _ = fresh_conn()
    outbox_enqueue(conn, lock, "carrwood/timecard", b'{"card_id":"A","ts":"2026-01-01T00:00:00Z"}')
    row = outbox_pick_ready(conn, lock)
    assert row is not None
    # Simulate retryable failure
    outbox_bump(conn, lock, row[0], attempts=0, err="DNS failure")
    # Immediately picking should now skip it (next_attempt_at in future)
    assert outbox_pick_ready(conn, lock) is None
    # Wait past the backoff cap (0.1s in test env)
    time.sleep(0.2)
    row2 = outbox_pick_ready(conn, lock)
    assert row2 is not None and row2[0] == row[0]
    assert row2[3] == 1, f"attempts not bumped: {row2[3]}"
    print("test_bump_delays_next_pick ok")


def test_dead_letter_moves_row():
    conn, lock, _ = fresh_conn()
    outbox_enqueue(conn, lock, "carrwood/timecard", b'{"card_id":"A","ts":"2026-01-01T00:00:00Z"}')
    row = outbox_pick_ready(conn, lock)
    outbox_dead_letter(conn, lock, row[0], 400, "schema reject")
    pending, dead, _ = outbox_stats(conn, lock)
    assert pending == 0 and dead == 1, (pending, dead)
    print("test_dead_letter_moves_row ok")


def test_parse_minimal():
    obj = parse("carrwood/timecard", '{"card_id":"A","ts":"2026-05-13T05:00:00Z","actor":"timecard"}')
    assert obj["site"] == "carrwood"
    assert obj["stream"] == "timecard"
    assert obj["card_id"] == "A"
    assert obj["actor"] == "timecard"
    assert obj["event"] == "tap"
    assert obj["ts"] == "2026-05-13T05:00:00Z"
    print("test_parse_minimal ok")


def test_is_retryable():
    class FakeResp:
        def __init__(self, status): self.status_code = status

    def mkhttp(status):
        e = requests.exceptions.HTTPError()
        e.response = FakeResp(status)
        return e

    assert is_retryable(mkhttp(500))
    assert is_retryable(mkhttp(503))
    assert is_retryable(mkhttp(429))
    assert is_retryable(mkhttp(408))
    assert not is_retryable(mkhttp(400))
    assert not is_retryable(mkhttp(401))
    assert not is_retryable(mkhttp(403))
    assert not is_retryable(mkhttp(404))
    assert is_retryable(requests.exceptions.ConnectionError("dns"))
    assert is_retryable(requests.exceptions.Timeout("slow"))
    assert not is_retryable(ValueError("not network"))
    print("test_is_retryable ok")


def test_wal_durability_flags():
    conn, lock, _ = fresh_conn()
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    sync = conn.execute("PRAGMA synchronous").fetchone()[0]
    assert mode.lower() == "wal", mode
    assert sync == 2, f"synchronous expected 2 (FULL), got {sync}"
    print("test_wal_durability_flags ok")


if __name__ == "__main__":
    test_enqueue_and_dedupe()
    test_pick_fifo_and_delete()
    test_bump_delays_next_pick()
    test_dead_letter_moves_row()
    test_parse_minimal()
    test_is_retryable()
    test_wal_durability_flags()
    print("ALL TESTS PASSED")
