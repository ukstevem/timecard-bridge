"""
timecard-bridge: MQTT -> Supabase ingestion with durable SQLite outbox.

Failure-mode coverage (see pss-employee-presence epic hc4):
- WAN/DNS/Supabase 5xx: row stays in outbox.db, retried with backoff
- Permanent 4xx: row moved to outbox_dead, queue keeps draining
- Bridge restart: clean_session=False + stable client_id -> broker replays;
                  WAL+FULL fsync per outbox commit -> survives power loss
- Broker replay duplicates: payload_sha256 UNIQUE in outbox
- Supabase retry duplicates: UNIQUE(card_id, device_id, ts) + Prefer: resolution=ignore-duplicates
"""

import hashlib
import json
import logging
import os
import signal
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SECRET_KEY"]
TABLE = "timecard_events"

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "timecard")
MQTT_PASS = os.environ.get("MQTT_PASS", "letmein")
MQTT_CLIENT_ID = "timecard-bridge"

OUTBOX_PATH = os.environ.get("OUTBOX_PATH", "/data/outbox.db")
BACKOFF_BASE_S = float(os.environ.get("BACKOFF_BASE_S", "1"))
BACKOFF_CAP_S = float(os.environ.get("BACKOFF_CAP_S", "60"))
WORKER_IDLE_SLEEP_S = float(os.environ.get("WORKER_IDLE_SLEEP_S", "1"))
STATS_INTERVAL_S = float(os.environ.get("STATS_INTERVAL_S", "30"))
HTTP_TIMEOUT_S = float(os.environ.get("HTTP_TIMEOUT_S", "10"))

MQTT_TOPICS = [
    ("carrwood/jobcard", 1),
    ("carrwood/timecard", 1),
    ("foxwood/timecard", 1),
]

ALLOWED_EVENTS = {"tap", "in", "out", "test"}
ALLOWED_ACTORS = {"admin", "test", "harvester", "timecard"}

# 4xx codes we still retry — transient on the client side
RETRY_4XX = {408, 425, 429}

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("timecard-bridge")
shutdown_event = threading.Event()


def iso_or_now_z(s):
    if s:
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            log.warning("bad ts in payload, falling back to wall-clock: %r", s)
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse(topic, raw):
    parts = topic.split("/")
    site = parts[0] if len(parts) > 0 else "unknown-site"
    stream = parts[1] if len(parts) > 1 else "unknown-stream"

    obj = json.loads(raw)
    event = obj.get("event", "tap")
    if event not in ALLOWED_EVENTS:
        event = "tap"
    actor = obj.get("actor")
    if actor and actor not in ALLOWED_ACTORS:
        actor = None

    return {
        "ts": iso_or_now_z(obj.get("ts")),
        "site": site,
        "stream": stream,
        "card_id": obj["card_id"],
        "device_id": obj.get("device_id", "unknown-device"),
        "event": event,
        "actor": actor,
        "topic": topic,
        "firmware": obj.get("firmware"),
        "raw_payload": obj,
    }


# ---------- Outbox -----------------------------------------------------------

OUTBOX_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    recv_ts         TEXT NOT NULL,
    topic           TEXT NOT NULL,
    raw_bytes       BLOB NOT NULL,
    payload_sha256  BLOB NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT,
    last_error      TEXT,
    UNIQUE(topic, payload_sha256)
);
CREATE INDEX IF NOT EXISTS idx_outbox_ready ON outbox(next_attempt_at, id);

CREATE TABLE IF NOT EXISTS outbox_dead (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    failed_at       TEXT NOT NULL,
    http_status     INTEGER,
    error           TEXT,
    topic           TEXT NOT NULL,
    raw_bytes       BLOB NOT NULL,
    payload_sha256  BLOB NOT NULL,
    attempts        INTEGER NOT NULL
);
"""


def open_outbox(path):
    """Open outbox conn with WAL + fsync-per-commit durability."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.executescript(OUTBOX_SCHEMA)
    return conn


def outbox_enqueue(conn, lock, topic, raw_bytes):
    """Idempotent insert: dedupes broker-replayed messages by content hash."""
    h = hashlib.sha256(raw_bytes).digest()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    with lock:
        cur = conn.execute(
            "INSERT OR IGNORE INTO outbox(recv_ts, topic, raw_bytes, payload_sha256) "
            "VALUES (?, ?, ?, ?)",
            (now, topic, raw_bytes, h),
        )
        return cur.rowcount  # 1 = inserted, 0 = duplicate


def outbox_pick_ready(conn, lock):
    with lock:
        row = conn.execute(
            "SELECT id, topic, raw_bytes, attempts FROM outbox "
            "WHERE next_attempt_at IS NULL OR next_attempt_at <= ? "
            "ORDER BY id LIMIT 1",
            (datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),),
        ).fetchone()
    return row  # tuple or None


def outbox_delete(conn, lock, row_id):
    with lock:
        conn.execute("DELETE FROM outbox WHERE id=?", (row_id,))


def outbox_bump(conn, lock, row_id, attempts, err):
    delay = min(BACKOFF_CAP_S, BACKOFF_BASE_S * (2 ** attempts))
    next_at = (
        datetime.now(timezone.utc).timestamp() + delay
    )
    next_at_iso = datetime.fromtimestamp(next_at, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    with lock:
        conn.execute(
            "UPDATE outbox SET attempts=attempts+1, next_attempt_at=?, last_error=? "
            "WHERE id=?",
            (next_at_iso, err[:500], row_id),
        )


def outbox_dead_letter(conn, lock, row_id, http_status, err):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    with lock:
        conn.execute(
            "INSERT INTO outbox_dead(failed_at, http_status, error, topic, raw_bytes, "
            "payload_sha256, attempts) "
            "SELECT ?, ?, ?, topic, raw_bytes, payload_sha256, attempts "
            "FROM outbox WHERE id=?",
            (now, http_status, err[:500], row_id),
        )
        conn.execute("DELETE FROM outbox WHERE id=?", (row_id,))


def outbox_stats(conn, lock):
    with lock:
        pending = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
        dead = conn.execute("SELECT COUNT(*) FROM outbox_dead").fetchone()[0]
        oldest = conn.execute(
            "SELECT MIN(recv_ts) FROM outbox"
        ).fetchone()[0]
    return pending, dead, oldest


# ---------- Supabase --------------------------------------------------------


def supabase_insert(row):
    """POST a parsed row. Raise (status, body) on non-2xx. 2xx + 409 both ok."""
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=ignore-duplicates",
    }
    r = requests.post(url, headers=headers, data=json.dumps(row), timeout=HTTP_TIMEOUT_S)
    if r.status_code == 409:
        return  # already inserted by a prior partial-success; treat as success
    if r.status_code >= 400:
        r.raise_for_status()


def is_retryable(exc):
    if isinstance(exc, requests.exceptions.HTTPError):
        code = exc.response.status_code
        return code >= 500 or code in RETRY_4XX
    # ConnectionError / Timeout / DNS-resolution failures are all retryable
    return isinstance(exc, requests.exceptions.RequestException)


# ---------- Worker ----------------------------------------------------------


def worker_loop(conn, lock):
    log.info("worker started, outbox=%s", OUTBOX_PATH)
    last_stats = 0.0

    while not shutdown_event.is_set():
        now = time.time()
        if now - last_stats >= STATS_INTERVAL_S:
            try:
                pending, dead, oldest = outbox_stats(conn, lock)
                log.info(
                    "outbox stats: pending=%d dead=%d oldest=%s",
                    pending, dead, oldest,
                )
            except Exception:
                log.exception("stats failed")
            last_stats = now

        try:
            row = outbox_pick_ready(conn, lock)
        except Exception:
            log.exception("outbox pick failed")
            shutdown_event.wait(WORKER_IDLE_SLEEP_S)
            continue

        if row is None:
            shutdown_event.wait(WORKER_IDLE_SLEEP_S)
            continue

        row_id, topic, raw_bytes, attempts = row
        try:
            parsed = parse(topic, raw_bytes.decode("utf-8", errors="replace"))
            supabase_insert(parsed)
            outbox_delete(conn, lock, row_id)
            log.info(
                "delivered card=%s ts=%s site=%s (id=%d, attempts=%d)",
                parsed["card_id"], parsed["ts"], parsed["site"],
                row_id, attempts,
            )
        except Exception as e:
            status = (
                e.response.status_code
                if isinstance(e, requests.exceptions.HTTPError)
                else None
            )
            err = f"{type(e).__name__}: {e}"
            if is_retryable(e):
                outbox_bump(conn, lock, row_id, attempts, err)
                log.warning(
                    "retry id=%d attempts=%d status=%s err=%s",
                    row_id, attempts + 1, status, err,
                )
            else:
                outbox_dead_letter(conn, lock, row_id, status, err)
                log.error(
                    "dead-letter id=%d status=%s err=%s topic=%s",
                    row_id, status, err, topic,
                )


# ---------- MQTT ------------------------------------------------------------


def on_connect(client, userdata, flags, rc, properties=None):
    log.info("mqtt connected rc=%s session_present=%s", rc, flags.session_present)
    for t, qos in MQTT_TOPICS:
        client.subscribe(t, qos=qos)
        log.info("subscribed %s qos=%d", t, qos)


def on_disconnect(client, userdata, *args):
    rc = args[-2] if args else 0
    log.warning("mqtt disconnected rc=%s", rc)


def make_on_message(conn, lock):
    def on_message(client, userdata, msg):
        try:
            inserted = outbox_enqueue(conn, lock, msg.topic, msg.payload)
            if inserted:
                log.debug("enqueued topic=%s bytes=%d", msg.topic, len(msg.payload))
            else:
                log.debug("dedup-ignored topic=%s (broker replay)", msg.topic)
        except Exception:
            # Do NOT swallow silently — let paho handle the unack so QoS1
            # delivers again on next reconnect. Log + re-raise.
            log.exception("outbox enqueue failed topic=%s", msg.topic)
            raise

    return on_message


# ---------- Main ------------------------------------------------------------


def main():
    conn = open_outbox(OUTBOX_PATH)
    lock = threading.Lock()

    pending_at_start, dead_at_start, _ = outbox_stats(conn, lock)
    log.info(
        "bridge starting: outbox pending=%d dead=%d host=%s topics=%s",
        pending_at_start, dead_at_start, MQTT_HOST,
        [t for t, _ in MQTT_TOPICS],
    )

    worker = threading.Thread(target=worker_loop, args=(conn, lock), daemon=True)
    worker.start()

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
        clean_session=False,
    )
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = make_on_message(conn, lock)
    client.reconnect_delay_set(min_delay=1, max_delay=60)

    def shutdown(*_):
        log.info("shutdown requested")
        shutdown_event.set()
        try:
            client.disconnect()
        except Exception:
            pass

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    try:
        client.loop_forever()
    finally:
        shutdown_event.set()
        worker.join(timeout=5)
        conn.close()
        log.info("bye")


if __name__ == "__main__":
    main()
