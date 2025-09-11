import os, json, datetime
from dotenv import load_dotenv

# Load .env from the script directory
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

import paho.mqtt.client as mqtt
import requests


SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SECRET_KEY"]   # service role key
TABLE = "timecard_events"

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "timecard")
MQTT_PASS = os.environ.get("MQTT_PASS", "letmein")

# data topics
MQTT_TOPICS = [
    ("carrwood/jobcard", 1),
    ("carrwood/timecard", 1),
    ("foxwood/timecard", 1),
]

# status topics (per-device + optional heartbeat)
STATUS_TOPICS = [
    ("+/+/status/+", 1),        # <site>/<stream>/status/<device_id>
    ("+/+/status/+/hb", 1),     # <site>/<stream>/status/<device_id>/hb
]

ALLOWED_EVENTS = {"tap","in","out","test"}
ALLOWED_ACTORS = {"admin","test","harvester","timecard"}

def iso_or_now_z(s: str | None) -> str:
    if s:
        try:
            dt = datetime.datetime.fromisoformat(s.replace("Z","+00:00"))
            return dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            pass
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def parse(topic: str, raw: str) -> dict:
    parts = topic.split("/")
    site = parts[0] if len(parts) > 0 else "unknown-site"
    stream = parts[1] if len(parts) > 1 else "unknown-stream"

    obj = json.loads(raw)  # Expect JSON
    event = obj.get("event","tap")
    if event not in ALLOWED_EVENTS: event = "tap"
    actor = obj.get("actor")
    if actor and actor not in ALLOWED_ACTORS: actor = None

    return {
        "ts": iso_or_now_z(obj.get("ts")),
        "site": site,
        "stream": stream,
        "card_id": obj["card_id"],
        "device_id": obj.get("device_id","unknown-device"),
        "event": event,
        "actor": actor,
        "topic": topic,
        "firmware": obj.get("firmware"),
        "raw_payload": obj
    }

def supabase_insert(row: dict):
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    r = requests.post(url, headers=headers, data=json.dumps(row), timeout=10)
    r.raise_for_status()

def supabase_upsert_status(row: dict):
    # Upsert on (site, stream, device_id)
    url = f"{SUPABASE_URL}/rest/v1/device_status?on_conflict=site,stream,device_id"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    # Ensure last_seen is set server-side
    row.setdefault("last_seen", datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
    r = requests.post(url, headers=headers, data=json.dumps([row]), timeout=10)
    r.raise_for_status()

def handle_status(topic: str, payload: str):
    # topic formats:
    #   <site>/<stream>/status/<device_id>
    #   <site>/<stream>/status/<device_id>/hb
    parts = topic.split("/")
    if len(parts) < 4:
        return
    site, stream = parts[0], parts[1]
    device_id = parts[3] if len(parts) >= 4 else "unknown"
    is_hb = (len(parts) >= 5 and parts[4] == "hb")

    status = payload.strip()
    rssi = None
    firmware = None

    if is_hb:
        # Treat heartbeat as "online" ping; payload may contain JSON with rssi/fw
        status = "online"
        try:
            obj = json.loads(payload)
            rssi = obj.get("rssi")
            firmware = obj.get("fw") or obj.get("firmware")
        except Exception:
            pass

    # Normalise status strings
    status = "online" if status.lower().startswith("on") else ("offline" if status.lower().startswith("off") else status)

    row = {
        "site": site,
        "stream": stream,
        "device_id": device_id,
        "status": status,
        "rssi": rssi,
        "firmware": firmware,
    }
    supabase_upsert_status(row)

def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected:", rc)
    for t, qos in MQTT_TOPICS + STATUS_TOPICS:
        client.subscribe(t, qos=qos)
        print("Subscribed:", t)

def on_message(client, userdata, msg):
    try:
        raw_bytes = msg.payload  # keep original for safe logging
        raw = raw_bytes.decode("utf-8", errors="replace")

        # Status paths: <site>/<stream>/status/<device_id>[/hb]
        if "/status/" in msg.topic:
            handle_status(msg.topic, raw)
            # include retain flag in logs (handy when clearing retained msgs)
            print(f"STATUS retain={msg.retain} {msg.topic} {raw}")
            return

        # Normal tap/event payloads
        row = parse(msg.topic, raw)
        supabase_insert(row)
        print(f"OK {row['site']} {row['stream']} {row['card_id']} {row['ts']}")
    except Exception as e:
        # Avoid decoding again if something blew up early
        sample = (raw if 'raw' in locals() else str(raw_bytes[:200]))
        print("ERR", e, "topic=", msg.topic, "payload_sample=", sample)


if __name__ == "__main__":
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="timecard-bridge")
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_forever()
