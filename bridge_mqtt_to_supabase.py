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
MQTT_TOPICS = [
    ("carrwood/jobcard", 1),
    ("carrwood/timecard", 1),
    ("foxwood/timecard", 1),
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

def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected:", rc)
    for t, qos in MQTT_TOPICS:
        client.subscribe(t, qos=qos)
        print("Subscribed:", t)

def on_message(client, userdata, msg):
    try:
        raw = msg.payload.decode("utf-8", errors="strict")
        row = parse(msg.topic, raw)
        supabase_insert(row)
        print("OK", row["site"], row["stream"], row["card_id"], row["ts"])
    except Exception as e:
        print("ERR", e, "topic=", msg.topic, "payload=", msg.payload[:200])

if __name__ == "__main__":
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="timecard-bridge")
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_forever()
