import os
import json
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
import psycopg2

BROKER_HOST = os.getenv("MQTT_HOST", "mosquitto")
BROKER_PORT = int(os.getenv("MQTT_INTERNAL_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_USE_TLS = os.getenv("MQTT_USE_TLS", "false").strip().lower() == "true"
SHELLY_TOPIC = os.getenv("SHELLY_TOPIC", "test/topic")

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_INTERNAL_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_CONNECT_RETRIES = int(os.getenv("POSTGRES_CONNECT_RETRIES", "5"))
DB_CONNECT_DELAY_SECONDS = float(os.getenv("POSTGRES_CONNECT_DELAY_SECONDS", "2"))


# Establish connection to the Postgres database
def get_connection():
    connection_error = None

    for attempt in range(1, DB_CONNECT_RETRIES + 1):
        try:
            return psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )
        except psycopg2.OperationalError as exc:
            connection_error = exc
            print(
                "Postgres connection failed "
                f"(attempt {attempt}/{DB_CONNECT_RETRIES}): {exc}"
            )
            if attempt < DB_CONNECT_RETRIES:
                time.sleep(DB_CONNECT_DELAY_SECONDS)

    raise connection_error


# Insert or update Shelly device information in the `shelly_devices` table
def upsert_device(conn, device_id, name=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO shelly_devices (source, device_id, name)
            VALUES (%s, %s, %s)
            ON CONFLICT (source, device_id)
            DO UPDATE SET
                name = COALESCE(EXCLUDED.name, shelly_devices.name)
            """,
            ("shelly", device_id, name),
        )


# Insert the raw Shelly message into the `shelly_raw_messages` table for auditing and debugging purposes
def insert_raw_message(conn, device_id, topic, payload_obj, event_time=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO shelly_raw_messages (device_id, topic, payload, event_time)
            VALUES (%s, %s, %s::jsonb, %s)
            """,
            (device_id, topic, json.dumps(payload_obj), event_time),
        )


# Insert a Shelly device measurement into the `shelly_measurements` table for timeseries analysis
def insert_measurement(conn, device_id, metric, value, unit=None, event_time=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO shelly_measurements (device_id, metric, value, unit, event_time)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (device_id, metric, value, unit, event_time),
        )


# Connect to the MQTT broker and subscribe to the topic
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code:", rc)

    if rc == 0:
        client.subscribe(SHELLY_TOPIC, qos=0)
        print(f"Subscribed to topic: {SHELLY_TOPIC}")
    else:
        print("Shelly connection refused. Check username, password, and topic.")


# Check if a metric presents in the payload and insert it into the `shelly_measurements` table
def maybe_insert_metric(conn, device_id, metric, value, unit, event_time):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        insert_measurement(conn, device_id, metric, float(value), unit, event_time)
        print(f"Inserted measurement: {device_id} | {metric}={value} {unit or ''}")


def parse_device_id(topic):
    # Expected Shelly topic examples:
    # - shellyplugsg3-xxxx/status/switch:0
    # - shellypro3em-xxxx/status/em:0
    if not topic:
        return None
    parts = topic.split("/")
    if len(parts) < 2:
        return None
    device_id = parts[0]
    if parts[1] != "status":
        return None
    return device_id


# Extract the event time from the payload if available, otherwise use the current time
def resolve_event_time(payload_obj):
    default_event_time = datetime.now(timezone.utc)

    if not isinstance(payload_obj, dict):
        return default_event_time

    for key in ("aenergy", "ret_aenergy"):
        section = payload_obj.get(key)
        if isinstance(section, dict):
            minute_ts = section.get("minute_ts")
            if isinstance(minute_ts, (int, float)):
                return datetime.fromtimestamp(minute_ts, tz=timezone.utc)

    return default_event_time


def insert_plug_metrics(conn, device_id, payload_obj, event_time):
    maybe_insert_metric(conn, device_id, "apower", payload_obj.get("apower"), "W", event_time)

    aenergy = payload_obj.get("aenergy", {})
    if isinstance(aenergy, dict):
        maybe_insert_metric(conn, device_id, "aenergy_total", aenergy.get("total"), "Wh", event_time)

    ret_aenergy = payload_obj.get("ret_aenergy", {})
    if isinstance(ret_aenergy, dict):
        maybe_insert_metric(conn, device_id, "ret_aenergy_total", ret_aenergy.get("total"), "Wh", event_time)


def insert_pro3em_metrics(conn, device_id, payload_obj, event_time):
    unit_by_metric = {
        "total_current": "A",
        "a_act_power": "W",
        "b_act_power": "W",
        "c_act_power": "W",
        "total_act_power": "W",
        "a_aprt_power": "VA",
        "b_aprt_power": "VA",
        "c_aprt_power": "VA",
        "total_aprt_power": "VA",
    }

    for metric, unit in unit_by_metric.items():
        maybe_insert_metric(conn, device_id, metric, payload_obj.get(metric), unit, event_time)

# Process incoming MQTT messages, extract relevant data, and store it in the database
def on_message(client, userdata, msg):
    payload_text = msg.payload.decode()
    print(f"Received message on {msg.topic}: {payload_text}")

    try:
        payload_obj = json.loads(payload_text)
    except json.JSONDecodeError:
        payload_obj = {"raw_payload": payload_text}

    # Get the device_id from the topic (e.g., "shellyplugsg3-xxxx" from "shellyplugsg3-xxxx/status/switch:0")
    device_id = parse_device_id(msg.topic)
    if not device_id:
        print("Skipping message: no device_id derived from topic")
        return

    # Get the event time from the payload if available, otherwise use the current time
    event_time = resolve_event_time(payload_obj)

    with get_connection() as conn:
        # Upsert Shelly device information in the PostgreSQL database to ensure we have a record of this device
        upsert_device(conn, device_id=device_id, name=device_id)
        print(f"Upserted device: {device_id}")

        # Insert the raw Shelly message into the `shelly_raw_messages` table for auditing and debugging purposes
        insert_raw_message(
            conn,
            device_id=device_id,
            topic=msg.topic,
            payload_obj=payload_obj,
            event_time=event_time
        )

        # Insert metrics based on the device type (plug or pro3em) and available fields in the payload
        insert_plug_metrics(conn, device_id, payload_obj, event_time)
        insert_pro3em_metrics(conn, device_id, payload_obj, event_time)


# Set up MQTT client
client = mqtt.Client()
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

if MQTT_USE_TLS:
    client.tls_set()

print("Starting Shelly ingestor...")
print(f"MQTT_HOST={BROKER_HOST}")
print(f"MQTT_INTERNAL_PORT={BROKER_PORT}")
print(f"MQTT_USERNAME={'set' if MQTT_USERNAME else 'not set'}")
print(f"MQTT_USE_TLS={MQTT_USE_TLS}")
print(f"SHELLY_TOPIC={SHELLY_TOPIC}")

# Connect to the MQTT broker and start the loop to process messages
client.connect(BROKER_HOST, BROKER_PORT, 60)
client.loop_forever()
