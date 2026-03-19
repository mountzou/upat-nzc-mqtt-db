import os
import json
import time
import psycopg2
import paho.mqtt.client as mqtt

TTN_MQTT_HOST = os.getenv("TTN_MQTT_HOST", "eu1.cloud.thethings.network")
TTN_MQTT_PORT = int(os.getenv("TTN_MQTT_PORT", "1883"))
TTN_USERNAME = os.getenv("TTN_USERNAME")
TTN_PASSWORD = os.getenv("TTN_PASSWORD")
TTN_TOPIC = os.getenv("TTN_TOPIC")

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_INTERNAL_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_CONNECT_RETRIES = int(os.getenv("POSTGRES_CONNECT_RETRIES", "5"))
DB_CONNECT_DELAY_SECONDS = float(os.getenv("POSTGRES_CONNECT_DELAY_SECONDS", "2"))


# Establish connection to the PostgreSQL database
def db_connect():
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


# Insert or update UPAT device information in the `upat_devices` table
def upsert_device(conn, source, device_id, dev_eui, name=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO upat_devices (source, device_id, dev_eui, name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (source, device_id)
            DO UPDATE SET
                dev_eui = EXCLUDED.dev_eui,
                name = COALESCE(EXCLUDED.name, upat_devices.name)
            """,
            (source, device_id, dev_eui, name),
        )


# Insert the raw TTN message into the `upat_raw_messages` table for auditing and debugging purposes
def insert_raw_message(conn, source, device_id, topic, payload_obj, event_time=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO upat_raw_messages (source, device_id, topic, payload, event_time)
            VALUES (%s, %s, %s, %s::jsonb, %s)
            """,
            (source, device_id, topic, json.dumps(payload_obj), event_time),
        )


# Insert a UPAT device measurement into the `upat_measurements` table for timeseries analysis
def insert_measurement(conn, device_id, metric, value, unit=None, event_time=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO upat_measurements (device_id, metric, value, unit, event_time)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (device_id, metric, value, unit, event_time),
        )

# Check if a metric presents in the payload and insert it into the `upat_measurements` table
def maybe_insert_metric(conn, device_id, metric, value, unit, event_time):
    if value is not None:
        insert_measurement(conn, device_id, metric, value, unit, event_time)
        print(f"Inserted measurement: {device_id} | {metric}={value} {unit or ''}")


# Connect to the MQTT broker and subscribe to the topic
def on_connect(client, userdata, flags, rc):
    print("Connected to TTN MQTT with result code:", rc)

    if rc == 0:
        client.subscribe(TTN_TOPIC)
        print(f"Subscribed to TTN topic: {TTN_TOPIC}")
    else:
        print("TTN connection refused. Check username, password, and topic.")


# Process incoming MQTT messages, extract relevant data, and store it in the database
def on_message(client, userdata, msg):
    payload_text = msg.payload.decode()
    print(f"Received TTN message on {msg.topic}")

    payload_obj = json.loads(payload_text)

    end_device_ids = payload_obj.get("end_device_ids", {})
    device_id = end_device_ids.get("device_id")
    dev_eui = end_device_ids.get("dev_eui")
    event_time = payload_obj.get("received_at")

    if not device_id:
        print("Skipping message: no device_id found")
        return

    with db_connect() as conn:
        # Upsert UPAT device information in the database to ensure we have a record of this device
        upsert_device(conn, source="ttn", device_id=device_id, dev_eui=dev_eui, name=device_id)
        print(f"Upserted device: {device_id} ({dev_eui})")

        insert_raw_message(
            conn,
            source="ttn",
            device_id=device_id,
            topic=msg.topic,
            payload_obj=payload_obj,
            event_time=event_time
        )
        print("Inserted raw TTN message")

        decoded = payload_obj.get("uplink_message", {}).get("decoded_payload", {})
        environmental = decoded.get("environmental", {})
        co2_data = decoded.get("co2", {})
        voc_data = decoded.get("voc_index", {})
        pm_mc = decoded.get("pm_MC", {})

        maybe_insert_metric(conn, device_id, "temperature", environmental.get("temperature"), "C", event_time)
        maybe_insert_metric(conn, device_id, "relative_humidity", environmental.get("relative_humidity"), "%", event_time)
        maybe_insert_metric(conn, device_id, "co2", co2_data.get("co2"), "ppm", event_time)
        maybe_insert_metric(conn, device_id, "voc", voc_data.get("voc"), None, event_time)
        maybe_insert_metric(conn, device_id, "pm1", pm_mc.get("pm1"), "ug/m3", event_time)
        maybe_insert_metric(conn, device_id, "pm25", pm_mc.get("pm25"), "ug/m3", event_time)
        maybe_insert_metric(conn, device_id, "pm4", pm_mc.get("pm4"), "ug/m3", event_time)
        maybe_insert_metric(conn, device_id, "pm10", pm_mc.get("pm10"), "ug/m3", event_time)


# Set up MQTT client
client = mqtt.Client()
client.username_pw_set(TTN_USERNAME, TTN_PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

print("Starting TTN ingestor...")
print(f"TTN_MQTT_HOST={TTN_MQTT_HOST}")
print(f"TTN_MQTT_PORT={TTN_MQTT_PORT}")
print(f"TTN_USERNAME={TTN_USERNAME}")
print(f"TTN_TOPIC={TTN_TOPIC}")

# Connect to the MQTT broker and start the loop to process messages
client.connect(TTN_MQTT_HOST, TTN_MQTT_PORT, 60)
client.loop_forever()
