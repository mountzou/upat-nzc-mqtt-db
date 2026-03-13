import os
import json
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


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def upsert_device(source, device_id, dev_eui, name=None):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO devices (source, device_id, dev_eui, name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (source, device_id)
        DO UPDATE SET
            dev_eui = EXCLUDED.dev_eui,
            name = COALESCE(EXCLUDED.name, devices.name)
        """,
        (source, device_id, dev_eui, name),
    )

    conn.commit()
    cur.close()
    conn.close()


def insert_raw_message(source, device_id, topic, payload_obj, event_time=None):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO raw_messages (source, device_id, topic, payload, event_time)
        VALUES (%s, %s, %s, %s::jsonb, %s)
        """,
        (source, device_id, topic, json.dumps(payload_obj), event_time),
    )

    conn.commit()
    cur.close()
    conn.close()


def insert_measurement(device_id, metric, value, unit=None, event_time=None):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO measurements (device_id, metric, value, unit, event_time)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (device_id, metric, value, unit, event_time),
    )

    conn.commit()
    cur.close()
    conn.close()


def maybe_insert_metric(device_id, metric, value, unit, event_time):
    if value is not None:
        insert_measurement(device_id, metric, value, unit, event_time)
        print(f"Inserted measurement: {device_id} | {metric}={value} {unit or ''}")


def on_connect(client, userdata, flags, rc):
    print("Connected to TTN MQTT with result code:", rc)

    if rc == 0:
        client.subscribe(TTN_TOPIC)
        print(f"Subscribed to TTN topic: {TTN_TOPIC}")
    else:
        print("TTN connection refused. Check username, password, and topic.")


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

    upsert_device(
        source="ttn",
        device_id=device_id,
        dev_eui=dev_eui,
        name=device_id
    )
    print(f"Upserted device: {device_id} ({dev_eui})")

    insert_raw_message(
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

    maybe_insert_metric(device_id, "temperature", environmental.get("temperature"), "C", event_time)
    maybe_insert_metric(device_id, "relative_humidity", environmental.get("relative_humidity"), "%", event_time)
    maybe_insert_metric(device_id, "co2", co2_data.get("co2"), "ppm", event_time)
    maybe_insert_metric(device_id, "voc", voc_data.get("voc"), None, event_time)
    maybe_insert_metric(device_id, "pm1", pm_mc.get("pm1"), "ug/m3", event_time)
    maybe_insert_metric(device_id, "pm25", pm_mc.get("pm25"), "ug/m3", event_time)
    maybe_insert_metric(device_id, "pm4", pm_mc.get("pm4"), "ug/m3", event_time)
    maybe_insert_metric(device_id, "pm10", pm_mc.get("pm10"), "ug/m3", event_time)


client = mqtt.Client()
client.username_pw_set(TTN_USERNAME, TTN_PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

print("Starting TTN ingestor...")
print(f"TTN_MQTT_HOST={TTN_MQTT_HOST}")
print(f"TTN_MQTT_PORT={TTN_MQTT_PORT}")
print(f"TTN_USERNAME={TTN_USERNAME}")
print(f"TTN_TOPIC={TTN_TOPIC}")

client.connect(TTN_MQTT_HOST, TTN_MQTT_PORT, 60)
client.loop_forever()
