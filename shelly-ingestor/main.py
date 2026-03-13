import os
import json
import paho.mqtt.client as mqtt
import psycopg2

BROKER_HOST = os.getenv("MQTT_HOST", "mosquitto")
BROKER_PORT = int(os.getenv("MQTT_INTERNAL_PORT", "1883"))
TOPIC = os.getenv("SHELLY_TOPIC", "test/topic")

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


def insert_raw_message(device_id, topic, payload_obj):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO raw_messages (source, device_id, topic, payload, event_time)
        VALUES (%s, %s, %s, %s::jsonb, NOW())
        """,
        ("shelly", device_id, topic, json.dumps(payload_obj)),
    )

    conn.commit()
    cur.close()
    conn.close()


def insert_measurement(device_id, metric, value, unit=None):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO measurements (device_id, metric, value, unit, event_time)
        VALUES (%s, %s, %s, %s, NOW())
        """,
        (device_id, metric, value, unit),
    )

    conn.commit()
    cur.close()
    conn.close()


def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code:", rc)
    client.subscribe(TOPIC)
    print(f"Subscribed to topic: {TOPIC}")


def on_message(client, userdata, msg):
    payload_text = msg.payload.decode()
    print(f"Received message on {msg.topic}: {payload_text}")

    device_id = "demo-device"

    try:
        payload_obj = json.loads(payload_text)
    except json.JSONDecodeError:
        payload_obj = {"raw_payload": payload_text}

    insert_raw_message(
        device_id=device_id,
        topic=msg.topic,
        payload_obj=payload_obj
    )
    print("Inserted into raw_messages")

    if isinstance(payload_obj, dict) and "temperature" in payload_obj:
        insert_measurement(
            device_id=device_id,
            metric="temperature",
            value=payload_obj["temperature"],
            unit="C"
        )
        print("Inserted temperature into measurements")


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_HOST, BROKER_PORT, 60)
client.loop_forever()
