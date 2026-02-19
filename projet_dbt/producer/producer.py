import json
import time
import random
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from faker import Faker

fake = Faker()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "iot_sensors")

# ⏳ Attente Kafka
producer = None
for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print("✅ Connexion Kafka OK")
        break
    except NoBrokersAvailable:
        print("⏳ Kafka pas prêt, retry...")
        time.sleep(5)

if not producer:
    raise RuntimeError("❌ Impossible de se connecter à Kafka")

print("🚀 IoT Producer démarré")

while True:
    event = {
        "sensor_id": fake.uuid4(),
        "temperature": round(random.uniform(10, 90), 2),
        "humidity": round(random.uniform(30, 90), 2),
        "timestamp": int(time.time())
    }

    producer.send(TOPIC, event)
    producer.flush()
    print("📡 Event envoyé:", event)

    time.sleep(2)
