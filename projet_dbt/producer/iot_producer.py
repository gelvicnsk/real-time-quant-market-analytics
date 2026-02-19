import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

SENSORS = ["TEMP_01", "TEMP_02", "TEMP_03"]
LOCATIONS = ["paris", "lyon", "marseille"]

while True:
    event = {
        "sensor_id": random.choice(SENSORS),
        "location": random.choice(LOCATIONS),
        "temperature": round(random.uniform(15, 35), 2),
        "humidity": random.randint(30, 90),
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.send("iot_sensors", event)
    print("Sent:", event)
    time.sleep(1)
