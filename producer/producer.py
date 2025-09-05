import os
import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "city-temperatures")

cities = [
    ("Warsaw", 12.0),
    ("Gdansk", 11.0),
    ("Krakow", 13.0),
    ("Wroclaw", 13.5),
    ("Poznan", 12.5),
    ("Lodz", 12.0),
    ("Berlin", 11.0),
    ("Paris", 14.0),
    ("Madrid", 30.0),
    ("Oslo", 7.0),
]

p = Producer({"bootstrap.servers": BROKER})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        pass  # quiet

# baseline oscillators per city to produce semi-realistic temps
phases = {c[0]: random.random() * 3.14 for c in cities}

while True:
    city, base = random.choice(cities)

    # Diurnal-ish swing
    phases[city] += 0.03
    diurnal = 6.0 * (1 + 0.7 * random.random()) * (random.random() - 0.5)

    # Normal noise
    noise = random.gauss(0, 1.2)

    temp = base + diurnal + noise

    # Rare spike/drop anomaly ~1% of messages
    if random.random() < 0.01:
        temp += random.choice([-1, 1]) * random.uniform(12, 20)

    msg = {
        "city": city,
        "temperature": round(temp, 2),
        "ts_iso": datetime.now(timezone.utc).isoformat()
    }

    p.produce(TOPIC, json.dumps(msg).encode("utf-8"), callback=delivery_report)
    p.poll(0)

    print(f"Produced: {msg}")
    time.sleep(1)
