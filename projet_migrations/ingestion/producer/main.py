import json
import time
import os
import math
import random
from datetime import datetime
from confluent_kafka import Producer

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "market_data")

producer = Producer({"bootstrap.servers": KAFKA})


# ---------------- CONFIG MARCHÉ ----------------
ASSETS = {
    "TSLA": {"price": 190.0, "mu": 0.00015, "sigma": 0.02},
    "AAPL": {"price": 170.0, "mu": 0.00010, "sigma": 0.015},
    "BTC-USD": {"price": 65000.0, "mu": 0.00030, "sigma": 0.04},
}

dt = 1 / 60  # 1 seconde marché ≈ 1 minute trading

# corrélation (crypto plus volatile)
CORRELATION = {
    ("TSLA", "AAPL"): 0.6,
    ("TSLA", "BTC-USD"): 0.2,
    ("AAPL", "BTC-USD"): 0.15,
}

market_trend = 0.0


def correlated_noise(symbol):
    base = random.gauss(0, 1)

    for (a, b), corr in CORRELATION.items():
        if symbol == a:
            base += corr * random.gauss(0, 1)
        if symbol == b:
            base += corr * random.gauss(0, 1)

    return base


def update_market_trend():
    global market_trend
    market_trend += random.gauss(0, 0.00005)
    market_trend = max(min(market_trend, 0.001), -0.001)


def generate_tick(symbol, asset):
    global market_trend

    S = asset["price"]
    mu = asset["mu"] + market_trend
    sigma = asset["sigma"]

    Z = correlated_noise(symbol)

    # GBM formula
    new_price = S * math.exp((mu - (sigma**2) / 2) * dt + sigma * math.sqrt(dt) * Z)

    # volume realistic
    volume = abs(random.gauss(200, 80)) * (1 + sigma * 5)

    asset["price"] = new_price

    return {
        "symbol": symbol,
        "price": round(new_price, 2),
        "volume": round(volume, 2),
        "event_time": datetime.utcnow().isoformat()
    }


print("📈 Quant Market Simulator Started")

while True:
    update_market_trend()

    for symbol, asset in ASSETS.items():
        tick = generate_tick(symbol, asset)

        producer.produce(TOPIC, json.dumps(tick).encode())
        producer.poll(0)

        print(tick)

    time.sleep(1)
