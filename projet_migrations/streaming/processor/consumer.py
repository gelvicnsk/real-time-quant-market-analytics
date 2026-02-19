import os, json, math, time
import requests
from datetime import datetime
from dateutil.parser import isoparse
from confluent_kafka import Consumer
from indicators import IndicatorEngine
from signal_engine import SignalEngine

# ---------------- CONFIG ----------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "market_data")
GROUP = os.getenv("KAFKA_GROUP_ID", "stream-processor")

CH_URL = os.getenv("CLICKHOUSE_HTTP_URL", "http://clickhouse:8123")
CH_DB = os.getenv("CLICKHOUSE_DB", "market")
SMA_WINDOW = int(os.getenv("SMA_WINDOW", "5"))

INSERT_URL = f"{CH_URL}/?query=INSERT%20INTO%20{CH_DB}.market_ticks%20FORMAT%20JSONEachRow"
SIGNAL_INSERT_URL = f"{CH_URL}/?query=INSERT%20INTO%20{CH_DB}.signals%20FORMAT%20JSONEachRow"

engine = IndicatorEngine(sma_window=SMA_WINDOW)
signal_engine = SignalEngine()

# ---------------- CANDLES ----------------
candles = {}
WINDOW = 60  # 1 minute


# ---------------- CLICKHOUSE SAFE INSERT ----------------
def safe_post(url, payload, retries=3):
    for i in range(retries):
        try:
            r = requests.post(url, data=payload, timeout=5)
            r.raise_for_status()
            return
        except Exception as e:
            print(f"[clickhouse retry {i+1}] {e}")
            time.sleep(1)


def insert_clickhouse(row: dict):
    payload = json.dumps(row) + "\n"
    safe_post(INSERT_URL, payload)


def insert_signal(row: dict):
    payload = json.dumps(row) + "\n"
    safe_post(SIGNAL_INSERT_URL, payload)


# ---------------- CANDLE BUILDER ----------------
def finalize_old_candles(current_bucket):
    """Flush les anciennes bougies"""
    to_delete = []
    for (symbol, bucket), candle in candles.items():
        if bucket < current_bucket - 1:
            process_candle(symbol, candle)
            to_delete.append((symbol, bucket))

    for k in to_delete:
        del candles[k]


def update_candle(symbol, ts, price, volume):
    bucket = int(ts.timestamp()) // WINDOW
    key = (symbol, bucket)

    finalize_old_candles(bucket)

    if key not in candles:
        candles[key] = {
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volume,
            "ts": datetime.fromtimestamp(bucket * WINDOW)
        }
        return None

    c = candles[key]
    c["high"] = max(c["high"], price)
    c["low"] = min(c["low"], price)
    c["close"] = price
    c["volume"] += volume
    return None


# ---------------- PROCESS LOGIC ----------------
def process_candle(symbol, candle):
    ret, sma = engine.update(symbol, candle["close"])

    out = {
        "symbol": symbol,
        "ts": candle["ts"].strftime("%Y-%m-%d %H:%M:%S"),
        "open": candle["open"],
        "high": candle["high"],
        "low": candle["low"],
        "close": candle["close"],
        "volume": candle["volume"],
        "return_log": float(ret),
        "sma_5": float(sma),
    }

    insert_clickhouse(out)
    print("[candle]", out)

    # -------- SIGNAL --------
    signal, score, mom, vol = signal_engine.update(symbol, candle["close"])

    signal_row = {
        "ts": out["ts"],
        "symbol": symbol,
        "signal": int(signal),
        "score": float(score),
        "momentum": float(mom),
        "volatility": float(vol)
    }

    insert_signal(signal_row)
    print("[signal]", signal_row)


# ---------------- MAIN LOOP ----------------
def main():
    print(f"[processor] {BOOTSTRAP} | topic={TOPIC}")

    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    })

    c.subscribe([TOPIC])

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("[kafka error]", msg.error())
                continue

            try:
                rec = json.loads(msg.value().decode())
            except:
                continue

            # ---------- timestamp compat ----------
            ts_str = rec.get("event_time") or rec.get("timestamp") or rec.get("ts")
            if not ts_str:
                print("⚠️ no timestamp", rec)
                continue

            ts = isoparse(ts_str)

            # ---------- price compat ----------
            price = rec.get("price") or rec.get("close")
            if price is None:
                continue
            price = float(price)

            volume = float(rec.get("volume", 0))
            symbol = rec["symbol"]

            update_candle(symbol, ts, price, volume)

    finally:
        c.close()


if __name__ == "__main__":
    main()
