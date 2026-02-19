import os
import requests
import psycopg2
from fastapi import FastAPI, Query

CLICKHOUSE_HTTP_URL = os.getenv("CLICKHOUSE_HTTP_URL", "http://clickhouse:8123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "market")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "market")
PG_USER = os.getenv("POSTGRES_USER", "market")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "market")

app = FastAPI(title="Market Platform API")

def ch_query(sql: str) -> str:
    # Retour texte TSV
    url = f"{CLICKHOUSE_HTTP_URL}/?database={CLICKHOUSE_DB}&query={requests.utils.quote(sql)}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.text

def pg_log(question: str, generated_sql: str | None):
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO nl_queries(question, generated_sql) VALUES (%s, %s)",
                (question, generated_sql),
            )
    finally:
        conn.close()

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/latest")
def latest(symbol: str = Query("TSLA"), limit: int = Query(10, ge=1, le=500)):
    sql = f"""
    SELECT symbol, ts, close, sma_5, return_log, volume
    FROM market_ticks
    WHERE symbol = '{symbol}'
    ORDER BY ts DESC
    LIMIT {limit}
    FORMAT JSON
    """
    pg_log(f"latest({symbol},{limit})", sql.strip())
    url = f"{CLICKHOUSE_HTTP_URL}/?database={CLICKHOUSE_DB}&query={requests.utils.quote(sql)}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

@app.get("/daily_avg")
def daily_avg(symbol: str = Query("TSLA"), days: int = Query(7, ge=1, le=365)):
    sql = f"""
    SELECT
      symbol,
      toDate(ts) AS day,
      avg(close) AS avg_close,
      avg(volume) AS avg_volume
    FROM market_ticks
    WHERE symbol = '{symbol}'
      AND ts >= now() - INTERVAL {days} DAY
    GROUP BY symbol, day
    ORDER BY day DESC
    FORMAT JSON
    """
    pg_log(f"daily_avg({symbol},{days})", sql.strip())
    url = f"{CLICKHOUSE_HTTP_URL}/?database={CLICKHOUSE_DB}&query={requests.utils.quote(sql)}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()
