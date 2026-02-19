-- Evénements bruts (ingérés depuis Kafka via Flink)
CREATE TABLE IF NOT EXISTS public.raw_sensor_events (
  event_id      BIGSERIAL PRIMARY KEY,
  sensor_id     TEXT NOT NULL,
  temperature   DOUBLE PRECISION NOT NULL,
  humidity      DOUBLE PRECISION NOT NULL,
  event_ts      TIMESTAMPTZ NOT NULL,
  ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Agrégats minute (upsert : (sensor_id, window_start))
CREATE TABLE IF NOT EXISTS public.sensor_agg_1m (
  sensor_id     TEXT NOT NULL,
  window_start  TIMESTAMPTZ NOT NULL,
  window_end    TIMESTAMPTZ NOT NULL,
  cnt           BIGINT NOT NULL,
  avg_temp      DOUBLE PRECISION NOT NULL,
  max_temp      DOUBLE PRECISION NOT NULL,
  min_temp      DOUBLE PRECISION NOT NULL,
  avg_humidity  DOUBLE PRECISION NOT NULL,
  max_humidity  DOUBLE PRECISION NOT NULL,
  min_humidity  DOUBLE PRECISION NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (sensor_id, window_start)
);

-- Anomalies détectées (insert-only)
CREATE TABLE IF NOT EXISTS public.sensor_anomalies (
  anomaly_id    BIGSERIAL PRIMARY KEY,
  sensor_id     TEXT NOT NULL,
  window_start  TIMESTAMPTZ NOT NULL,
  window_end    TIMESTAMPTZ NOT NULL,
  rule          TEXT NOT NULL,
  score         DOUBLE PRECISION,
  max_temp      DOUBLE PRECISION,
  avg_temp      DOUBLE PRECISION,
  std_temp      DOUBLE PRECISION,
  max_humidity  DOUBLE PRECISION,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_ts ON public.raw_sensor_events (event_ts);
CREATE INDEX IF NOT EXISTS idx_anom_created ON public.sensor_anomalies (created_at);
