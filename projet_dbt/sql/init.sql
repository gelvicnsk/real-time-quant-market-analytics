CREATE SCHEMA IF NOT EXISTS iot;

-- 1) événements bruts (historique)
CREATE TABLE IF NOT EXISTS iot.raw_iot_events (
  event_id      BIGSERIAL PRIMARY KEY,
  sensor_id     TEXT NOT NULL,
  temperature   DOUBLE PRECISION NOT NULL,
  humidity      DOUBLE PRECISION NOT NULL,
  event_ts      TIMESTAMPTZ NOT NULL,
  ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_ts ON iot.raw_iot_events(event_ts);
CREATE INDEX IF NOT EXISTS idx_raw_events_sensor ON iot.raw_iot_events(sensor_id);

-- 2) métriques par fenêtre (upsert)
CREATE TABLE IF NOT EXISTS iot.iot_metrics_1m (
  sensor_id        TEXT NOT NULL,
  window_start     TIMESTAMPTZ NOT NULL,
  window_end       TIMESTAMPTZ NOT NULL,
  avg_temperature  DOUBLE PRECISION NOT NULL,
  max_temperature  DOUBLE PRECISION NOT NULL,
  avg_humidity     DOUBLE PRECISION NOT NULL,
  events_count     BIGINT NOT NULL,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(sensor_id, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_metrics_window ON iot.iot_metrics_1m(window_start, window_end);

-- 3) alertes (toutes les anomalies)
CREATE TABLE IF NOT EXISTS iot.iot_alerts (
  alert_id      BIGSERIAL PRIMARY KEY,
  sensor_id     TEXT NOT NULL,
  temperature   DOUBLE PRECISION NOT NULL,
  humidity      DOUBLE PRECISION NOT NULL,
  event_ts      TIMESTAMPTZ NOT NULL,
  rule          TEXT NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_ts ON iot.iot_alerts(event_ts);
