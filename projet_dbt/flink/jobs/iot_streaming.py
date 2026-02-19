from pyflink.table import EnvironmentSettings, TableEnvironment

# ======================
# FLINK ENV
# ======================
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

t_env.get_config().set("execution.runtime-mode", "streaming")

# ======================
# SOURCE : KAFKA (FIX TIMESTAMP + GROUP ID)
# ======================
t_env.execute_sql("""
CREATE TABLE kafka_events (
  sensor_id STRING,
  temperature DOUBLE,
  humidity DOUBLE,
  `timestamp` BIGINT,
  event_time AS TO_TIMESTAMP_LTZ(`timestamp` * 1000, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot_sensors',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-iot',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
)
""")

# ======================
# SINK : POSTGRES
# ======================
t_env.execute_sql("""
CREATE TABLE pg_raw_events (
  sensor_id STRING,
  temperature DOUBLE,
  humidity DOUBLE,
  event_ts TIMESTAMP(3)
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/iot',
  'table-name' = 'raw_sensor_events',
  'username' = 'admin',
  'password' = 'admin',
  'driver' = 'org.postgresql.Driver'
)
""")

# ======================
# STREAMING JOB (NE RIEN METTRE APRÈS)
# ======================
t_env.execute_sql("""
INSERT INTO pg_raw_events
SELECT
  sensor_id,
  temperature,
  humidity,
  CAST(event_time AS TIMESTAMP(3))
FROM kafka_events
""")

print("🚀 Flink streaming job RUNNING")
