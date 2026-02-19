from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# 1) Source Kafka avec event_time + watermark
table_env.execute_sql("""
CREATE TABLE iot_stream (
  sensor_id STRING,
  location STRING,
  temperature DOUBLE,
  humidity INT,
  ts STRING,
  event_time AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot_sensors',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-iot',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
)
""")

# 2) Sink Postgres RAW
table_env.execute_sql("""
CREATE TABLE raw_iot (
  sensor_id STRING,
  location STRING,
  temperature DOUBLE,
  humidity INT,
  event_time TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/iot',
  'table-name' = 'raw_iot',
  'username' = 'admin',
  'password' = 'admin'
)
""")

# 3) Sink Postgres AGG (sliding window)
table_env.execute_sql("""
CREATE TABLE iot_agg_1m_5m (
  location STRING,
  window_start TIMESTAMP_LTZ(3),
  window_end TIMESTAMP_LTZ(3),
  avg_temp DOUBLE,
  max_temp DOUBLE,
  avg_humidity DOUBLE,
  cnt BIGINT
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/iot',
  'table-name' = 'iot_agg_1m_5m',
  'username' = 'admin',
  'password' = 'admin'
)
""")

# 4) Insert RAW
table_env.execute_sql("""
INSERT INTO raw_iot
SELECT sensor_id, location, temperature, humidity, event_time
FROM iot_stream
""")

# 5) Insert AGG sliding window 5min / step 1min
table_env.execute_sql("""
INSERT INTO iot_agg_1m_5m
SELECT
  location,
  window_start,
  window_end,
  AVG(temperature) AS avg_temp,
  MAX(temperature) AS max_temp,
  AVG(CAST(humidity AS DOUBLE)) AS avg_humidity,
  COUNT(*) AS cnt
FROM TABLE(
  HOP(TABLE iot_stream, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
)
GROUP BY location, window_start, window_end
""")
