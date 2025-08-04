-- ===========================================
-- 1. Flink 실행 설정
-- ===========================================
SET 'execution.runtime-mode'                    = 'streaming';
SET 'restart-strategy'                          = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts'     = '5';
SET 'restart-strategy.fixed-delay.delay'        = '10s';
SET 'execution.checkpointing.interval'          = '10s';
SET 'parallelism.default'                       = '3';

-- ===========================================
-- 2. Kafka Source Table (Events)
-- ===========================================
CREATE TABLE events_src (
  display_id   BIGINT,
  uuid         STRING,
  document_id  BIGINT,
  `timestamp`  BIGINT,
  geo_location STRING,
  platform_id INT,
  event_time   AS TO_TIMESTAMP_LTZ(`timestamp` + 1465876799998, 3),
  act_prod_time TIMESTAMP(6),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic'     = 'events',
  'properties.bootstrap.servers' = 'kafka-broker-1:29092,kafka-broker-2:29092,kafka-broker-3:29092',
  'properties.group.id'          = 'flink-sql-events',
  'scan.startup.mode'            = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format'                       = 'json'
);

-- ===========================================
-- 3. PostgreSQL Sink Table
-- ===========================================
CREATE TABLE events_sink (
  display_id   BIGINT,
  uuid         STRING,
  document_id  BIGINT,
  `timestamp`        BIGINT,
  geo_location STRING,
  platform_id INT,
  event_time TIMESTAMP(3),
  act_prod_time TIMESTAMP(6),
  act_load_time TIMESTAMP(6),
  PRIMARY KEY (uuid, display_id) NOT ENFORCED
) 
WITH (
  'connector' = 'jdbc',
  'url'       = 'jdbc:postgresql://postgresql:5432/kafka_data',
  'table-name'= 'events',
  'username'  = 'admin',
  'password'  = 'admin',
  'sink.buffer-flush.max-rows' = '1000',
  'sink.buffer-flush.interval' = '3s'
);

-- ===========================================
-- 4. Streaming Insert Job
-- ===========================================
INSERT INTO events_sink
SELECT 
  display_id, 
  uuid, 
  document_id, 
  `timestamp`, 
  geo_location, 
  platform_id,
  event_time, 
  act_prod_time,
  PROCTIME() AS act_load_time
FROM events_src;