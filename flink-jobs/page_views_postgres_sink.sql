-- ===========================================
-- 1. Flink 실행 설정
-- ===========================================
SET 'execution.runtime-mode' = 'streaming';
SET 'restart-strategy' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '5';
SET 'restart-strategy.fixed-delay.delay' = '10s';
SET 'execution.checkpointing.interval' = '10s';
SET 'parallelism.default' = '3';

-- ===========================================
-- 2. Kafka Source Table (Page Views)
-- ===========================================
CREATE TABLE page_views_src (
  uuid          STRING,
  document_id   BIGINT,
  `timestamp`   BIGINT,
  geo_location  STRING,
  traffic_source INT,
  platform_id INT,
  event_time    AS TO_TIMESTAMP_LTZ(`timestamp` + 1465876799998, 3),
  act_prod_time TIMESTAMP(6),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic'     = 'page_views',
  'properties.bootstrap.servers' = 'kafka-broker-1:29092,kafka-broker-2:29092,kafka-broker-3:29092',
  'properties.group.id'          = 'flink-sql-page-views',
  'scan.startup.mode'            = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format'                       = 'json'
);

-- ===========================================
-- 3. PostgreSQL Sink Table
-- ===========================================
CREATE TABLE page_views_sink (
  uuid           STRING,
  document_id    BIGINT,
  `timestamp`    BIGINT,
  geo_location   STRING,
  traffic_source INT,
  platform_id INT,
  event_time TIMESTAMP(3),
  act_prod_time TIMESTAMP(6),
  act_load_time TIMESTAMP(6),
  PRIMARY KEY (uuid, document_id) NOT ENFORCED      -- Not Enforced로 하면 Upsert, 아예 PK 안쓰면 append 모드로 중복 key들어가면 오류 발생
)
WITH (
  'connector' = 'jdbc',
  'url'       = 'jdbc:postgresql://postgresql:5432/kafka_data',
  'table-name'= 'page_views',
  'username'  = 'admin',
  'password'  = 'admin',
  'sink.buffer-flush.max-rows' = '1000',
  'sink.buffer-flush.interval' = '3s'
);

-- ===========================================
-- 4. Streaming Insert Job
-- ===========================================
INSERT INTO page_views_sink
SELECT 
  uuid, 
  document_id, 
  `timestamp`, 
  geo_location, 
  traffic_source, 
  platform_id,
  event_time,
  act_prod_time,
  PROCTIME() AS act_load_time
FROM page_views_src;