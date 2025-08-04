SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.resource.default-parallelism' = '6';   -- 파티션(3) × 2

-- 1) Kafka Source Tables
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
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'json'
);

CREATE TABLE page_views_src (
  uuid         STRING,
  document_id  BIGINT,
  `timestamp`  BIGINT,
  geo_location STRING,
  traffic_source INT,
  platform_id INT,
  event_time   AS TO_TIMESTAMP_LTZ(`timestamp` + 1465876799998, 3),
  act_prod_time TIMESTAMP(6),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic'     = 'page_views',
  'properties.bootstrap.servers' = 'kafka-broker-1:29092,kafka-broker-2:29092,kafka-broker-3:29092',
  'properties.group.id'          = 'flink-sql-views',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'json'
);

-- 2) Redis Sink (Hash 모드 - Upsert)
--    하나의 테이블로 모든 피처를 적재: redis_key, redis_hash(map)
CREATE TABLE redis_sink (
  redis_key  STRING,
  redis_hash MAP<STRING, STRING>,
  PRIMARY KEY (redis_key) NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'mode'      = 'hash',
  'command'   = 'HSET',        -- HMSET 대응
  'host'      = 'redis',
  'port'      = '6379',
  'hash.tag'  = '{%s}',        -- uuid/doc 등 키 일부를 슬롯 태그로
  'timeout'   = '3000'         -- ms
);

-- ======================================================================
-- 3) 피처 View (5-분 Tumbling 윈도우)
-- ======================================================================

-- (A) 사용자별 클릭 / 뷰 카운트
CREATE VIEW user_click_cnt_5m AS
SELECT
    uuid,
    TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS win_end,
    COUNT(*) AS click5m
FROM events_src
GROUP BY uuid, TUMBLE(event_time, INTERVAL '5' MINUTE);

CREATE VIEW user_view_cnt_5m AS
SELECT
    uuid,
    TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS win_end,
    COUNT(*) AS view5m
FROM page_views_src
GROUP BY uuid, TUMBLE(event_time, INTERVAL '5' MINUTE);

-- -- (B) 문서·디스플레이 클릭 수
-- CREATE VIEW doc_cnt_5m AS
-- SELECT
--     document_id,
--     TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS win_end,
--     COUNT(*) AS doc_cnt_5m
-- FROM events_src
-- GROUP BY document_id, TUMBLE(event_time, INTERVAL '5' MINUTE);

-- CREATE VIEW display_cnt_5m AS
-- SELECT
--     display_id,
--     TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS win_end,
--     COUNT(*) AS display_cnt_5m
-- FROM events_src
-- GROUP BY display_id, TUMBLE(event_time, INTERVAL '5' MINUTE);

-- -- (C) 지역별 Top-1 문서 / 디스플레이
-- CREATE VIEW geo_doc_rank AS
-- SELECT
--     geo_location,
--     win_end,
--     document_id,
--     cnt,
--     ROW_NUMBER() OVER (PARTITION BY geo_location, win_end ORDER BY cnt DESC) AS rn
-- FROM (
--     SELECT
--         geo_location,
--         document_id,
--         TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS win_end,
--         COUNT(*) AS cnt
--     FROM events_src
--     GROUP BY geo_location, document_id, TUMBLE(event_time, INTERVAL '5' MINUTE)
-- );
-- CREATE VIEW geo_top_doc_5m AS
-- SELECT geo_location, win_end, document_id, cnt
-- FROM geo_doc_rank WHERE rn = 1;

-- CREATE VIEW geo_display_rank AS
-- SELECT
--     geo_location,
--     win_end,
--     display_id,
--     cnt,
--     ROW_NUMBER() OVER (PARTITION BY geo_location, win_end ORDER BY cnt DESC) AS rn
-- FROM (
--     SELECT
--         geo_location,
--         display_id,
--         TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS win_end,
--         COUNT(*) AS cnt
--     FROM events_src
--     GROUP BY geo_location, display_id, TUMBLE(event_time, INTERVAL '5' MINUTE)
-- );
-- CREATE VIEW geo_top_display_5m AS
-- SELECT geo_location, win_end, display_id, cnt
-- FROM geo_display_rank WHERE rn = 1;

-- -- (D) 5-분 최다 접속 지역 & 활성 유저 수
-- CREATE VIEW geo_top_5m AS
-- SELECT
--     win_end,
--     FIRST_VALUE(geo_location) OVER (PARTITION BY win_end ORDER BY total DESC) AS top_geo,
--     MAX(total) AS max_visits
-- FROM (
--     SELECT
--         geo_location,
--         TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS win_end,
--         COUNT(*) AS total
--     FROM page_views_src
--     GROUP BY geo_location, TUMBLE(event_time, INTERVAL '5' MINUTE)
-- )
-- GROUP BY win_end;

-- CREATE VIEW active_user_5m AS
-- SELECT
--     TUMBLE_END(event_time, INTERVAL '5' MINUTE)   AS win_end,
--     COUNT(DISTINCT uuid)                          AS active_user_5m
-- FROM (
--     SELECT uuid, event_time FROM events_src
--     UNION ALL
--     SELECT uuid, event_time FROM page_views_src
-- )
-- GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE);

-- ======================================================================
-- 4) Redis 적재
--    - 사용자 / 아이템 : 윈도우 종료 시점 값 저장, TTL 600 초
--    - 기타(지역 Top, 활성 유저) : 키 고정, TTL 300 초
-- ======================================================================

-- 사용자 피처 (click5m + view5m)
INSERT INTO redis_sink
SELECT
    CONCAT('feat:user:{', uc.uuid, '}', ':', DATE_FORMAT(uc.win_end, 'yyyyMMddHHmm')) AS redis_key,
    MAP[
        'click5m', CAST(uc.click5m AS STRING),
        'view5m', CAST(COALESCE(uv.view5m, 0) AS STRING)
    ] AS redis_hash
FROM user_click_cnt_5m uc
LEFT JOIN user_view_cnt_5m uv
ON uc.uuid = uv.uuid AND uc.win_end = uv.win_end;

-- -- 문서 클릭 수
-- INSERT INTO redis_sink
-- SELECT
--     CONCAT('feat:doc:{', document_id, '}', ':', DATE_FORMAT(win_end, 'yyyyMMddHHmm')) AS redis_key,
--     MAP['doc_cnt_5m', CAST(doc_cnt_5m AS STRING)]
-- FROM doc_cnt_5m;

-- -- 디스플레이 클릭 수
-- INSERT INTO redis_sink
-- SELECT
--     CONCAT('feat:display:{', display_id, '}', ':', DATE_FORMAT(win_end, 'yyyyMMddHHmm')) AS redis_key,
--     MAP['display_cnt_5m', CAST(display_cnt_5m AS STRING)]
-- FROM display_cnt_5m;

-- -- 지역별 Top-1 문서
-- INSERT INTO redis_sink
-- SELECT
--     CONCAT('feat:geo_doc:{', geo_location, '}')               AS redis_key,
--     MAP[
--         'top_doc_id', CAST(document_id AS STRING),
--         'doc_clicks_5m', CAST(cnt AS STRING),
--         'updated_at', CAST(win_end AS STRING)
--     ]                                                         AS redis_hash
-- FROM geo_top_doc_5m;

-- -- 지역별 Top-1 디스플레이
-- INSERT INTO redis_sink
-- SELECT
--     CONCAT('feat:geo_display:{', geo_location, '}')           AS redis_key,
--     MAP[
--         'top_display_id', CAST(display_id AS STRING),
--         'display_clicks_5m', CAST(cnt AS STRING),
--         'updated_at', CAST(win_end AS STRING)
--     ]                                                         AS redis_hash
-- FROM geo_top_display_5m;

-- -- 5-분 최다 접속 지역 & 활성 유저 수 (전역 단일 키)
-- INSERT INTO redis_sink
-- SELECT
--     'feat:global:5m' AS redis_key,
--     MAP[
--         'top_geo',         top_geo,
--         'geo_visits_5m',   CAST(max_visits AS STRING),
--         'active_user_5m',  CAST(au.active_user_5m AS STRING),
--         'win_end',         CAST(gt.win_end AS STRING)
--     ] AS redis_hash
-- FROM geo_top_5m AS gt
-- JOIN active_user_5m AS au
-- ON gt.win_end = au.win_end;