# 실행 예:
#   flink run -py feature_batch_update.py --processingDate 2025-08-02

import argparse
from datetime import datetime, timedelta, timezone

from pyflink.common import Row, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, StreamTableEnvironment
from pyflink.table.expressions import col, date_format, lit
from pyflink.table.window import Tumble

# ---------- 파라미터 ---------- #
parser = argparse.ArgumentParser()
parser.add_argument("--processingDate", required=True, help="yyyy-MM-dd")
args = parser.parse_args()

proc_date = datetime.strptime(args.processingDate, "%Y-%m-%d").date()
day_start = datetime.combine(proc_date, datetime.min.time()).replace(
    tzinfo=timezone.utc
)
day_end = day_start + timedelta(days=1)

# Calcite 형식에 맞게 변환 (시간대 정보 제거)
day_start_str = day_start.strftime("%Y-%m-%d %H:%M:%S")
day_end_str = day_end.strftime("%Y-%m-%d %H:%M:%S")

# ---------- Flink 환경 ---------- #
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
t_env = StreamTableEnvironment.create(env)
t_env.get_config().set("pipeline.execution-mode", "BATCH")  # 배치 모드

# ---------- PostgreSQL 소스 ---------- #
postgres_props = {
    "connector": "jdbc",
    "url": "jdbc:postgresql://postgresql:5432/kafka_data",
    "table-name": "events",
    "username": "admin",
    "password": "admin",
}

# Events 테이블 생성
t_env.execute_sql(
    f"""
CREATE TABLE events_src (
  display_id     BIGINT,
  uuid           STRING,
  document_id    BIGINT,
  `timestamp`    BIGINT,
  geo_location   STRING,
  platform_id    INT,
  event_time     TIMESTAMP(3),
  act_prod_time  TIMESTAMP(6),
  act_load_time  TIMESTAMP(6)
) WITH (
  {", ".join([f"'{k}' = '{v}'" for k, v in postgres_props.items()])},
  'table-name' = 'events'
)
"""
)

# Page Views 테이블 생성
postgres_props["table-name"] = "page_views"
t_env.execute_sql(
    f"""
CREATE TABLE page_views_src (
  uuid           STRING,
  document_id    BIGINT,
  `timestamp`    BIGINT,
  geo_location   STRING,
  traffic_source INT,
  platform_id    INT,
  event_time     TIMESTAMP(3),
  act_prod_time  TIMESTAMP(6),
  act_load_time  TIMESTAMP(6)
) WITH (
  {", ".join([f"'{k}' = '{v}'" for k, v in postgres_props.items()])},
  'table-name' = 'page_views'
)
"""
)

# 20분 집계 및 JOIN을 SQL로 직접 수행
features = t_env.sql_query(
    f"""
    SELECT 
        COALESCE(c.uuid, v.uuid) as uuid,
        COALESCE(c.window_end, v.window_end) as window_end,
        COALESCE(c.click20m, 0) as click20m,
        COALESCE(v.view20m, 0) as view20m
    FROM (
        SELECT 
            uuid,
            TIMESTAMP '{day_start.strftime('%Y-%m-%d %H:%M:%S')}' + INTERVAL '20' MINUTE as window_end,
            COUNT(display_id) as click20m
        FROM events_src 
        WHERE event_time >= TIMESTAMP '{day_start_str}' 
        AND event_time < TIMESTAMP '{day_end_str}'
        GROUP BY uuid
    ) c
    RIGHT JOIN (
        SELECT 
            uuid,
            TIMESTAMP '{day_start.strftime('%Y-%m-%d %H:%M:%S')}' + INTERVAL '20' MINUTE as window_end,
            COUNT(document_id) as view20m
        FROM page_views_src 
        WHERE event_time >= TIMESTAMP '{day_start_str}' 
        AND event_time < TIMESTAMP '{day_end_str}'
        GROUP BY uuid
    ) v ON c.uuid = v.uuid AND c.window_end = v.window_end
"""
)

# ---------- PostgreSQL Sink ---------- #
sink_props = {
    "connector": "jdbc",
    "url": "jdbc:postgresql://postgresql:5432/kafka_data",
    "table-name": "features_20m",
    "username": "admin",
    "password": "admin",
}

t_env.execute_sql(
    f"""
CREATE TABLE features_sink (
  uuid  STRING    NOT NULL,
  window_end  TIMESTAMP(3) NOT NULL,
  click20m    BIGINT,
  view20m     BIGINT,
  PRIMARY KEY (uuid, window_end) NOT ENFORCED
) WITH (
  {", ".join([f"'{k}' = '{v}'" for k, v in sink_props.items()])}
)
"""
)

# 결과를 PostgreSQL에 저장
features.execute_insert("features_sink")
