# pyflink_kafka_to_redis.py

import json
import logging
import sys

import redis
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    DataTypes,
    EnvironmentSettings,
    Schema,
    StreamTableEnvironment,
    TableDescriptor,
)
from pyflink.table.expressions import col, date_format, lit
from pyflink.table.window import Tumble


def main():
    # 1. 환경 설정
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(6)
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    # 2. Kafka Source 테이블 생성
    t_env.execute_sql(
        """
    CREATE TABLE events_src (
      display_id   BIGINT,
      uuid         STRING,
      document_id  BIGINT,
      `timestamp`  BIGINT,
      geo_location STRING,
      platform_id  INT,
      event_time   AS TO_TIMESTAMP_LTZ(`timestamp` + 1465876799998, 3),
      WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
      'connector' = 'kafka',
      'topic'     = 'events',
      'properties.bootstrap.servers' = 'kafka-broker-1:29092,kafka-broker-2:29092,kafka-broker-3:29092',
      'properties.group.id'          = 'flink-py-events-redis',
      'scan.startup.mode'            = 'group-offsets',
      'properties.auto.offset.reset' = 'earliest',
      'format'                       = 'json'
    )
    """
    )

    t_env.execute_sql(
        """
    CREATE TABLE page_views_src (
      display_id   BIGINT,
      uuid         STRING,
      document_id  BIGINT,
      `timestamp`  BIGINT,
      geo_location STRING,
      platform_id  INT,
      event_time   AS TO_TIMESTAMP_LTZ(`timestamp` + 1465876799998, 3),
      WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
      'connector' = 'kafka',
      'topic'     = 'page_views',
      'properties.bootstrap.servers' = 'kafka-broker-1:29092,kafka-broker-2:29092,kafka-broker-3:29092',
      'properties.group.id'          = 'flink-py-views-redis',
      'scan.startup.mode'            = 'group-offsets',
      'properties.auto.offset.reset' = 'earliest',
      'format'                       = 'json'
    )
    """
    )

    # 3. 5분 윈도우 집계 (Table API)
    events = t_env.from_path("events_src")
    page_views = t_env.from_path("page_views_src")

    # Click 집계
    # fmt: off
    user_click_cnt_20m = (
        events
            .window(                            # 어떤 방식으로 시간을 자를지 정의
                Tumble.over(lit(20).minutes)     # 길이 20분 Tumbling Window
                .on(col("event_time"))          # 윈도우 기준 시간
                .alias("w")                     # 윈도우 별칭, 윈도우 자체를 하나의 컬럼으로
            )
            .group_by(col("uuid"), col("w"))    # 동일 윈도우, 동일 사용자 별 묶음
            .select(
                col("uuid").alias("click_uuid"),                    # 사용자 ID 
                col("w").end.alias("click_win_end"),  # 윈도우 끝 시각
                col("display_id").count.alias("click20m") # 윈도우 내부 레코드 수 (Count(*) 와 동일)
            )
    )

    # View 집계
    # fmt: off
    user_view_cnt_20m = (
        page_views
            .window(                            # 어떤 방식으로 시간을 자를지 정의
                Tumble.over(lit(20).minutes)     # 길이 20분 Tumbling Window
                .on(col("event_time"))          # 윈도우 기준 시간
                .alias("w")                     # 윈도우 별칭, 윈도우 자체를 하나의 컬럼으로
            )
            .group_by(col("uuid"), col("w"))    # 동일 윈도우, 동일 사용자 별 묶음
            .select(
                col("uuid").alias("view_uuid"),                    # 사용자 ID 
                col("w").end.alias("view_win_end"),  # 윈도우 끝 시각
                col("display_id").count.alias("view20m")  # 윈도우 내부 레코드 수 (Count(*) 와 동일)
            )
    )

    # Join 및 Redis key/value 생성
    # fmt: off
    feature_tbl = (user_click_cnt_20m
        .left_outer_join(
            user_view_cnt_20m,
            (user_click_cnt_20m.click_uuid == user_view_cnt_20m.view_uuid)
            & (user_click_cnt_20m.click_win_end == user_view_cnt_20m.view_win_end),
        )
        .select(
            (
                lit("feat:user:{")                                       # Redis Key 앞부분 고정 prefix
                + user_click_cnt_20m.click_uuid                                 # 사용자 ID
                + lit("}:")                                              # 사용자 ID 끝 부분 고정 suffix
                + date_format(user_click_cnt_20m.click_win_end, "yyyyMMddHHmm") # 윈도우 끝 시각
            ).alias("redis_key"),                                        # 최종 Key 컬럼 이름
            col("click20m"),                                              # 20분 윈도우 내 클릭 수
            user_view_cnt_20m.view20m.if_null(lit(0)).alias("view20m"),     # 20분 윈도우 내 뷰 수 (집계 없을 때 0으로)
    ))

    # 4. 결과를 파이썬에서 직접 Redis로 저장
    redis_client = redis.Redis(host="redis", port=6379, db=0)

    with feature_tbl.execute().collect() as results:
        for row in results:
            redis_key = row[0]
            redis_hash = {"click20m": str(row[1]), "view20m": str(row[2])}
            # Redis HSET
            redis_client.hset(redis_key, mapping=redis_hash)
            print(f"Saved to Redis: {redis_key} -> {redis_hash}")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    main()
