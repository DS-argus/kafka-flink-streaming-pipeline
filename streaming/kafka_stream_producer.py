#!/usr/bin/env python3

import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pytz

KST = pytz.timezone("Asia/Seoul")

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient


# 설정 파일 로드
def load_config(config_path: str) -> dict:
    """YAML 설정 파일을 로드합니다."""
    try:
        with open(config_path, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        print(f"❌ 설정 파일을 찾을 수 없습니다: {config_path}")
        return {}
    except yaml.YAMLError as e:
        print(f"❌ YAML 파일 파싱 오류: {e}")
        return {}


# 설정 로드
CONFIG = load_config("kafka_config.yml")


# 현재 컨테이너의 플랫폼 설정 가져오기
PLATFORM_NAME = os.getenv("PLATFORM_NAME")  # desktop, mobile, tablet
if not PLATFORM_NAME:
    raise ValueError("PLATFORM_NAME 환경변수가 설정되지 않았습니다.")

platform_config = (
    CONFIG.get("environment", {}).get("platforms", {}).get(PLATFORM_NAME, {})
)
if not platform_config:
    raise ValueError(f"플랫폼 '{PLATFORM_NAME}'에 대한 설정을 찾을 수 없습니다.")

# 플랫폼별 설정
PLATFORM_ID = int(platform_config.get("platform_id"))
if not PLATFORM_ID:
    raise ValueError(f"플랫폼 '{PLATFORM_NAME}'의 platform_id가 설정되지 않았습니다.")

SELECTED_PLATFORMS = platform_config.get("selected_platforms")
if not SELECTED_PLATFORMS:
    raise ValueError(
        f"플랫폼 '{PLATFORM_NAME}'의 selected_platforms가 설정되지 않았습니다."
    )

# 공통 설정
ENV_CONFIG = CONFIG.get("environment", {})
START_DATE = ENV_CONFIG.get("start_date")
if not START_DATE:
    raise ValueError("start_date가 설정되지 않았습니다.")

END_DATE = ENV_CONFIG.get("end_date")
if not END_DATE:
    raise ValueError("end_date가 설정되지 않았습니다.")

TIME_SCALE_FACTOR = float(ENV_CONFIG.get("time_scale_factor"))
if TIME_SCALE_FACTOR is None:
    raise ValueError("time_scale_factor가 설정되지 않았습니다.")

# Kafka 설정
KAFKA_CONFIG = CONFIG.get("kafka", {})
KAFKA_BOOTSTRAP_SERVERS = KAFKA_CONFIG.get("bootstrap_servers")
if not KAFKA_BOOTSTRAP_SERVERS:
    raise ValueError("kafka.bootstrap_servers가 설정되지 않았습니다.")

STREAMING_CONFIG = CONFIG.get("streaming", {})


# logs 폴더가 없으면 생성
os.makedirs("logs", exist_ok=True)

# 로그 파일 이름 (날짜/시간 포함)
log_filename = f"logs/kafka_stream_{PLATFORM_NAME.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# 로거 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 포맷터 설정
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# 파일 핸들러 (파일에 로그 저장)
file_handler = logging.FileHandler(log_filename, encoding="utf-8")
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

# 콘솔 핸들러 (터미널에 로그 출력)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

# 로거에 핸들러 추가
logger.addHandler(file_handler)
logger.addHandler(console_handler)


class KafkaDataStreamer:
    def __init__(self, bootstrap_servers=None):
        """
        Kafka 데이터 스트리머 초기화

        Args:
            bootstrap_servers: Kafka 브로커 주소 리스트
        """
        # bootstrap_servers 설정
        if bootstrap_servers is None:
            bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS.split(",")

        self.bootstrap_servers = bootstrap_servers
        self.admin_client: Optional[AdminClient] = None
        self.events_producer = None  # events 전용 producer
        self.page_views_producer = None  # page_views 전용 producer
        self.topics = ["events", "page_views"]

        # 현재 플랫폼 설정
        self.platform_id = PLATFORM_ID
        self.platform_name = PLATFORM_NAME

        # 데이터 경로 설정
        self.events_path = Path("rawdata/events_partitioned")
        self.page_views_path = Path("rawdata/page_views_partitioned")

        # 날짜 범위 설정
        self.start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
        self.end_date = datetime.strptime(END_DATE, "%Y-%m-%d")

        # 시간 스케일 팩터 설정
        self.time_scale_factor = TIME_SCALE_FACTOR

        # 컬럼 정의
        self.events_columns = [
            "display_id",
            "uuid",
            "document_id",
            "timestamp",
            "geo_location",
        ]

        self.page_views_columns = [
            "uuid",
            "document_id",
            "timestamp",
            "geo_location",
            "traffic_source",
        ]

        logger.info(f"🚀 {PLATFORM_NAME} 스트리머 초기화 완료")
        logger.info(f"   - 플랫폼 ID: {self.platform_id}")
        logger.info(
            f"   - 날짜 범위: {self.start_date.strftime('%Y-%m-%d')} ~ {self.end_date.strftime('%Y-%m-%d')}"
        )
        logger.info(f"   - 시간 스케일 팩터: {self.time_scale_factor}")

    def connect_kafka(self):
        """Kafka에 연결하고 필요한 컴포넌트들을 초기화"""
        try:
            # Admin 클라이언트 생성
            admin_config = {
                "bootstrap.servers": ",".join(self.bootstrap_servers),
                "client.id": "data_streamer_admin",
            }
            self.admin_client = AdminClient(admin_config)
            logger.info("✅ Kafka Admin 클라이언트 연결 성공")

            # Events 전용 Producer 생성
            producer_config = KAFKA_CONFIG.get("producer", {})
            events_config = producer_config.copy()
            events_config.update(KAFKA_CONFIG.get("events_producer"))
            events_config.update(
                {
                    "bootstrap.servers": ",".join(self.bootstrap_servers),
                }
            )
            self.events_producer = Producer(events_config)

            # Page Views 전용 Producer 생성
            page_views_config = producer_config.copy()
            page_views_config.update(KAFKA_CONFIG.get("page_views_producer"))
            page_views_config.update(
                {
                    "bootstrap.servers": ",".join(self.bootstrap_servers),
                }
            )
            self.page_views_producer = Producer(page_views_config)

            logger.info(
                f"✅ {PLATFORM_NAME} Producer 생성 완료 (Events용, Page Views용)"
            )
            return True

        except Exception as e:
            logger.error(f"❌ Kafka 연결 실패: {e}")
            return False

    def calculate_sleep_duration(
        self, current_timestamp, next_timestamp, time_scale_factor
    ):
        """실제 timestamp 차이에 맞춰서 sleep 시간 계산"""
        if pd.isna(current_timestamp) or pd.isna(next_timestamp):
            return 0.001

        # timestamp 차이 (s)
        time_diff = (next_timestamp - current_timestamp) / 1000

        # 시간 스케일 팩터 적용 (실제 시간을 더 많이 축소)
        sleep_duration = time_diff * time_scale_factor

        # 최소/최대 sleep 시간 제한
        sleep_duration = max(0.001, sleep_duration)

        return sleep_duration

    def get_date_range(self) -> List[datetime]:
        """처리할 날짜 범위 반환"""
        dates = []
        current_date = self.start_date

        while current_date <= self.end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)

        return dates

    def get_parquet_files(self, date: datetime, data_type: str) -> Optional[Path]:
        """특정 날짜의 현재 플랫폼 parquet 파일 경로 반환"""
        date_str = date.strftime("%Y-%m-%d")
        base_path = self.events_path if data_type == "events" else self.page_views_path
        platform_path = (
            base_path / f"platform={self.platform_id}" / f"event_date={date_str}"
        )

        if platform_path.exists():
            parquet_files = list(platform_path.glob("*.parquet"))
            if parquet_files:
                return parquet_files[0]
            else:
                logger.warning(f"⚠️  Parquet 파일이 없습니다: {platform_path}")
        else:
            logger.warning(f"⚠️  경로가 존재하지 않습니다: {platform_path}")

        return None

    def on_send_success(self, err, msg):
        """전송 성공 시 호출되는 콜백 함수"""
        if err is not None:
            logger.error(f"❌ 메시지 전송 실패: {err}")
        else:
            # logger.info(
            #     f"✅ 메시지 전송 성공: 파티션 {msg.partition()}, 오프셋 {msg.offset()}"
            # )
            pass

    def on_send_error(self, err, msg):
        """전송 실패 시 호출되는 콜백 함수"""
        logger.error(f"❌ 메시지 전송 실패: {err}")

    def produce_data_batch(
        self, events_file: Optional[Path], page_views_file: Optional[Path]
    ):
        """배치 방식으로 page_views를 메인으로 스트리밍하고, events는 timestamp 비교로 중간에 삽입"""
        if (events_file is None or not events_file.exists()) and (
            page_views_file is None or not page_views_file.exists()
        ):
            return

        # Producer가 초기화되지 않은 경우 처리
        if self.events_producer is None or self.page_views_producer is None:
            logger.error("❌ Producer가 초기화되지 않았습니다.")
            return

        # Events 전체 데이터 로드 (크기가 작기 때문에)
        events_df = None
        if events_file and events_file.exists():
            events_parquet = pq.ParquetFile(events_file)
            events_df = events_parquet.read(columns=self.events_columns).to_pandas()
            logger.info(f"📦 Events 전체 로드: {len(events_df)}개 레코드")

        # Page Views 배치 반복자 초기화
        page_views_parquet = (
            pq.ParquetFile(page_views_file)
            if page_views_file and page_views_file.exists()
            else None
        )
        page_views_batches = (
            page_views_parquet.iter_batches(
                batch_size=50000, columns=self.page_views_columns
            )
            if page_views_parquet
            else None
        )

        # 현재 배치 데이터
        page_views_df = None
        page_views_idx = 0
        events_idx = 0

        batch_count = 0
        events_count = 0
        page_views_count = 0
        start_time = time.time()
        prev_time = None

        logger.info(f"📤 {PLATFORM_NAME}: 배치 스트리밍 시작")

        try:
            while True:
                # Page Views 배치 로드 (필요한 경우)
                if page_views_df is None or page_views_idx >= len(page_views_df):
                    if page_views_batches:
                        try:
                            page_views_batch = next(page_views_batches)
                            page_views_df = page_views_batch.to_pandas()
                            page_views_idx = 0
                            logger.info(
                                f"📦 Page Views 배치 로드: {len(page_views_df)}개 레코드"
                            )
                        except StopIteration:
                            page_views_df = None
                            page_views_batches = None
                    else:
                        page_views_df = None

                # 더 이상 처리할 데이터가 없으면 종료
                if page_views_df is None and (
                    events_df is None or events_idx >= len(events_df)
                ):
                    break

                # Page Views를 메인으로 스트리밍
                while page_views_df is not None and page_views_idx < len(page_views_df):
                    current_page_views_time = page_views_df.iloc[page_views_idx][
                        "timestamp"
                    ]

                    # Events의 다음 timestamp 확인
                    next_events_time = float("inf")
                    if events_df is not None and events_idx < len(events_df):
                        next_events_time = events_df.iloc[events_idx]["timestamp"]

                    # Page Views timestamp가 Events 다음 timestamp보다 크면 Events 스트리밍
                    if (
                        current_page_views_time > next_events_time
                        and events_df is not None
                        and events_idx < len(events_df)
                    ):
                        # Events 스트리밍 시작
                        while events_df is not None and events_idx < len(events_df):
                            events_time = events_df.iloc[events_idx]["timestamp"]

                            # Events timestamp가 현재 Page Views timestamp보다 작거나 같을 때만 스트리밍
                            if events_time <= current_page_views_time:
                                row = events_df.iloc[events_idx]
                                events_idx += 1
                                events_count += 1

                                # Events 메시지 전송
                                message = row.to_dict()
                                message["platform_id"] = self.platform_id
                                message["act_prod_time"] = datetime.now(KST).strftime(
                                    "%Y-%m-%d %H:%M:%S.%f"
                                )

                                message_json = json.dumps(message, default=str)
                                key = str(message.get("uuid", ""))

                                self.events_producer.produce(
                                    topic="events",
                                    key=key.encode("utf-8"),
                                    value=message_json.encode("utf-8"),
                                    callback=self.on_send_success,
                                )

                                batch_count += 1

                                # 실제 시간 간격 시뮬레이션
                                if prev_time is not None:
                                    sleep_duration = self.calculate_sleep_duration(
                                        prev_time, events_time, self.time_scale_factor
                                    )
                                    time.sleep(sleep_duration)

                                prev_time = events_time
                            else:
                                break

                    # Page Views 데이터 전송
                    row = page_views_df.iloc[page_views_idx]
                    page_views_idx += 1
                    page_views_count += 1

                    # Page Views 메시지 전송
                    message = row.to_dict()
                    message["platform_id"] = self.platform_id
                    message["act_prod_time"] = datetime.now(KST).strftime(
                        "%Y-%m-%d %H:%M:%S.%f"
                    )

                    message_json = json.dumps(message, default=str)
                    key = str(message.get("uuid", ""))

                    self.page_views_producer.produce(
                        topic="page_views",
                        key=key.encode("utf-8"),
                        value=message_json.encode("utf-8"),
                        callback=self.on_send_success,
                    )

                    batch_count += 1

                    # 실제 시간 간격 시뮬레이션
                    if prev_time is not None:
                        sleep_duration = self.calculate_sleep_duration(
                            prev_time, current_page_views_time, self.time_scale_factor
                        )
                        time.sleep(sleep_duration)

                    prev_time = current_page_views_time

                    # 주기적인 poll : local queue full 오류 방지? -> 해도 안되네?
                    if batch_count % 1000 == 0:
                        self.events_producer.poll(0)
                        self.page_views_producer.poll(0)

                # 진행상황 로그
                if batch_count % 100000 == 0:
                    elapsed_time = time.time() - start_time
                    logger.info(
                        f"  {PLATFORM_NAME} - 전송 진행: {batch_count:,} "
                        f"(Events: {events_count:,}, Page Views: {page_views_count:,}) "
                        f"경과시간: {elapsed_time:.2f}초"
                    )

        except Exception as e:
            logger.error(f"❌ 배치 처리 중 오류: {e}")
        finally:
            # 최종 flush
            self.events_producer.flush()
            self.page_views_producer.flush()
            total_time = time.time() - start_time

            logger.info(
                f"✅ {PLATFORM_NAME} - 배치 스트리밍 완료: 총 {batch_count:,}개 메시지 "
                f"(Events: {events_count:,}, Page Views: {page_views_count:,}) "
                f"소요시간: {total_time:.2f}초, 평균 TPS: {batch_count/total_time:.1f}"
            )

    def process_daily_data(self, date: datetime):
        """특정 날짜의 현재 플랫폼 데이터를 배치 방식으로 처리"""
        date_str = date.strftime("%Y-%m-%d")
        logger.info(f"\n{'='*70}")
        logger.info(f"📅 {date_str} {PLATFORM_NAME} 데이터 처리 시작")
        logger.info(f"{'='*70}")

        # Events와 Page Views 파일 경로 가져오기
        events_file = self.get_parquet_files(date, "events")
        page_views_file = self.get_parquet_files(date, "page_views")

        # 데이터 파일이 있는지 확인
        if events_file is None and page_views_file is None:
            logger.warning(
                f"⚠️  {PLATFORM_NAME}: Events와 Page Views 데이터가 모두 없습니다."
            )
            return

        # 배치 방식으로 데이터 처리
        logger.info(f"🚀 {PLATFORM_NAME}: 배치 처리 시작")
        self.produce_data_batch(events_file, page_views_file)

        logger.info(f"✅ {date_str} {PLATFORM_NAME} 데이터 처리 완료!")

    def start_streaming(self):
        """데이터 스트리밍 시작"""
        logger.info(f"🚀 {PLATFORM_NAME} Kafka 데이터 스트리밍 시작")

        # Kafka 연결
        if not self.connect_kafka():
            logger.error("❌ Kafka 연결 실패. 프로그램을 종료합니다.")
            return

        # 날짜별 처리
        dates = self.get_date_range()
        logger.info(
            f"📊 처리할 날짜 범위: {len(dates)}일 "
            f"({dates[0].strftime('%Y-%m-%d')} ~ {dates[-1].strftime('%Y-%m-%d')})"
        )

        for i, date in enumerate(dates):
            try:
                # 해당 날짜 데이터 처리
                self.process_daily_data(date)
                logger.info(f"✅ {date.strftime('%Y-%m-%d')} 데이터 전송 완료!")

            except KeyboardInterrupt:
                logger.info("\n사용자 인터럽트로 프로그램을 종료합니다.")
                break
            except Exception as e:
                logger.error(f"❌ 날짜 {date.strftime('%Y-%m-%d')} 처리 중 오류: {e}")
                continue

        logger.info("🎉 모든 데이터 스트리밍 완료!")

        # Producer 종료
        if self.events_producer:
            self.events_producer.flush()
        if self.page_views_producer:
            self.page_views_producer.flush()


def main():
    """메인 함수"""
    print(f"🎯 {PLATFORM_NAME} Kafka 데이터 스트리밍 Producer")
    print("=" * 70)

    # Kafka 서버 설정
    kafka_servers = KAFKA_BOOTSTRAP_SERVERS.split(",")

    print(f"📡 Kafka 서버: {kafka_servers}")
    print(f"📂 Events 데이터 경로: rawdata/events_partitioned/")
    print(f"📂 Page Views 데이터 경로: rawdata/page_views_partitioned/")
    print(f"🏷️  생성할 토픽: events, page_views")
    print(f"📄 로그 파일: {log_filename}")
    print("=" * 70)

    # 시작 확인
    print(f"\n📋 설정 요약:")
    print(f"   - 플랫폼: {PLATFORM_NAME} (ID: {PLATFORM_ID})")
    print(f"   - 날짜 범위: {START_DATE} ~ {END_DATE}")
    print(f"   - 시간 스케일 팩터: {TIME_SCALE_FACTOR}")

    # 자동 시작
    print("\n🚀 스트리밍을 시작합니다...")

    # 스트리머 생성 및 시작
    streamer = KafkaDataStreamer(kafka_servers)
    streamer.start_streaming()


if __name__ == "__main__":
    main()
