### 실행 방법
```
# 필요한 서비스 실행
docker compose up -d

# Kafka 토픽 생성
./00.create_topics.sh

# 데이터 스트리밍 시작 
./01.start_streaming.sh

# flink postgres sink 실행
./02.postgres_flink.sh

# flink redis sink 실행
./03.redis_flink.sh
```

### 상세
##### 0. 전처리 
- platform, event_date 별로 나누고 timestamp 순으로 정렬해서 parquet으로 저장
- events.csv는 `notebooks/preprocess.ipynb`에서 처리 후 `rawdata/events_partitioned`에 저장
- page_views.csv 90GB 정도라서 AWS에서 처리 
    - S3에 csv 업로드
    - GLUE ETL에 spark job 만들어서 partition 만들어 저장
        - Glue Crawler로 table 만든 다음에 Athena에서 쿼리 날릴 수 있음
    - 폴더 전체를 aws CLI로 다운로드
        - `aws s3 cp s3://data-engineering-project-argus/page_views_partitioned/ ./ --recursive`
        - `rawdata/page_views_partitioned` 에 저장
- 고민해야 할 내용
    - 지금 spark 처리는 사실상 무의미. timestamp 섞이는게 싫어서 for loop을 돌렸더니 병렬처리가 안되는 것 같음
    - 메인은 아니니 나중에 고민

##### 1. 시뮬레이션 (Python)
- page_views, events를 Kafka 브로커로 전송
- platform, 시작일, 스트리밍할 파일, 전송 속도 선택하면 platform 별 컨테이너 실행
    - 현재 tablet 단독만 가능 (메모리 부족)
- 현재 구현은 page_views, events 간의 순서를 보장하기 위해 시간 비교하면서 하나씩 출력 (비효율) + timestamp 차이만큼 sleep
- 실제로 얼마나 이후 작업이 지연되는지 확인하기 위해 실제 send 직전 시간인 'act_prod_time' 데이터에 추가 
- 고민해야 할 내용 
    - parquet load + read + indexing 너무 별로 + sleep도 사실상 의미가 있는지는 잘... 
    - 들어온 데이터 보유 기간 (retention) : 바로 postgres에 넣으니까 1GB되면 지워버려도 될 듯
    - idempotence : 중복데이터가 들어와도 db에 넣을 때 처리?
        - Flink JDBC Sink 설정에 추가 고려
            - 'sink.ignore-delete' = 'true'
            - 'sink.ignore-update' = 'false'
            - 'sink.ignore-insert' = 'false'

##### 2. flink + postgres sink + redis sink
- flink 구성 : jobmanager + taskmanager x 3
    - taskmanager는 각각 slot 4개씩
- postgres sink : `./submit_flink_postgres.sh`
    - 각 topic 별로 parallelism.default = 3으로 설정해서 partition마다 하나의 slot이 담당
    - timestamp를 date 형태로 변환 & 저장되는 실제 시간을 확인하기 위해 'act_load_time' 칼럼 추가
- redis sink
    - 실시간으로 계산할 내용
        - User activity
            - user_click_cnt_5m : 최근 5분 events 클릭 수 
            - user_view_cnt_5m : 최근 5분 page_views 클릭 수 
        - Item Exposure
            - doc_cnt_5m : 최근 5분 document 클릭 수
            - display_cnt_5m : 최근 5분 display 클릭 수
        - Others
            - geo_top_doc_5m : 최근 5분 지역별 클릭 가장 많이 된 document
            - geo_top_display_5m : 최근 5분 지역별 클릭 가장 많이 된 display
            - geo_top_5m : 최근 5분 가장 접속이 많이 이루어지는 지역
            - active_user_5m : 최근 5분 접속한 유저

- 고민해야 할 내용
    - 원래는 컨테이너 시작과 동시에 job 실행시켜서 'kafka->flink->postgres' 만들어 놓은 뒤, 데이터가 kafka로 들어오면 바로 이어지게 하고 싶었으나 안됨
        - 불필요한 행동인가?
    - 따로 주기적으로 offset안해주면 kafka-ui에서는 안보이는 것 같은데 원래 그런가
        - 자동 반영된다고 하는 곳도 있고 아니라는 곳도 있는 것 같고...
    - db index + partitioning
    - flink taskmanager 개수
        - parallelism 관련 옵션?
    - offset 관리

##### 3. Kafka-Prometheus
- kafka-exporter로 prometheus 모니터링
    - https://github.com/danielqsj/kafka_exporter
- sum(rate(kafka_topic_partition_current_offset[1m])) by (topic) : 각 토픽 별 최근 1분 초당 처리량
    - producer 동작 확인
    - platform 3만 했을 때, event는 7.421892360339542, page_view는 528.3542953646505
- sum(kafka_consumergroup_lag) by (consumergroup, topic)
    - postgres_sink 동작 확인
- grafana는 일단 제외


##### 4. postgres 분석 및 Tableau 시각화
- `/analysis` 참고
- postgres jar 다운받으면 잘 연결됨
- 다양하게 시각화 해볼 수 있음
- 고민해야 할 내용
    - 어떤 시각화?
    - 대시보드 구성은 어떻게?
    - 동적으로 구성 가능?


##### 5. airflow로 20m feature 배치 업데이트
- 구현 중
    - flink job 처리할 때 OOM
    - airflow dockerfile 에 flink 추가




