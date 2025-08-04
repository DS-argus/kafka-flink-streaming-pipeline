#!/bin/bash

# Redis Sink Python Job 제출
docker exec flink-jobmanager \
  flink run \
    -py /scripts/new_redis_sink.py \
    -d