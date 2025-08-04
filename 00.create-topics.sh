#!/bin/bash
# events 토픽 생성
docker compose exec kafka-broker-1 kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --topic events \
    --partitions 3 \
    --replication-factor 3

# page_views 토픽 생성
docker compose exec kafka-broker-1 kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --topic page_views \
    --partitions 3 \
    --replication-factor 3
