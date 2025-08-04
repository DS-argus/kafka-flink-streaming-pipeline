#!/bin/bash

docker exec flink-jobmanager sql-client.sh -f /scripts/events_postgres_sink.sql
docker exec flink-jobmanager sql-client.sh -f /scripts/page_views_postgres_sink.sql