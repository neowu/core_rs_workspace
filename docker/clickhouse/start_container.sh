#!/bin/sh
container run -d --name clickhouse --cpus 4 --memory 2g \
    -v ./config.d/custom_config.xml:/etc/clickhouse-server/config.d/custom_config.xml \
    -v ./users.d/root.xml:/etc/clickhouse-server/users.d/root.xml \
    -e CLICKHOUSE_USER=root \
    -e CLICKHOUSE_PASSWORD=root \
    -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
    --ulimit nofile=262144 \
    clickhouse/clickhouse-server:26.8
