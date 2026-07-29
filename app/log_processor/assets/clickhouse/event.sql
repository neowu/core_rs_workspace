CREATE TABLE IF NOT EXISTS log.event
(
    timestamp DateTime64(3, 'UTC'),
    id String,
    app LowCardinality(String),
    client_timestamp DateTime64(3, 'UTC'),
    result Enum8('OK' = 1, 'WARN' = 2, 'ERROR' = 3),
    action LowCardinality(String),
    error_code LowCardinality(Nullable(String)),
    error_message Nullable(String),
    context Map(LowCardinality(String), String),
    stats Map(LowCardinality(String), Decimal64(3)),
    info Map(LowCardinality(String), String) CODEC(ZSTD(3)),

    INDEX idx_id id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_error_code error_code TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_context_keys mapKeys(context) TYPE bloom_filter(0.01),
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (toStartOfHour(timestamp), app, action)
TTL timestamp + INTERVAL 30 DAY
SETTINGS ttl_only_drop_parts = 1
