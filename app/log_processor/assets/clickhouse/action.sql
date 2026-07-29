CREATE TABLE IF NOT EXISTS log.action
(
    timestamp DateTime64(3, 'UTC'),
    id String,
    app LowCardinality(String),
    host LowCardinality(String),
    result Enum8('OK' = 1, 'WARN' = 2, 'ERROR' = 3),
    action LowCardinality(String),
    ref_id Nullable(String),
    ref_ids Array(String),
    error_code LowCardinality(Nullable(String)),
    error_message Nullable(String),
    context Map(LowCardinality(String), String),
    multi_context Map(LowCardinality(String), Array(String)),
    stats Map(LowCardinality(String), Decimal64(3)),

    INDEX idx_id id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_ref_id ref_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_error_code error_code TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_context_keys mapKeys(context) TYPE bloom_filter(0.01),
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (toStartOfHour(timestamp), app, action)
TTL timestamp + INTERVAL 30 DAY
SETTINGS ttl_only_drop_parts = 1
