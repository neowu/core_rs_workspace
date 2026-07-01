CREATE TABLE IF NOT EXISTS log.trace
(
    timestamp DateTime64(3, 'UTC'),
    id String,
    app LowCardinality(String),
    error_code LowCardinality(Nullable(String)),
    content String CODEC(ZSTD(3)),
    INDEX idx_id id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_error_code error_code TYPE bloom_filter(0.01) GRANULARITY 1,
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (toStartOfHour(timestamp), app)
TTL timestamp + INTERVAL 30 DAY
