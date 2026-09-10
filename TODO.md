- always make state with Box::leak()?
- jemalloc

TODO:

- think about nats ERROR header

- check clickhouse bug since 26.8 later,
  reproduce by

```sql
CREATE TABLE t (id UInt32, t DateTime64(3,'UTC')) ENGINE=MergeTree ORDER BY id;
INSERT INTO t SELECT 1, toDateTime64('2026-07-16 12:30:45.123',3,'UTC');
SELECT count() FROM t WHERE t = '2026-07-16T12:30:45.123Z' AND id = 1;
```

> Code: 53. DB::Exception: Cannot convert string '2026-07-16T12:30:45.123Z' to type DateTime64(3, 'UTC'). (TYPE_MISMATCH) (version 26.8.2.7 (official build))
