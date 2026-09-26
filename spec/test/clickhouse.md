# ClickHouse e2e test

Start `clickhouse`, then run `cargo test -p clickhouse_test`. Code:
[`test/clickhouse_test`](../../test/clickhouse_test).

- Each test drops and recreates its table. Inserts are async on the server, so tests flush the
  async insert queue before reading back.
- Entity: `Row` round trip with `Enum8`, integers at bounds, and arrays; stored text checked with
  raw SQL.
- Decimal: `Decimal64` round trip, positive and negative.
- Date: `DateTime64` and `Date` round trip, timezone rendering, and out of range dates rejected.
- Map: `Map` columns of string, string array and integer values, including empty maps. A
  repeated key keeps both entries and lookup resolves to the first, which `log_processor` relies on.

The ClickHouse contract is specified in [clickhouse.md](../clickhouse.md).
