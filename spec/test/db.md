# DB e2e test

Start `postgres`, then run `cargo test -p db_test`. Code: [`test/db_test`](../../test/db_test).

- Each test drops and recreates its table, then exercises the `Entity` derive and `repository`
  against it.
- Auto increment id: insert returns the generated id; select, update (including setting a column
  to null) and delete by id.
- Composite id: insert, `insert_ignore` and `upsert` on conflict, update, select and delete by
  both keys, and delete all.
- Date: timestamp and date columns round trip, including values before the epoch and nullable
  columns; stored text is checked with raw SQL.

The DB contract is specified in [db.md](../db.md).
