# DB e2e test

Start `postgres`, then run `cargo test -p db_test`. Code: [`test/db_test`](../../test/db_test).

- Each test drops and recreates its table, then exercises the `Entity` derive and `repository`
  against it.
- Auto increment id: insert returns the generated id; select, update (including setting a column
  to null) and delete by id.
- Composite id: insert, `insert_ignore` and `upsert` on conflict, update, select and delete by
  both keys, and delete all.
- Transaction: commit persists, rollback and drop discard, commit after a failed statement
  returns error and rolls back, db is usable afterwards.
- Date: timestamp and date columns round trip, including values before the epoch and nullable
  columns; stored text is checked with raw SQL.
- FromRow: `#[derive(FromRow)]` maps a raw multi column select by `#[column(name)]`, including a
  column named differently from its field and a null column; a missing column fails with `DB_ERROR`.
- Error: connect failure and a failed statement return `DB_ERROR` with the pool uri in the message.

The DB contract is specified in [db.md](../db.md).
