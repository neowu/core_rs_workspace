# DB design

Code: [`framework_db/src/repository.rs`](../lib/framework_db/src/repository.rs),
[`framework_db/src/connection.rs`](../lib/framework_db/src/connection.rs),
[`framework_db/src/transaction.rs`](../lib/framework_db/src/transaction.rs),
[`framework/src/pool.rs`](../lib/framework/src/pool.rs) · siblings:
[`benchmark/http_server.md`](benchmark/http_server.md)

## Design decisions

### Repository statements are prepared once per connection

Every `repository` call executes a prepared `Statement` from the connection's statement cache, never
a sql string: tokio-postgres prepares a string on every call, an extra round trip per query.
Repository sql comes from the entity macros or the condition builder, so the set of distinct
statements is small and bounded. To keep it bounded, `is_in` binds the list as one array param
(`column = ANY($n)`), so sql does not vary by list size.

### Empty `is_in` values and empty `updates` are rejected

Both return an exception rather than generating invalid sql or silently matching nothing.
`is_in` returns `Result`, so the error surfaces where the condition is built.

### Null conditions use `is_null` / `not_null`

`eq(None)` binds NULL and generates `column = $n`, which never matches in SQL. Match NULL with
`is_null()`.

### Connections with any error are discarded

Any statement error, timeout, or statement future dropped before completion (e.g. by the caller's
own timeout or `select!`) marks the connection broken, it is dropped at release instead of
returned to the pool. Error types are not distinguished, log / trace explain the cause. This covers
a timed out query still in flight (cancel is best effort and racy, reuse could stall or cancel the
next borrower's query), a cancelled statement whose sent query still runs and may abort the
transaction, and a failed statement inside a transaction, the trade off is a reconnect
after expected errors such as unique violation.

The raw sql helpers in `database.rs` are not cached, their sql is arbitrary and the cache has no
eviction.

### Transactions are explicit guards, statements take an `Executor`

`db.begin()` returns a `Transaction` that pins one pooled connection, ended by `commit()` or
`rollback()`. `repository` and `database` functions take `impl Executor`: `&db` checks out a
connection per call, `&mut tx` runs on the pinned one, so the same functions serve both and which
statements are in a transaction is visible at the call site. A closure style
(`db.transaction(|tx| ..)`) was rejected, async closures borrowing `tx` cannot express a `Send`
future. No nested transactions / savepoints, no isolation level options yet.

### Transaction failure handling

- Dropped without commit or rollback (e.g. early return by `?`): the connection is marked broken
  and discarded, never returned to the pool mid transaction.
- A failed or timed out statement marks the connection broken (see above), every later statement
  and `commit()` in that transaction fail fast. This also guards against postgres turning `COMMIT`
  of an aborted transaction into a silent rollback. `rollback()` on a broken connection returns ok,
  the discarded connection is rolled back by the server.
- A transaction holds its connection until it ends, avoid awaiting non-db work inside it.

### Pool lifetime counts from creation

A resource keeps its `created_time` across checkouts. `max_life_time` is its physical age: an
expired resource is dropped at checkout and not returned to the pool at release. `max_valid_window`
is idle time since the last return, past it the resource is validated before reuse.
Within `max_valid_window` validation is skipped unless the resource was closed while idle
(`is_closed`), then it goes through `is_valid` and is dropped.

### Checkout timeout covers the whole checkout

`checkout_timeout` bounds permit wait, creation and validation together. A connect stalled in auth
or a validation hung on a half-open socket fails the checkout instead of holding a permit forever.
