# DB design

Code: [`framework_db/src/repository.rs`](../lib/framework_db/src/repository.rs),
[`framework_db/src/connection.rs`](../lib/framework_db/src/connection.rs),
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

### Timed out connections are discarded

On query timeout the connection is marked broken and dropped at release instead of returned to
the pool. The timed out query is still in flight on that connection and the cancel request is
best effort and racy, so reusing it could stall or cancel the next borrower's query.

The raw sql helpers in `database.rs` are not cached, their sql is arbitrary and the cache has no
eviction.

### Pool lifetime counts from creation

A resource keeps its `created_time` across checkouts. `max_life_time` is its physical age: an
expired resource is dropped at checkout and not returned to the pool at release. `max_valid_window`
is idle time since the last return, past it the resource is validated before reuse.
