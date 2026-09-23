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
statements is small and bounded.

The raw sql helpers in `database.rs` are not cached, their sql is arbitrary and the cache has no
eviction.

### Pool lifetime counts from creation

A resource keeps its `created_time` across checkouts. `max_life_time` is its physical age: an
expired resource is dropped at checkout and not returned to the pool at release. `max_valid_window`
is idle time since the last return, past it the resource is validated before reuse.
