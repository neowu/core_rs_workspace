# framework_db review

Reviewed on 2026-10-07. These are proposed changes; no implementation changes have been made.

## Findings

### P1: Discard connections after query timeouts

Code: [connection.rs](../lib/framework_db/src/connection.rs#L49).

After a timeout, cancellation is sent and the connection can return to the pool. The driver documents cancellation as unacknowledged and racy: a delayed cancellation can affect the next borrower, while failed cancellation can leave it waiting behind the original query.

Mark the connection unusable on timeout so the pool discards it. Bound cancellation cleanup as well.

### P1: Include preparation and validation in timeout coverage

Code: [statement preparation](../lib/framework_db/src/connection.rs#L29), [connection validation](../lib/framework_db/src/connection.rs#L82).

Statement preparation runs outside `with_timeout`, and connection validation has no deadline. A stalled server can hold requests and pool slots indefinitely despite the configured five-second timeout. The pool's checkout timeout currently covers waiting for a permit, not the entire checkout process.

Apply deadlines to preparation and validation, and ensure failed or timed-out operations cannot return an unsafe connection to the pool.

### P2: Simplify is_in using one array parameter

Code: [field.rs](../lib/framework_db/src/field.rs#L73).

Empty input generates invalid `IN ()` SQL. Each list length also produces a different cached statement, contradicting the assumption in [the DB spec](../spec/db.md) that statement variants are small and bounded.

Use `column = ANY($n)` with a boxed `Vec<V>`. This handles empty lists, keeps SQL stable across list lengths, and removes per-element boxing. Consider a cache bound for other dynamic query combinations.

### P2: Handle empty updates explicitly

Code: [repository.rs](../lib/framework_db/src/repository.rs#L115).

An empty `updates` vector generates invalid SQL without a `SET` clause. This is reachable through the demo's optional update fields.

Return `Ok(0)` or a clear validation error before acquiring a connection, and document the chosen behavior.

### P2: Quote column identifiers consistently

Code: [field.rs](../lib/framework_db/src/field.rs#L69), [entity macros](../lib/framework_macro/src/entity.rs).

Column names are interpolated directly into conditions, updates, and macro-generated SQL. Reserved names such as `order` or mixed-case identifiers can fail or resolve incorrectly.

Quote and escape identifiers consistently across generated SQL and condition/update builders.

## Smaller simplifications

- Done: placeholder numbers are derived from `params.len()` after push; the mutable counter is removed.
- Done: added `is_null()` alongside `not_null()`. `eq(None)` still generates SQL equality with NULL, which never matches.

## Validation and follow-up

- `cargo test -p framework_db --lib`: all 17 unit tests passed.
- PostgreSQL integration tests remain unverified because container-runtime access was denied.
- When implementing changes, add coverage for timeout recovery, empty lists, empty updates, and quoted identifiers. Update `/spec` with the resulting behavior and design decisions.
