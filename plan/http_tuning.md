# HTTP middleware logging tuning

Status: HTTP batching removed after benchmark review; action/log refactor retained.

Code: [`web/server.rs`](../lib/framework/src/web/server.rs),
[`log.rs`](../lib/framework/src/log.rs), [`log/action.rs`](../lib/framework/src/log/action.rs),
[`web/client_info.rs`](../lib/framework/src/web/client_info.rs).
Contracts: [`action_log.md`](../spec/action_log.md), [`log_mask.md`](../spec/log_mask.md).

## Decision

The experiment shared one action borrow before the handler await and one after it, replacing
logging macros with direct action writes. Per-line timestamps were unchanged.

Review of the [2026-10-03 HTTP report](../report/2026-10-03_http.html) did not establish a consistent
benefit sufficient to justify the extra middleware code. Restore ordinary `log!`, `context!`, and
`stats!` calls. Client-IP parsing logs malformed addresses with `warn!` and returns its normal
fallback client info; it does not pass an action or return a warning for the server to log.

This is a decision to favor readability based on the measured workloads, not a claim that batching
can never help. The benchmark's representative header workload remains available for future tests.

## Retained refactor

- `Action::add_context` accepts `ContextValues`, truncates each value, and appends the entry without
  logging. `context!` formats its trace message and delegates storage to this method.
- `truncate_with_marker`, its tests, and the context limit live in `log/action.rs`.
- `__context` accesses and borrows the task-local directly, like the other logging adapters.
- Trace messages log original values subject to the 10,000-byte message limit. Stored context
  values retain the 1,000-byte limit and UTF-8-safe truncation marker.
- Per-line timestamps, cookie parsing and ordering, deferred masking, warning severity, and
  fallback client addresses retain their existing behavior. Health checks still bypass the action.

## Verification

Run `cargo +nightly fmt`, `cargo clippy --workspace --all-targets`,
`cargo test -p framework --lib`, and `cargo test -p http_test`.
Socket tests require local listener access. Benchmark method: [HTTP benchmark](../spec/benchmark/http_server.md).
