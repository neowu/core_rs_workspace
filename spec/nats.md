# Nats design

Code: [`framework_nats/src/service.rs`](../lib/framework_nats/src/service.rs),
[`framework_nats/src/consumer.rs`](../lib/framework_nats/src/consumer.rs) · siblings:
[`task_executor.md`](task_executor.md), [`benchmark/nats_api_server.md`](benchmark/nats_api_server.md)

## Design decisions

### The task name is the subject

`Service` and `Consumer` each own a `TaskExecutor`, and spawn every request / message under the
subject it arrived on — the `&'static str` handler map key, taken with `get_key_value`. There is no
per-handler task name.

A `request:` / `message:` prefix was tried and dropped: the executor is never shared, and the only
reader of task names is the shutdown warning each component logs itself (`request aborted` /
`message aborted`), so the prefix repeated what the line already said at the cost of an interned
string and a wrapper struct per handler.
