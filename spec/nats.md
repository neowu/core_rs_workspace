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
reader of task names is the shutdown warning each component logs itself (`request still running
...` / `message still running ...`), so the prefix repeated what the line already said at the cost of an interned
string and a wrapper struct per handler.

### A permit is taken before the next request is pulled

`Service` waits for a semaphore permit, then for a message, each raced against shutdown. A saturated
service stops pulling, so a request waits in the broker instead of in a handler-less task, and a
wait on handlers that never finish still sees shutdown and unsubscribes (it used to take the
message first and wait for the permit unraced, so stuck handlers hung shutdown forever). `Consumer`
races its permit wait against shutdown the same way; the pulled message is left unacked and
redelivered after `ack_wait`.

### Link headers are static `HeaderName`s

`ref_id`, `client`, `msg_type` are `HeaderName::from_static` consts. async-nats turns a `&str` key
into a new `HeaderName` on every `get` / `insert` (validate, then copy into `Bytes`), which the
service pays per request for each link header it reads.

### A payload is decoded to text once

The lossy utf-8 view that the request / message log line prints is the one `from_json` parses, so
a valid payload is scanned once and borrowed, never copied.
