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

### A permit is taken before the next request is read

`Service` waits for a semaphore permit, then for a message, each raced against shutdown, so a wait
on handlers that never finish still sees shutdown (it used to take the message first and wait for
the permit unraced, so stuck handlers hung shutdown forever). Core NATS has no flow control: a
saturated service stops reading, but the server keeps delivering, and requests wait in the
client's subscription buffer (async-nats default 65,536 per subscription), not in the broker.

### Service shutdown drains

On shutdown each subscription is drained: the server stops routing new requests here, so they
go to other queue group members, and the requests already delivered to this client, which core
NATS never redelivers, are still handled. They are dispatched at once without permits:
`max_concurrency` is a soft limit, and waiting on permits would let a stuck handler hold the buffer
back indefinitely. Once drained the buffer no longer grows; a saturated service bursts its backlog
downstream on release. In-flight handlers then get 30 s; whatever overruns it is abandoned and its
caller times out. Dropping the subscriptions instead left every buffered request
to time out at the caller (measured in a rolling release under saturation: 800 of 9000 failed, each
after the full 10 s request timeout).

### A consumer reads each batch to its end, then drains it

Goal: a release never leaves a message unacked until `ack_wait` (30 min). `Consumer` pulls finite
batches and reads each one to its end without waiting on permits, dispatching whenever a permit is
free and queueing the rest locally; the queue is dispatched before the next pull. On shutdown the
current batch is still read to its end (within `batch_max_wait`), what is left queued is nak'd for
immediate redelivery to another instance or the next release, and in-flight handlers get 30 s;
only handlers still running then are abandoned to `ack_wait`.

- Reading never waits on permits because async-nats ends a batch at `expires + 5s` and drops what
  it still buffers; reading while blocked on busy handlers stranded the rest (the old loop).
- A batch is finite, so once it ends nothing more arrives for it and the queue is everything this
  instance holds.
- Queued messages are nak'd rather than handled locally: shutdown doesn't wait on a backlog behind
  busy handlers, and in a rolling release a running instance takes them over at once. `stream()` (continuous pull) was rejected: it re-pulls on its own, and its buffer
  cannot be drained without dropping messages still in flight (measured up to a full batch
  stranded on shutdown).
- Sizing each pull to free permits was rejected: with fast handlers it pulls a few messages per
  round trip (~6x slower).

### `max_ack_pending` is unlimited

Both consumers set `max_ack_pending = -1`: the framework already bounds in-flight messages (one
batch plus permits for `Consumer`, one batch for `BatchConsumer`), and the server default (1000) silently capped a
`BatchConsumer` batch at 1000 and made it wait out `batch_max_wait` under backlog. Consumers are
created with create-or-update, so an existing durable picks this up on restart.

### One active `BatchConsumer` instance per durable

`BatchConsumer` acks a batch with one ack under `AckPolicy::All`, which also acks every lower
sequence, including messages another instance pulled from the same durable and has not finished,
so a crash there loses them instead of redelivering.

### Link headers are static `HeaderName`s

`ref_id`, `client`, `msg_type` are `HeaderName::from_static` consts. async-nats turns a `&str` key
into a new `HeaderName` on every `get` / `insert` (validate, then copy into `Bytes`), which the
service pays per request for each link header it reads.

### A payload is decoded to text once

The lossy utf-8 view that the request / message log line prints is the one `from_json` parses, so
a valid payload is scanned once and borrowed, never copied.
