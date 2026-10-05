# Kafka review

Reviewed 2026-10-05, against commit `1d21d4e`.

Scope: `lib/framework_kafka`, its framework logging/JSON dependencies, current application
callers, Kafka specs, and `test/kafka_test`. Review only; recommendations below are not implemented.

The first priority is failure handling: the consumer currently commits offsets after failed
handlers, despite the spec's at-least-once requirement. The clearest performance opportunities
are removing redundant message copies, reducing batch latency where appropriate, and draining
completed tasks during dispatch. Producer batching and compression settings need workload
measurements before changing defaults.

## Findings to fix first

### 1. P1 — Failed or panicking handlers can be committed as successfully processed

Evidence: [consumer.rs](../lib/framework_kafka/src/consumer.rs), lines 177–187, 254–291,
320–333, and 347–368; [Kafka contract](../spec/kafka.md).

- Single-message handling discards the action result with `.map(drop)`. A failed message does
  not stop later messages in its key group.
- Bulk handling discards `_result`; decoding failures are logged and omitted from the handler's
  input. Both failed records and a failed bulk operation can be covered by the next commit.
- The outer `join_all(handles)` discards every `JoinError`. A bulk handler panic therefore still
  leads to a commit. For single handlers, `JoinSet::join_all` propagates a child panic and cancels
  remaining group tasks; the outer join then discards that panic too.

For example, a temporary Elasticsearch failure in `app/log_processor` returns an error, but the
consumer still commits the batch. Restarting the same group will not recover those records once
that commit succeeds. Logging an error is not successful processing.

**Recommendation:** return processing outcomes to the consumer, inspect join failures, and only
advance offsets through successfully completed records. The simplest initial policy is to retain
and retry the failed batch, or stop consumption and allow replay, with bounded backoff and an
explicit poison-message policy. Preserve per-key order during retries. A dead-letter policy must
confirm its write before treating the original record as handled.

Merely skipping one commit and continuing the loop is insufficient: a later commit of the
consumer state can skip the same failed records. For selective progress, disable automatic offset
storage and track each partition's contiguous completed offset, committing the next offset only
after all preceding records have succeeded. Different keys can complete out of order within one
partition. Make downstream writes idempotent because retries can repeat successful side effects.

**Validation:** force a single-handler error, bulk-handler error, panic, and malformed JSON, then
restart with the same group and inspect committed offsets and replay. Include a failure followed
by a successful higher offset in the same partition.

### 2. P1 — Slow batches can exceed consumer liveness limits and stall shutdown

Evidence: [consumer.rs](../lib/framework_kafka/src/consumer.rs), lines 165–201, 247, 289,
and 320–333.

The consumer stops polling while all topic handlers finish. `StreamConsumer` still requires
regular polling; its background wakeups do not process messages while the application is awaiting
handlers. A batch that takes longer than `max.poll.interval.ms` can cause partition reassignment
and failed commits. The bundled librdkafka configuration defaults this interval to five minutes.

The budget must cover the entire batch, including semaphore waits and serial work for a hot key.
For illustration, 1,000 messages sharing one key with 400 ms handlers take roughly 400 seconds,
even with `max_concurrency = 100`. This is arithmetic, not a benchmark.

Shutdown is observed during collection and poll-error backoff, but not while waiting for permits
or handlers. One indefinitely pending handler prevents the consumer from stopping.

**Recommendation:** expose the poll interval, define a batch/handler time budget and a bounded
shutdown drain, and size batches against worst-case processing time. Timed-out work must remain
uncommitted under finding 1. If workloads require longer processing, consider a bounded dispatch
queue with partition pause/resume and continued consumer polling; do this only with explicit
completion offsets and rebalance handling. Simply polling ahead with today's offset storage
would make commits unsafe.

**Validation:** a slow hot key, a never-completing handler, shutdown during semaphore wait, and a
second group member joining while work is in progress.

### 3. P2 — Producer queue waits have no upper bound

Evidence: [producer.rs](../lib/framework_kafka/src/producer.rs), lines 30–35 and 66.

`message.timeout.ms = 5000` bounds delivery after enqueueing; `send(record, Timeout::Never)`
allows unlimited retries while the local producer queue is full. The public send operation can
therefore take longer than five seconds under sustained overload. Pending calls also retain
their serialized payloads outside the native queue's memory limits.

**Recommendation:** configure a finite queue timeout separately from delivery timeout, and bound
admitted concurrent sends. Put admission control before serialization to limit retained payload
memory. Return an explicit overload/timeout error. Document that cancelling an already-enqueued
send does not retract the Kafka record, so an application retry may duplicate it.

**Validation:** use a deliberately small producer queue and a broker outage or slow delivery;
verify bounded latency and memory under concurrent sends, then verify recovery.

### 4. P2 — Zero-valued consumer settings can deadlock or spin

Evidence: [consumer.rs](../lib/framework_kafka/src/consumer.rs), lines 88–102, 165–202,
217, 247, and 322.

`max_concurrency = 0` constructs a semaphore that can never admit a handler. Once a batch arrives,
shutdown cannot release the wait. `poll_max_records = 0` makes collection return immediately,
creating an empty loop with no awaited pending operation; it can consume a runtime worker.

**Recommendation:** reject zero concurrency and zero record limits at construction, with a clear
configuration error or fail-fast assertion consistent with existing startup behavior. Define the
meaning of zero wait time explicitly; either reject it or implement an intentional immediate
drain mode rather than relying on selection between simultaneously ready branches.

**Validation:** constructor-level tests for invalid settings; no broker is needed.

### 5. P2 — Asynchronous commit completion is not observed

Evidence: [consumer.rs](../lib/framework_kafka/src/consumer.rs), lines 162 and 185–187.

The immediate return from `commit_consumer_state(Async)` does not confirm broker acknowledgement.
The default consumer context has an empty commit callback, so the error branch here misses
failures reported after enqueueing. This makes persistent commit failure and replay risk hard to
detect. Shutdown also has no application-level confirmation of its final commit outcome.

**Recommendation:** add a consumer context that records commit completions, failures, and latency.
Define a bounded final-commit policy during graceful shutdown. If using a synchronous commit,
perform the blocking operation outside a Tokio worker and bound the wait. Do not replay stale
commit requests blindly after newer progress or a partition reassignment.

**Validation:** cause a commit failure, verify the callback/metric, and restart the same group
after graceful shutdown to check the last acknowledged offsets.

## Performance improvements

### 6. P2 — Remove redundant payload and key allocations

Evidence: [consumer.rs](../lib/framework_kafka/src/consumer.rs), lines 123, 140, 221–222,
259–264, 311–313, 349–350, and 382–387.

Every message is detached, copying its payload, key, topic, and headers. `payload()` then copies
the payload again into a `String` before deserialization. Keyed single-message handling allocates
a key for grouping and another for the public `Message`. Topic grouping also allocates a topic
string for every record, even when that topic already has a map entry.

**Recommendation, in order:**

1. Keep the owned message for the current spawned-task design, but decode from its borrowed
   payload bytes or a validated borrowed `&str`. This removes one full payload allocation without
   introducing a new consumer architecture.
2. Group topics through borrowed lookup and allocate only for a new map entry. Avoid rebuilding
   keys that can be carried through decoding/dispatch.
3. Profile whether decoding directly from `BorrowedMessage` before spawning is worthwhile. It can
   avoid detaching entirely, but moves decoding into collection/dispatch and changes where CPU
   work and action instrumentation occur. It is a larger tradeoff, not an automatic improvement.

Also correct the current lossy UTF-8 behavior: invalid bytes inside a JSON string can become
replacement characters and be accepted as changed data. Strict UTF-8/JSON decoding should route
invalid records through the explicit failure policy. Define tombstone handling separately from
malformed JSON if null payloads are supported.

**Validation:** compare allocation count/bytes and CPU per record with representative small and
large JSON payloads. Test invalid UTF-8, missing payloads, and unchanged valid-message behavior.

### 7. P2 — Bound batch bytes as well as record count

Evidence: [consumer.rs](../lib/framework_kafka/src/consumer.rs), lines 53–60, 166–184,
and 207–229; [log processor configuration](../app/log_processor/src/main.rs), lines 68–75.

The batch has a record limit but no byte limit. The log processor permits 5,000 records, whose
total size can vary greatly. Raw messages, detached copies, decoded objects, and downstream request
buffers can coexist. The semaphore limits handler concurrency; it does not bound the bytes already
collected. Native consumer prefetch is another memory budget.

**Recommendation:** add an application batch-byte threshold alongside record/time thresholds,
and tune native prefetch limits to the service's memory budget. A record that crosses the byte
threshold still needs to be handled; allow one oversized record to make progress or apply an
explicit oversize policy. Count-based limits alone cannot guarantee a fixed RSS ceiling.

**Validation:** a burst of large records, mixed record sizes, and slow handlers; measure peak RSS
and throughput, including native allocations that framework action statistics may not capture.

### 8. P2 — Reap completed group tasks while dispatching

Evidence: [consumer.rs](../lib/framework_kafka/src/consumer.rs), lines 309–333.

The semaphore correctly bounds active group handlers, but completed tasks remain in the `JoinSet`
until every group has been spawned. With many distinct keys or unkeyed records, retained task
bookkeeping grows with the batch size rather than concurrency. A panic is also not inspected until
dispatch finishes. Each unkeyed record additionally gets its own one-element vector and task.

**Recommendation:** interleave completion handling with dispatch using `join_next`/`try_join_next`,
including while waiting for a permit, and inspect every result. Keep the shared semaphore so
single and bulk topics still obey the same limit. Consider bounded in-task futures only if
profiling shows task overhead matters; they change CPU parallelism and logging context behavior.

This is retention of completed task bookkeeping, not a claim that the current code runs more than
`max_concurrency` group handlers at once or retains all completed payloads indefinitely.

**Validation:** a large batch of distinct keys with low concurrency; measure retained task count,
allocation volume, and time to observe a handler panic.

### 9. P2 — Tune collection delay and the barrier across topics to the workload

Evidence: [consumer.rs](../lib/framework_kafka/src/consumer.rs), lines 63–70, 165–185,
and 214–227; [log processor configuration](../app/log_processor/src/main.rs), lines 68–75.

Under light traffic, a message arriving near the start of a collection window waits nearly the
full window before handling: one second by default, three seconds in the log processor. Collection
wait is outside each handler's action timing, so handler elapsed time alone conceals this delay.
All topics then wait for the slowest topic before the next batch is collected.

For the current log processor, there are only three bulk topic handlers and one invocation per
topic per batch. Increasing `max_concurrency` above three cannot increase their handler parallelism.
A slow topic can delay subsequent work on the other two even when permits are available.

**Recommendation:** choose the collection window from the end-to-end latency objective. Benchmark
shorter windows for single-message workloads; bulk sinks may benefit from the existing larger
batches. Prefer separate consumer instances for topics with materially different latency/throughput
requirements before introducing a complex pipeline. Overlapping batches requires completion-aware
offsets and preservation of per-key order across batch boundaries.

**Validation:** sparse traffic, steady load, bursts, and one deliberately slow topic; record
collection wait, batch size, handler time, and end-to-end p50/p95/p99 latency separately.

### 10. P2 — Make producer tuning and ordering policy explicit

Evidence: [producer.rs](../lib/framework_kafka/src/producer.rs), lines 28–35 and 39–70;
[producer loops in tests](../test/kafka_test/tests/bulk_message_test.rs), lines 25–28.

Only bootstrap servers are configurable; compression is fixed to zstd and delivery timeout to
five seconds. Callers that await every send serially have one outstanding delivery per loop and
provide little opportunity for that loop's records to batch together. Concurrent callers can
already share this producer, so the producer itself is not globally serialized.

**Recommendation:** introduce a small producer configuration for queue/delivery budgets, queue
capacity, linger, batch size, compression, and idempotence. For throughput-oriented callers, use
bounded concurrent sends or a batch API that enqueues in input order and then awaits delivery
results. Increasing linger without supplying concurrent records mainly adds latency.

Benchmark zstd against available alternatives with real payloads; compression CPU and network
savings depend on payload size and batch density. Test a small grid of concurrency and linger
settings rather than adopting a universal preset.

The constructor also leaves producer idempotence disabled under the bundled defaults. Retries
can duplicate or reorder records when requests are in flight concurrently. If callers need
per-partition produce order through retries, enable idempotence with compatible settings. This
does not provide exactly-once application side effects or deduplicate application-level retries.

**Validation:** throughput, CPU, compression ratio, queue depth, and acknowledgement latency;
include retry/failover tests that verify same-key ordering and duplicate behavior.

## Smaller improvements and measurement gaps

- **Metrics naming and meaning:** single handling records `kafka_read_entries`, while bulk uses
  `kafka_read_messages`; producer counters increment before delivery succeeds. Use consistent
  names and distinguish attempts, successes, failures, and invalid records. The existing
  `kafka_consumer_lag` is message timestamp age, not broker offset lag; future timestamps and
  `LogAppendTime` do not contribute. Add actual partition lag, queue wait, batch bytes, commit
  errors, and processing latency where useful. Sources: consumer lines 273–279 and 353–359;
  producer lines 63–68.
- **Bulk trace headers:** `collect::<Option<HashSet<_>>>()` loses all collected `ref_id` or `client`
  values if any record lacks that header. If partial provenance is useful, collect available
  values instead and test mixed producers. Source: consumer lines 248–252 and 281–286.
- **Ordering scope:** key groups are topic-wide, so identical keys from different partitions
  serialize unnecessarily. Kafka only defines ordering within a partition. Keep topic-wide
  serialization if intentional; otherwise document partition-and-key ordering before changing
  the grouping key. Source: consumer lines 309–318.
- **Logging:** payload logging and one action per single message have a cost, but the framework
  already bounds log formatting. Do not describe it as an unlimited full-payload log copy. Profile
  action/logging cost after removing the definite copies above, then consider configurable payload
  tracing while preserving error diagnostics.

## Suggested implementation order

1. Fix handler outcomes, panic handling, and commit boundaries; define poison-message behavior.
2. Validate configuration and bound producer admission, processing, and shutdown waits.
3. Observe asynchronous commit results and add the metrics needed for tuning.
4. Remove the extra payload copy, add byte budgets, and reap completed tasks during dispatch.
5. Benchmark batch windows, producer concurrency, compression, and linger. Introduce independent
   topic consumers or pipelining only if measurements justify the complexity.

Keep the current nonblocking `StreamConsumer` and shared handler semaphore. Both are useful design
choices; this review does not justify replacing them with a blocking poll loop or unbounded tasks.

When implementing behavior changes, update [spec/kafka.md](../spec/kafka.md) with the accepted
failure, ordering, shutdown, and configuration contracts, and [spec/test/kafka.md](../spec/test/kafka.md)
with the new coverage. This review changes no implementation or accepted contract, so those specs
remain unchanged here.

## Verification and limits

- Passed `cargo check -p framework_kafka --locked --offline`.
- Passed `cargo check -p kafka_test --tests --locked --offline`.
- Inspected all three existing Kafka integration tests. They cover successful single/bulk delivery
  and the shared concurrency bound, but not committed-offset recovery, repeated-key ordering,
  handler failures/panics, rebalance behavior, or shutdown deadlines. Their semaphore waits have
  no timeout; add bounded waits so regressions fail instead of hanging.
- Broker-backed tests were not run. `container list` was denied by the execution sandbox, so broker
  availability was not established. No throughput or allocation benchmark was run; all proposed
  performance gains remain unmeasured.
- Dependency source checked: locked `rdkafka 0.39.0`, `rdkafka-sys 4.10.0+2.12.1`, and
  `tokio 1.53.2`. macOS uses dynamic linking, and local `pkg-config --modversion rdkafka` reports
  `2.15.1`; benchmark and deployment reports should record the actual native library version
  rather than assume the Cargo lockfile fixes it on every platform.

Upstream references supporting dependency behavior:

- [rdkafka 0.39 StreamConsumer liveness contract](https://docs.rs/rdkafka/0.39.0/rdkafka/consumer/struct.StreamConsumer.html).
- [rdkafka 0.39 FutureProducer queue timeout and delivery behavior](https://docs.rs/rdkafka/0.39.0/rdkafka/producer/struct.FutureProducer.html).
- [rdkafka 0.39 consumer commit callbacks](https://docs.rs/rdkafka/0.39.0/rdkafka/consumer/trait.ConsumerContext.html).
- [Tokio JoinSet completion and panic behavior](https://docs.rs/tokio/latest/tokio/task/struct.JoinSet.html#method.join_all).
- [Bundled librdkafka 2.12.1 configuration](https://github.com/confluentinc/librdkafka/blob/v2.12.1/CONFIGURATION.md).
