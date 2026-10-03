# NATS review

Reviewed on 2026-10-03. Scope: `lib/framework_nats`, its integration tests, the pinned
`async-nats` 0.50.0 implementation, and existing benchmark reports.

Fix delivery and buffering behavior before tuning small allocations.

## Findings

### [P1] Producer reports success before JetStream confirms storage

[`Producer::send`](../lib/framework_nats/src/producer.rs#L36) awaits publishing but drops the
returned acknowledgement future. Missing streams, server rejection, and acknowledgement
timeouts therefore do not reach the caller.

Await the acknowledgement. For throughput, pipeline a bounded number of acknowledgements
instead of serializing every publish.

### [P1] Batch acknowledgements can acknowledge another worker's unfinished messages

[`BatchConsumer`](../lib/framework_nats/src/consumer.rs#L308) uses `AckPolicy::All`. With multiple
instances sharing a durable, a faster worker can acknowledge messages still being processed
by another worker; a subsequent crash loses their redelivery.

Use explicit per-message acknowledgements, or enforce one active worker per durable. This
scope is documented by [NATS](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/consumers.md#ackpolicy).

### [P1] Slow handlers can strand prefetched messages for 30 minutes

[`Consumer`](../lib/framework_nats/src/consumer.rs#L174) waits for permits while consuming a
finite batch. In pinned `async-nats` 0.50.0, that batch stops yielding after `expires + 5s`,
even with buffered messages remaining. With the defaults, dispatch taking over six seconds
can abandon those messages until the hardcoded 30-minute acknowledgement timeout.

Use continuous consumption with bounded prefetch, or size each pull to available processing
capacity.

### [P2] Larger batch settings are constrained by an unset max_ack_pending

[`BatchConsumer` configuration](../lib/framework_nats/src/consumer.rs#L305) leaves
`max_ack_pending` at the server default, normally 1,000. The log processor requests 5,000
messages and only acknowledges after collecting the batch, so it cannot fill that batch
under default limits and waits for expiry despite backlog.

Configure the pending limit alongside batch size and worker count. See the
[server defaults](https://github.com/nats-io/nats-server/blob/main/server/consumer.go).

### [P2] Service shutdown discards buffered requests; they do not fail over

[`Service` shutdown](../lib/framework_nats/src/service.rs#L145) drops subscriptions and their
queued messages. Core NATS will not redistribute requests already delivered to this instance.

Drain buffered requests within a deadline, or reply with a retryable shutdown error. Also
correct [`spec/nats.md`](../spec/nats.md#L23): saturation buffers requests inside the client,
rather than keeping them at the broker. The default buffer is 65,536 messages per
subscription, after which messages are dropped. See the
[client documentation](https://docs.rs/async-nats/0.50.0/async_nats/struct.ConnectOptions.html#method.subscription_capacity).

## Performance experiments

### Reduce payload logging on successful calls

The saved [September 29 profile](../report/2026-09-29_nats_api/055922_profile.json) attributes
about 9.3% inclusive samples to `Action::log`. Benchmark configurable payload capture or
sampling while preserving error diagnostics. These are historical samples, not a measured
improvement.

### Reuse a continuous pull stream

Each current `.batch().messages()` creates a fresh inbox/subscription. Benchmark continuous
consumption with message and byte limits after addressing the slow-handler finding.

### Tune concurrency against latency, not throughput alone

The saved GET runs already show diminishing returns:

| Client concurrency | Requests/sec | p99 |
| ---: | ---: | ---: |
| [256](../report/2026-09-29_nats_api/055608.json) | 70,723 | 6.6 ms |
| [1,024](../report/2026-09-29_nats_api/055643.json) | 86,234 | 19.0 ms |
| [2,048](../report/2026-09-29_nats_api/055729.json) | 77,895 | 46.2 ms |
| [4,096](../report/2026-09-29_nats_api/055804.json) | 81,145 | 75.4 ms |

These measure client concurrency, so they do not directly establish the best
`ServiceConfig.max_concurrency`.

## Validation and limitations

- `cargo test -p framework_nats --locked --offline` passed. The crate contains zero unit or
  documentation tests.
- `cargo check -p nats_test --tests --locked --offline` passed; integration tests compile.
- E2e execution was not available because no local NATS container exists.
- No fresh performance benchmark was run; performance observations use the saved reports.
- This review proposes changes; no implementation or specification changes have been made.
