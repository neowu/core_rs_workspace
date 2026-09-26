# NATS e2e test

Start `nats`, then run `cargo test -p nats_test`. Code: [`test/nats_test`](../../test/nats_test).

- Setup creates or updates the JetStream stream `nats_test_stream` (subjects `nats_test.>`) and a
  durable pull consumer per test with `DeliverPolicy::New`, created before the consumer starts so
  no new message is missed.
- Single message: `Consumer` with handlers on two subjects with different payload types,
  explicit ack; assert subject and payload.
- Batch message: `BatchConsumer` receives 10 messages, ack policy `All`; assert subject.
- Service: `#[nats_api]` generated service and client for request/response, `()` request and
  response, and a service exception propagated with severity and code. After shutdown the
  service unsubscribes and requests fail with `NATS_NO_RESPONDERS`.
- Consumer tests wait on a semaphore released by handlers, then cancel shutdown and join.

The NATS contract is specified in [nats.md](../nats.md) and [nats_api.md](../nats_api.md).
