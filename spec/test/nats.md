# NATS e2e test

Start `nats`, then run `cargo test -p nats_test`. Code: [`test/nats_test`](../../test/nats_test).

- Setup creates or updates the JetStream stream `nats_test_stream` (subjects `nats_test.>`) and
  recreates a durable pull consumer per test with `DeliverPolicy::New`, created before the consumer
  starts so no new message is missed, and recreated so a rerun inherits no unacked messages.
- Single message: `Consumer` with handlers on two subjects with different payload types,
  explicit ack; assert subject and payload.
- Batch message: `BatchConsumer` receives 10 messages, ack policy `All`; assert subject.
- Service: `#[nats_api]` generated service and client for request/response, `()` request and
  response, and a service exception propagated with severity and code. After shutdown the
  service unsubscribes and requests fail with `NATS_NO_RESPONDERS`.
- Saturated shutdown: a service with one permit held by a handler that never finishes, and a second
  request waiting; after cancel it must unsubscribe within 2 s (`NATS_NO_RESPONDERS`), then stop
  once the handler is released.
- Saturated consumer: two permits held by handlers past `expires + 5s`; once released, all 10
  messages must be handled (none stranded unacked until `ack_wait`).
- Consumer drain: one permit held, the rest of the batch queued locally; after cancel the
  in-flight message is handled, and a second consumer on the same durable (the next release)
  handles the queued ones within 5 s; none is left ack pending.
- Batch backlog: 2500 messages stored before a `BatchConsumer` with batch size 2000 starts; batches
  must be 2000 then 500 (not cut at the server default `max_ack_pending` of 1000).
- Service drain: one permit held, requests buffered behind it; after cancel every buffered request
  is still answered, then the service stops.
- Consumer tests wait on a semaphore released by handlers, then cancel shutdown and join.

The NATS contract is specified in [nats.md](../nats.md) and [nats_api.md](../nats_api.md).
