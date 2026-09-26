# Kafka e2e test

Start `kafka`, then run `cargo test -p kafka_test`. Code: [`test/kafka_test`](../../test/kafka_test).
Kafka has no service API, so only messaging is covered.

- Topic names and consumer groups get a per-run suffix, so each run starts clean without leftover
  messages or stale group members. Topics are auto created by the first send.
- Messages are produced before the consumer starts, and the consumer uses
  `auto_offset_reset = "earliest"` to read them. Subscribing to topics that do not exist yet is
  avoided, since librdkafka only picks them up on its next metadata refresh.
- Single message: `add_handler` on two topics with different payload types, one keyed and one
  unkeyed message; assert key and payload.
- Bulk message: `add_bulk_handler` receives 10 keyed messages; assert each key matches its payload.
- Each test waits on a semaphore released by handlers, then cancels shutdown and joins.
