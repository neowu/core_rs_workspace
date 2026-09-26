# Kafka design

Code: [`framework_kafka/src/consumer.rs`](../lib/framework_kafka/src/consumer.rs),
[`framework_kafka/src/producer.rs`](../lib/framework_kafka/src/producer.rs) · siblings:
[`nats.md`](nats.md), [`test/kafka.md`](test/kafka.md)

## Consumer

- Poll a batch until `poll_max_records`, `poll_max_wait_time`, or shutdown; group by topic, run each
  topic's handler in its own task, wait for all, then commit the consumer state (at-least-once).
- Offsets are committed manually (`enable.auto.commit = false`), only after the whole batch is
  handled. Messages already polled when a poll error or shutdown occurs are still handled and
  committed; after a poll error the loop backs off 5s.
- `add_handler`: messages with the same key are handled sequentially in partition order; each key
  group (and each unkeyed message) runs in its own task. `add_bulk_handler`: one call per topic per
  batch.

## Design decisions

### `StreamConsumer`, not `BaseConsumer` on a worker

`BaseConsumer::poll(timeout)` is a blocking librdkafka call; run inside a task it parked a tokio
worker for up to `poll_max_wait_time` every loop and delayed shutdown by the same. rdkafka's
`StreamConsumer` polls librdkafka with a zero timeout and wakes the task from the queue's non-empty
callback, so batch collection is a plain `select!` over the message stream, a deadline, and the
shutdown signal.

A dedicated OS thread running the blocking poll, handing batches over a channel and receiving commit
requests back, was the alternative. It was not needed: `StreamConsumer` reads the same consumer
queue (same per-partition ordering) and `commit_consumer_state` delegates to the same base consumer,
so batch/commit semantics are unchanged without the extra thread and channels.

### `max_concurrency` bounds every handler task

Same meaning as nats `ConsumerConfig::max_concurrency`, but unlike nats one kafka consumer serves
both single and bulk topics, so one semaphore per consumer (default 100) is shared by all of them:
each `add_handler` key group holds one permit, and each `add_bulk_handler` call holds one permit, as
it occupies one task for the whole batch.

- Key groups: the permit is acquired *before* spawning the group task and held until the group
  finishes, so a 5,000 record batch never creates more than `max_concurrency` concurrent handlers
  (hitting ES / DB) or tasks.
- Bulk: the permit is acquired before `log::action` is built, so the queue wait is not counted in
  the action's `elapsed`.
