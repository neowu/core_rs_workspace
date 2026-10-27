# Iggy research

Status: **on hold** (2026-10-27). An experimental `framework_iggy` was built and passes its e2e tests, but adoption
waits because iggy's client network model scales poorly with the number of topics one app consumes.

Code: removed from the tree; it lives in commit `d132169` (`git show d132169`, or `git checkout d132169 -- <path>`
to restore): `lib/framework_iggy`, `test/iggy_test`, `docker/iggy`, `spec/iggy.md`, `spec/test/iggy.md`
· compare: [`spec/kafka.md`](../spec/kafka.md), [`spec/nats.md`](../spec/nats.md)

Versions: `iggy` SDK 0.11, server 0.9.

## Why on hold

### One request in flight per connection

The iggy TCP protocol is request/response with no request id: `TcpClient::send_raw` holds the socket from writing
a request until it reads the reply. It cannot pipeline or multiplex, so the only way to get concurrent requests is
more connections. The heartbeat goes through the same connection.

### Polls and group membership are per topic

`poll_messages` and `join_consumer_group` each take one stream and one topic. Unlike a Kafka fetch, which covers
every topic-partition a member owns on a broker, there is no multi-topic poll, and a consumer group is really one
membership per topic.

### No long poll

A poll returns at once, empty or not. An idle topic still costs one request per `poll_interval`.

### Consequence for an app with many topics (e.g. 200)

| Design | Connections | One round of polling |
|---|---|---|
| High-level `IggyConsumer` per topic (current) | N, each with its own login and heartbeat | parallel |
| Low-level API, one connection | 1 | N sequential round trips, mostly empty polls; a stuck reply stalls every topic |
| Low-level API, pool of K connections | K | N/K sequential polls per connection |

None of these is efficient at large N. The SDK is designed around one consumer per stream/topic, and iggy's model
expects few topics with many partitions rather than many fine-grained topics.

### Observed: a poll reply that never arrives

Against server 0.9, under concurrent consumer-group polls on one shared connection, a poll reply occasionally never
arrived. Because the connection serves one request at a time, every commit and the heartbeat waited for the 30 s
read timeout, which hung shutdown. This is why the current code opens a connection per topic.

### Compared with NATS JetStream

NATS multiplexes every subscription, pull, publish and ack over one connection, a pull carries `expires` (the
server holds it until data arrives), and a JetStream consumer can cover many subjects (wildcards,
`filter_subjects`). Many subjects per app costs one connection and nothing extra while idle.

What iggy / Kafka still do better:
- Per-key ordering across a consumer group: each partition goes to exactly one member, which `add_handler` key
  groups rely on. A JetStream pull consumer has no key affinity; that needs partitioned subjects (subject mapping)
  with a consumer per partition, or `max_ack_pending = 1`.
- Bulk throughput: batched fetch and per-partition offset commits are cheaper per message than per-message acks.

Rule of thumb: many topics at moderate volume fit NATS; a few very high-volume topics with strict per-key ordering
fit Kafka / iggy.

## Current code design

The handler contract is the same as Kafka: `add_handler` (key groups handled in order, groups in parallel) and
`add_bulk_handler`, bounded by `max_concurrency`, at-least-once. See `spec/iggy.md` in `d132169` for the full
decisions. The key points:

### Consumer (`consumer.rs`)

- Uses the high-level `IggyConsumer`, which handles group join and rejoin, reconnects, and partition reassignment.
  Rebuilding that on `poll_messages` would be most of the work.
- One `IggyConsumer` and one connection per topic, each running its own poll / handle / commit loop. The loops
  share the consumer's semaphore. The `IggyClient` is moved into the loop's task and dropped after it, because the
  client owns the heartbeat task and its `Drop` aborts it.
- `start` creates the stream and each topic if missing (4 partitions), then joins the group.
- A batch is polled until `poll_max_records`, `poll_max_wait_time` or shutdown, then handled (spawned, so a panic
  is caught), then committed by hand with `store_offset`, keeping the highest offset per partition.
  `AutoCommit::Disabled`, because every other mode can commit before the handler finishes.
- `poll_max_records` is also the poll `batch_length`. Otherwise `PollingStrategy::next()` can get its reading
  position `batch_length` ahead of the last commit, and every poll then returns only dropped messages.
- `poll_interval` (default 10 ms) paces polling, since there is no long poll. It caps throughput at
  `batch_length / poll_interval` per topic per instance.
- After a poll error the loop handles and commits what it already polled, with no extra backoff (the SDK already
  waited `polling_retry_interval`).
- On shutdown it calls `IggyConsumer::shutdown()`, which leaves the group so partitions are reassigned at once.

### Producer (`producer.rs`)

- Uses the low-level `send_messages`, so one `Producer` serves every topic of its stream over one connection.
  `IggyProducer` is bound to one topic. Each `send` is one request that waits for the server.
- `new` creates the stream if missing; the first send to a topic creates the topic.

### Headers (`lib.rs`)

- `ref_id`, `client` and `key` go in user headers. Iggy hashes `Partitioning::messages_key` to pick a partition but
  does not store the key, so the key is also written as a header. Header values are limited to 255 bytes.
- The consumer parses headers once per message into an internal `RawMessage`. Lag uses `origin_timestamp`, which
  the producing client sets in microseconds.

### Tests

`test/iggy_test` runs the same cases as Kafka (single, bulk, concurrency) plus commit, against
`docker/iggy/start_container.sh`. The stream and groups get a per-run suffix, so each run starts clean.

## If revisited

- Check whether a newer iggy server or SDK adds multiplexing, a multi-topic poll, or long polling. Any of these
  removes the main blocker.
- Otherwise, for many topics: use the low-level API with a small fixed pool of connections. Each connection polls
  its share of topics in turn, handles the batch in parallel, then commits. This means reimplementing group rejoin
  (on `DiagnosticEvent::SignedIn` and on `ConsumerGroupMemberNotFound`) and leaving the group on shutdown.
- Or design topics coarse-grained (few topics, many partitions, a type header) to fit iggy's model.
