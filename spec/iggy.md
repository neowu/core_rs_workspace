# Iggy design

Code: [`framework_iggy/src/consumer.rs`](../lib/framework_iggy/src/consumer.rs),
[`framework_iggy/src/producer.rs`](../lib/framework_iggy/src/producer.rs) · siblings:
[`kafka.md`](kafka.md), [`nats.md`](nats.md), [`test/iggy.md`](test/iggy.md)

Experimental, built on the `iggy` 0.11 SDK (server 0.9). The handler contract is the same as kafka; this file only
records where iggy differs. A `Producer` / `MessageConsumer` serves one stream, so `Topic` is a topic name within it.

## Consumer

- Same contract as [`kafka.md`](kafka.md#consumer): poll a batch until `poll_max_records`, `poll_max_wait_time`, or
  shutdown, handle it, then commit (at-least-once). Handler and decode errors are logged, not retried; a panic does
  not cancel other key groups. `add_handler` / `add_bulk_handler` and `max_concurrency` work as in kafka.
- An `IggyConsumer` reads one topic, so each topic runs its own poll / handle / commit loop on its own connection.
  They share the consumer's semaphore.
- `start` creates the stream and each topic if missing before joining the group.
- After a poll error the loop handles and commits what it already polled, then polls again without its own backoff:
  the SDK already waited `polling_retry_interval` before returning the error.
- On shutdown the consumer calls `IggyConsumer::shutdown()`. This leaves the group, so the server reassigns its
  partitions at once instead of when the connection drops.

## Producer

- One `Producer` serves every topic of its stream over one connection, and each `send` is one request that waits for
  the server. `new` creates the stream if missing; the first send to a topic creates it if missing.
- `ref_id` / `client` go in user headers, the same as kafka.

## Design decisions

### High-level consumer, low-level producer

The high-level `IggyConsumer` handles consumer group join and rejoin, reconnects, and partition reassignment.
Rebuilding that on `poll_messages` would be most of the work, so the consumer uses it. `IggyProducer` is bound to
one topic and needs its own `init`, so the producer calls `MessageClient::send_messages` directly instead.

### Commit by hand, per partition, after the batch

`AutoCommit::Disabled`: every other mode can commit a message before its handler finishes (the SDK default commits
with the poll request). The loop keeps the highest offset polled per partition and calls `store_offset` for each
one once the batch is handled. A consumer group member's messages interleave partitions.

### Streams and topics are created if missing

Kafka auto creates topics, so apps never provision them; iggy does not, so the framework creates them to keep that
workflow. Create is attempted and "already exists" counts as success, which is safe against concurrent creators. New
topics get 4 partitions (the server default is 1, which caps a group at one instance); an existing topic is never
changed, and adding partitions later moves keys to other partitions, as in kafka.

### One connection per consumer topic

An iggy connection carries one request at a time. With all topics on one shared connection, a commit waited behind
another topic's in-flight poll, and a poll whose reply never arrived (seen against server 0.9, under concurrent
consumer-group polls) blocked every commit and the heartbeat until the 30 s read timeout, hanging shutdown. Each
topic now connects separately, so a stuck poll only stalls its own topic.

### `poll_max_records` is also the poll `batch_length`

Under `PollingStrategy::next()` each poll starts after the stored offset, and the consumer drops anything at or
below its reading position. If the reading position runs `batch_length` or more ahead of the last commit, every
poll returns only dropped messages and the stream stalls. A batch never collects more than `batch_length` messages
before committing, so this cannot happen.

### `poll_interval` paces polling

Iggy has no long poll. Without a poll interval the consumer re-polls an idle topic as fast as the server answers.
`poll_interval` (default 10ms) is the smallest gap between two requests of one topic. It also caps throughput at
`batch_length / poll_interval` per topic per instance (100k/s by default).

### The message key is a header

Iggy hashes `Partitioning::messages_key` to pick a partition but does not store the key. The producer also writes
it to a `key` header, which the consumer reads to build `add_handler` key groups. Header values are limited to
255 bytes, so longer keys fail to send.

### Headers are parsed once per message

`user_headers_map` decodes the header block into an owned `BTreeMap` on every call. Each message is converted to an
internal `RawMessage` (key, ref_id, client, payload, origin timestamp) right after it is polled. Lag uses the
header's `origin_timestamp`, which the producing client sets in microseconds.

### Partition assignment

The server gives each partition to exactly one member of a group, so the partition count caps how many instances
a group can use, as in kafka. A partition that moves stays committable by its old owner until it hands it off, so
committing after the batch survives a rebalance. A long bulk handler delays the handoff.
