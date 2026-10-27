# Iggy e2e test

Start `iggy`, then run `cargo test -p iggy_test`. Code: [`test/iggy_test`](../../test/iggy_test).
The iggy contract is specified in [iggy.md](../iggy.md). Same cases as [kafka](kafka.md), plus commit.

- The stream and consumer groups get a per-run suffix, so each run starts clean without leftover messages or stale
  group members. The framework creates the stream and topics (producer on first send).
- Messages are produced before the consumer starts; a new group has no stored offset, so it reads from the first
  message.
- Single message, bulk message, concurrency: as in kafka.
- Commit: 10 messages over 4 partitions are handled and committed, the consumer stops, 3 more are sent, and a second
  consumer of the same group must see only those 3. Values are collected and asserted after the consumer stops,
  since a panicking handler is only logged.
- Each test waits on a semaphore released by handlers, then cancels shutdown and joins.
