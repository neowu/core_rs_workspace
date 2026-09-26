# Tokio runtime review

Source: https://dial9-rs.github.io/blog/principles-for-fast-tokio-applications/

Review of `lib/` and `app/` against the article's principles, ordered by priority.

## 1. Kafka consumer blocks a worker (high) — done

Done: switched to `StreamConsumer`, see `spec/kafka.md`.

`lib/framework_kafka/src/consumer.rs` `poll_message_groups` calls `BaseConsumer::poll(Timeout::After(..))`,
a synchronous rdkafka call, inside an async task. `log_processor` uses `poll_max_wait_time: 3s`, so one worker
is parked up to 3s every loop; shutdown also waits for the poll.

Plan:
- Run the poll loop on a dedicated OS thread (`std::thread::spawn`), send message groups to the runtime over a
  bounded `mpsc` channel, receive commit requests back after handlers finish.
- Alternative: switch to rdkafka `StreamConsumer` (async). Pick one after checking commit/ordering semantics.
- Shutdown: thread checks the cancellation token between polls, use a short poll timeout (e.g. 100ms) in the loop.

## 2. Kafka handlers have no concurrency bound (high) — done

Done: `ConsumerConfig::max_concurrency`, see `spec/kafka.md`.

`handle_messages` spawns one task per distinct key; with `poll_max_records: 5_000` that is up to 5,000 concurrent
handlers hitting ES / DB.

Plan:
- Add `max_concurrency` to kafka `ConsumerConfig` (same meaning as nats), gate each key-group task with a
  `Semaphore` permit.
- Update `spec/` kafka section.

## 3. Appender does blocking stdout writes on a worker (medium)

- `ConsoleAppender` / `GCloudAppender` `println!` per line; gcloud writes up to 2,000 trace lines per traced action,
  each a `write(2)` (stdout is line-buffered), all within one poll. A slow log pipe blocks the worker.
- Appender daemon (`lib/framework/src/system.rs`) handles one message per `recv()`.
- Channel is `unbounded_channel`; a stalled stdout grows memory without limit.

Plan:
- Drain with `recv_many` and write each batch through one `stdout().lock()` + `BufWriter`, flush once per batch.
- Consider moving console/gcloud appender off the runtime onto a dedicated OS thread (`blocking_recv`); nats
  appender stays async.
- Open decision: keep unbounded (never lose logs) vs bounded + `try_send` with a dropped-count metric.

## 4. No runtime health metrics (medium)

Article principle #1: measure schedule latency before optimizing.

Plan:
- Lag probe in `MetricsCollector`: measure how late `sleep` wakes up, report `runtime_schedule_lag`.
- Report stable tokio metrics from `Handle::current().metrics()`: `global_queue_depth`, `num_alive_tasks`.
- Per action poll stats in `ActionFuture` (next to `ActionAllocs::poll`): `poll_elapsed` (sum) and
  `max_poll_elapsed`, surfaces long polls per action in clickhouse.
- CPU throttling: read `nr_throttled` / `throttled_usec` from `cpu.stat` (cgroup v2), report deltas; CFS quota
  throttling is the container equivalent of the article's worker unpark delays.
- Update `spec/action_log.md` / metrics spec with the new stats.

## 5. TaskExecutor global mutex (low)

`lib/framework/src/task.rs` locks `Mutex<HashMap>` on every spawn and completion, only to list unfinished tasks
at shutdown. Critical section is one map op, fine at current concurrency. Revisit only if profiles show
contention; alternative is per-name atomic counters.

## Already aligned (no action)

- nats consumer / service bound concurrency with `Semaphore`.
- nats batch consumer + clickhouse batch insert favor throughput; the single long poll over a 5,000 message
  batch is acceptable since nothing latency-sensitive shares that process.
- std mutexes (pool, alert, intern) have short critical sections, never held across `.await`; no
  `tokio::sync::Mutex`.
- no `tokio::fs` / `spawn_blocking`; small `/proc` and `/sys` reads stay on workers.
- counters and id generator are atomics.
