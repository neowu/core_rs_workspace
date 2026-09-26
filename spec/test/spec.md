# End-to-end tests

Integration crates live under `test/`, one crate per service:
[http](http.md) · [nats](nats.md) · [kafka](kafka.md) · [db](db.md) · [clickhouse](clickhouse.md).

- Every test uses `#[integration_test]`: it initializes `System`, runs the body in a log action
  named after the fn, and fails with the exception backtrace. Keep one integration test per file,
  since the action sender is process wide and each test owns its runtime.
- Tests run on the multi thread tokio runtime, as apps do, so framework components are started
  with plain `tokio::spawn`.
- Tests exercise real services and transports, own their shutdown signal (cancel, then join the
  spawned component), and assert observable results.
- Start required services with Apple Container (`container start {service}`); create them once
  with `docker/{service}/start_container.sh`. Services are addressed as `{service}.test`.
- Each test sets up its own state (tables, streams, topics), so suites can be rerun without
  manual cleanup.
