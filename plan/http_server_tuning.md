# HTTP server tuning

Review target: `lib/framework/src/web/server.rs::http_server_layer`, using the
measurements described in `spec/benchmark/http_server.md`.

The recorded Axum middleware wrapper has 1.58% GET and 1.22% POST self CPU time.
These are not inclusive costs: logging, formatting, allocations, and action
completion also appear in other frames. Speedups must be measured rather than
inferred by adding unrelated self-time percentages.

## 1. Avoid owned metadata allocations at action completion

Implemented using `Cow<'static, str>` for action metadata and keys.

Use borrowed/static metadata in `ActionMessage` for application, host, kind,
context keys, and stats keys. The system context lives for the process lifetime,
so its host string can also be borrowed safely. Retain support for deserializing
owned messages and preserve the serialized format. Carry the key representation
through `Action` so completion moves the context and stats vectors directly.

## 2. Store scalar context values inline

Implemented using `ContextValues` (`SmallVec<[String; 1]>`) throughout action
collection and appender delivery.

Replace the per-scalar `Vec<String>` allocation with storage that holds one string
inline and supports multiple values. Carry this representation through
`ActionMessage`; converting back to a vector would only defer the allocation.
Preserve array serialization, empty/multiple values, truncation, console output,
and Google Cloud's single-value scalar output. A matched request sets at least
five scalar contexts, making five vector allocations candidates for removal.

## 3. Amortize per-line logging overhead (follow-up)

Consider a header-block writer to share task-local access and timestamp formatting
across headers, if one timestamp per block is acceptable. Do not disable early
log collection merely because `TraceAppender` discards successful traces: later
errors or `trace()` need the earlier request details.

## 4. Construct the fallback client IP lazily (follow-up)

Replace `client_ip.unwrap_or("unknown".to_owned())` with
`client_ip.unwrap_or_else(|| "unknown".to_owned())`. Check whether the optimized
allocation count changes before claiming a measured saving.

## Validation

- Test message serialization/deserialization and context scalar/empty/multiple
  values, including truncation and Google Cloud output.
- Run framework unit tests and workspace compilation/lint checks for consumers.
- Compare allocations/request and server CPU/request with the same benchmark
  settings before and after. The single h2c connection can mask throughput gains.
- Items 1 and 2 change public Rust field types, while preserving the wire format;
  external appenders that construct or explicitly type messages may need updates.

Validation completed: all 69 framework unit tests pass; Clippy passes for
`framework`, `framework_nats`, `http_test_server`, and `http_test_client`, including
all targets. The full workspace Clippy run is blocked by missing `cmake` when
building `rdkafka-sys`. No A/B performance run has been completed; the existing
release benchmark binaries are macOS ARM executables and this environment is Linux.

Consumer compatibility follow-up: updated `log_processor_rs` alert helpers and
ClickHouse row conversion for the new metadata/context types. All 23 application
unit tests and `cargo clippy -p log_processor_rs --all-targets --locked --offline`
pass, including a deserialized-message-to-row regression test.
