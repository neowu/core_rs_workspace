# NATS API tuning

Review target: `report/2026-09-22_nats_api_server.html`, the saved POST profile in
`target/nats_profile.json.gz`, and the request path in `lib/framework_nats`.
Benchmark requirements: `spec/benchmark/nats_api_server.md`.

Status: sections 2, 3 and 4 implemented and measured. Section 1 (hotspot metric labelling) and
section 5 (task bookkeeping) are still proposed.

Outcome, three interleaved before/after pairs per scenario, 15 s measured after a 3 s warmup,
prebuilt binaries on both sides and one client binary driving both:

| scenario | cpu µs / request | throughput / s | allocations / request |
|---|---|---|---|
| `get` before | 18.9 (18.73-18.99) | 72,400 | 33 |
| `get` after | 15.8 (15.66-15.87) | 74,500 | 28 |
| `post` before | 19.7 (19.43-19.94) | 71,700 | 35 |
| `post` after | 16.6 (16.58-16.66) | 74,000 | 30 |

Before and after ranges do not overlap on either scenario. Allocation counts are deterministic and
repeated exactly. A reprofile of the new code puts `log_line` and its callees at 2.96% of
non-parked samples, down from 4.34%, and the `watch::Receiver<Option<ServerInfo>>` drop frame no
longer appears. Design decisions and method notes are in
`spec/benchmark/nats_api_server.md` and `spec/action_log.md`.

Note found while implementing section 4: `time` 0.3.55's `Rfc3339::format_into` returns a byte
count that omits the subsecond digits, so `write_rfc3339` takes the written length from what its
cursor consumed. A test pins it against `to_rfc3339`.

## Evidence and measurement limits

- The report lists `ActionFuture::poll` at 1.87% GET self samples and
  `ActionAllocs::poll` at 1.68% POST. These frames contain inlined request work;
  they do not isolate the cost of allocation tracking.
- In the saved POST profile, `Action::log_line` and its callees account for
  1,180 samples, or 4.34% of the report's non-parked samples. Of 253 samples in
  `core::fmt::write`, 184 occur beneath `log_line`.
- Dropping `watch::Receiver<Option<ServerInfo>>` accounts for 0.57% GET and
  0.60% POST self samples. All 163 POST samples occur under request handling.
- The report mixes historical registration paths and allocator configurations.
  The latest generated-handler allocation runs record 33 allocations / 5,177
  bytes for GET and 35 allocations / 5,276 bytes for POST. Treat these as
  reference points, not a controlled performance baseline.

Do not add overlapping inclusive and self percentages or infer speedups from
them. Establish a fresh baseline with the current code before implementation.

## 1. Correct hotspot interpretation

`benchmark/report/src/hotspot.rs` counts samples equally, ignores the profile's
`threadCPUDelta`, and excludes only `__psynch_cvwait` and `__psynch_mutexwait`.
Its remaining sample count is labeled "on-cpu", although `kevent` may include
waiting. Mutex waits also need to be distinguished from idle capacity.

- Label the existing metric as samples excluding the named waits, rather than
  CPU time, and avoid interpreting the excluded percentage as spare capacity.
- Evaluate CPU-delta weighting separately, documenting its attribution limits;
  do not assume elapsed thread CPU belongs entirely to the sampled leaf.
- Include caller/inclusive views when investigating framework wrappers and
  shared functions such as formatting, clocks, and allocation.
- Preserve historical records and make any changed metric definition explicit.

## 2. Precompute request metadata

Targets: `framework_macro/src/nats_api.rs::build_handler_statement` and
`framework_nats/src/service.rs::Service::start`.

Build the function-name context and `request:{subject}` task name once during
handler registration. Preserve the concrete implementation type in the function
name and preserve existing context and shutdown diagnostic values.

The context currently owns its string: cloning a prebuilt value still allocates,
but avoids repeated formatting and buffer growth. Let task names use shared or
borrowed storage so they do not require an owned string allocation per request.
Check all `TaskExecutor` callers if its name parameter or storage type changes.

## 3. Share the NATS client across handlers

Target: `framework_nats/src/service.rs`, which clones `Client` per request.

Keep one client behind `Arc<Client>`, pass a shared reference into each request
task, and borrow the client for reply publishing. This should avoid cloning and
dropping its internal handles on every request, replacing that work with one
shared-reference clone/drop. Measure the actual benefit.

Preserve publish backpressure, reply/error behavior, connection lifetime, and
the service's existing concurrency and shutdown behavior.

## 4. Reduce trace formatting overhead

Targets: `framework/src/log/action.rs::Action::new` and `Action::log_line`, plus
timestamp formatting in `framework/src/time/datetime.rs`.

A successful benchmark request produces seven trace lines through subject,
client, payload, function context, and byte stats. Each line reads the clock and
formats an elapsed prefix.

- Optimize the fixed elapsed-time prefix while preserving its output format and
  per-line timestamps.
- Format the action's RFC3339 timestamp directly into its log buffer, avoiding
  the temporary string created during action construction.
- Consider batching task-local access and timestamps only as a separate design
  decision: one timestamp per block would change trace behavior.

Keep early trace collection so a later error or `trace()` retains request
history. Preserve truncation, severity promotion, context/stats records, and the
serialized action format. Do not disable allocation tracking based on the
wrapper's profile name.

## 5. Investigate task bookkeeping contention

Target: `framework/src/task.rs::TaskExecutor`.

Every request uses `TaskTracker` and inserts/removes a diagnostic entry in a
shared mutex-protected map. This adds two map lock acquisitions per request.
The existing report does not isolate their CPU or contention cost.

After the simpler changes, measure this path across worker counts and request
concurrency. If contention is material, evaluate sharded diagnostic storage or
optional detailed task-name tracking. Retain lifecycle tracking and useful
shutdown diagnostics; making names optional is an explicit behavior tradeoff.

## Validation and completion

- Implement and measure metadata caching and client sharing separately, then
  trace formatting. Pursue bookkeeping changes only if measurements justify them.
- Run relevant framework and macro unit tests, NATS integration tests, and
  compilation/lint checks for affected consumers. Start the integration-test
  broker with `container start nats` on the supported host.
- Verify successful and error replies, link headers, function context, log
  formatting/truncation, and concurrency/shutdown behavior as relevant to each
  change. Add focused regression tests for changed behavior.
- Use interleaved before/after GET and POST runs with the same payloads,
  concurrency, worker counts, broker, build profile, and allocator configuration.
  Use longer runs than the original five-second measurements and report spread.
- Measure server CPU/request with production per-action allocation tracking.
  Measure process-wide allocations/request separately with `ALLOC_STATS=1`;
  that mode replaces the framework allocator and disables per-action tracking.
  Do not compare CPU numbers across those allocator modes as an A/B result.
- Reprofile separately from timing runs. Compare allocation counts and bytes,
  server CPU/request, latency, and RSS; the single-client-connection benchmark
  can hide throughput improvements.
- Update `/spec` with implemented design decisions, behavior, and requirements.
  Keep implementation details in code and record measured outcomes here.

Accept changes only after relevant checks pass and controlled measurements show
the intended savings without material regressions. No fixed speedup is promised
by the current profile.
