# Benchmark

Code: [`benchmark/http_test_server`](../../benchmark/http_test_server),
[`benchmark/http_test_client`](../../benchmark/http_test_client),
[`benchmark/run.sh`](../../benchmark/run.sh), [`benchmark/profile.sh`](../../benchmark/profile.sh),
[`benchmark/report.sh`](../../benchmark/report.sh), [`benchmark/hotspots`](../../benchmark/hotspots) ·
results: [`spec/benchmark/report/`](report)

A benchmark workflow to find the bottleneck or hotspot of framework code, and to compare different
designs. **Not micro benchmarks** — the unit of measurement is a whole request crossing a real
socket, so what a run measures is the framework path as an app actually pays for it: accept, the
http server layer (action log, context, client info, header/cookie logging), routing, extractor,
controller, response serialization.

A benchmark is a pair of processes, started separately so each can be profiled, pinned or replaced
on its own:

| process | role |
|---|---|
| `http_test_server` | the target under test — a framework app with nothing wired but the http server |
| `http_test_client` | the measuring instrument — a closed loop load generator |

## Endpoints

The same payloads and the same work reached two ways, so a run can price one route style against
the other:

| scenario | path | route |
|---|---|---|
| `get` | `/benchmark/get` | plain controller, `web::route::get` + `Query` |
| `post` | `/benchmark/post` | plain controller, `web::route::post` + `Json` |
| `api_get` | `/benchmark/api/get` | `#[api]` generated, `MethodFilter::GET` + `Query` |
| `api_post` | `/benchmark/api/post` | `#[api]` generated, `MethodFilter::POST` + `Json` |

## Requirements

- Controllers do no work of their own. A get echoes a query scalar, a post sums a body array.
  Anything a controller does is noise added to what is being measured, so the only work in a run
  belongs to the framework.
- The plain and `#[api]` endpoints build their responses through the same `GetResponse::new` /
  `PostResponse::new`. A comparison is only meaningful while the two differ solely in the framework
  code between the socket and the call.
- The server is a normal framework app. It goes through `System::init` / `start_logger` /
  `start_service` exactly as a real app does — a benchmark that bypasses the framework's own setup
  measures nothing useful.
- Both sides are stateless and hold no external service. A run needs no container, unlike `test/`.
- The server binds `0.0.0.0:8080` with no override, and the client defaults to it. A benchmark host
  keeps that port free; a bind address knob is one more thing that can differ between two runs being
  compared.

## Design decisions

### The crates live outside `default-members`

`benchmark/*` is a workspace member but not a default member, same as `test/*`: `cargo build` and
`cargo clippy` at the workspace root stay fast, and a benchmark is built only when it is run. They
do not opt into `[lints] workspace = true` either — these are tools, not shipped code, and the
restriction lints (`print_stdout`, `unwrap_used`, `indexing_slicing`) fight a load generator.

### Everything under `benchmark/` is a crate

This document and the results live in `spec/benchmark/`, not beside the code they describe. The
workspace takes members by glob (`benchmark/*`), so a non-crate directory under `benchmark/` is
picked up as a member and every `cargo` command fails until it is named in `exclude`. Keeping
`benchmark/` to crates and scripts only means the glob needs no exception list, and it puts the spec
where the other specs are.

### One lib target, for the shared contract only

`http_test_server/src/lib.rs` holds the payload types and the `#[api]` trait, nothing else;
`main.rs` holds the app. The lib exists for exactly one reason — `http_test_client` depends on it so
the request and response shapes cannot drift between the two processes, which would silently change
what a run measures. Everything else is a `main.rs` with plain modules, no crate is split into
`lib.rs` + `main.rs` out of habit.

### Only `TraceAppender`

The server always runs `TraceAppender`. Action construction and the channel send stay — that is real
framework cost every app pays — but nothing is written per request, so appender output can never
become the bottleneck or make a run depend on how fast the terminal drains. `ConsoleAppender` would
measure `println!`.

### h2c only, over one shared connection

The client speaks **h2c** — cleartext http/2 with prior knowledge — because that is what
[`framework::http::HttpClient`](../../lib/framework/src/http.rs) does for internal calls:
`HttpClientConfig::internal_only()` sets `prefer_http2`, which becomes
`reqwest::ClientBuilder::http2_prior_knowledge()`. One connection is opened, kept alive, and every
request rides it as a multiplexed stream. Benchmarking http/1.1 would measure a protocol no internal
caller uses.

`verify` asserts the response came back as `HTTP/2`. A silent fallback to http/1.1 — a server
without the `http2` feature, a proxy in between — would otherwise quietly benchmark the wrong thing.

`--concurrency` is therefore open streams on one connection, not connections. That is the shape
being measured, and it has a cost: see the baseline.

### The client is not built on framework `HttpClient`

`HttpClient` opens an action per request and logs request, response, every header and the body. As a
measuring instrument that makes the client the bottleneck and puts framework work on both sides of
the wire, so the numbers no longer isolate the server. The client uses `reqwest` directly.

Benchmarking `HttpClient`, or the `#[api]` generated `BenchmarkServiceClient` built on it, is a
separate and legitimate target — it needs its own client side scenario, not a change to this one.

### The measured loop never parses a response

Deserializing on the client burns client CPU and does not exercise the server. So the loop reads the
body to completion (required, or the connection is not returned to the pool) and checks only the
status. Correctness is established once at startup: `verify` sends one request of the selected
scenario, parses the response and asserts on it, so a wrong url or a broken server fails fast
instead of producing a fast benchmark of 404s.

Everything else per-request is also hoisted out of the loop — the url is parsed once, the post body
serialized once into `Bytes` whose clone is a refcount bump.

### Closed loop, fixed concurrency

Each of `--concurrency` workers holds exactly one request in flight and sends the next as soon as
the previous resolves. Concurrency is the control, the server's own pace sets the rate. This is what
answers "how much can it do and where does the time go", which is the question here.

It does **not** answer "how does it behave at a rate above its capacity" — a closed loop cannot
overload the target, and its latency numbers are subject to coordinated omission. An open loop mode
(fixed request rate, backlog allowed to grow) is future work.

### Every latency sample is kept

A worker pushes nanos into its own `Vec` and the vectors are merged and sorted once at the end, so
percentiles are exact rather than estimated and the measured loop touches no shared state. A run of
a few million requests costs tens of MB, which is cheaper than a histogram dependency.

### Warmup is a discarded run of the same loop

`--warmup` runs the identical load phase and throws the result away, so the measured phase starts
with the connection pool established and the code paths hot.

### Cost is measured as cpu per request, not throughput

Client and server share the host, so throughput measures the pair, not the server: the four
scenarios land within 4% of each other and the ranking moves between runs. `run.sh` therefore reads
the server's own cpu time from `ps` around the client run and divides by every request served,
warmup included. That number is a property of the server alone. It is sampled from outside, so it
costs the server nothing and is always on.

`ps` resolves cpu time to 10 ms, which over a 20 s run is well under the run to run spread.

### Allocations per request are the sharp instrument

Allocation counts are deterministic where cpu time is noisy — they resolved the plain vs `#[api]`
question that throughput could not (exactly one extra allocation). `--features alloc_stats` swaps in
a counting global allocator, reported on shutdown and divided per request by `run.sh`.

Its counters are **sharded per thread and padded to a cache line**. The obvious version — four
shared `AtomicU64`, one of them a `fetch_max` for peak — measured 3.6x cpu per request, not because
atomics are slow but because every allocating thread wrote the same line. Sharded, the same
accounting is free within noise, so the only reason it stays behind a feature flag is that a
benchmark should not ship an allocator it did not mean to measure.

Peak live bytes is the one figure the sharded design cannot give, since it needs a global
`fetch_max`. `run.sh` polls rss instead, which is free and answers the same question.

### Every run is recorded, the report is derived

A run appends one `key=value` record to `spec/benchmark/report/<date>_http_server.txt` and
regenerates `<date>_http_server.html` from every record in that file. The text file is the data, the
html is a view of it — regenerating never loses anything, and the records stay greppable and
diffable.

The record comes from a single machine readable `data` line the client prints under `--record`,
never from scraping its human output, so changing how results are displayed cannot break the
report. `run.sh` adds what only it knows: server cpu, peak rss, heap counters, host, commit, cargo
profile, server thread count.

Profiling runs are deliberately not recorded — the profiler skews them, and a skewed row in the
history is worse than a missing one.

### Profiling is a separate script, not a mode

[`profile.sh`](../../benchmark/profile.sh) records the server under `samply` while the client drives
the same load. It builds `--profile profiling` (release plus full debug info — release alone only
carries line tables, which is not enough to attribute inlined frames).

Two things it must get right, both learned the hard way:

- **Kill the server, not samply.** `pkill -f` on the binary path matches samply's own command line
  too, and killing samply discards the recorded profile.
- **Saturate the server.** With the default 12 workers on a 12 core host the server parks between
  requests and two thirds of the profile is `__psynch_cvwait`. `TOKIO_WORKER_THREADS=4` gives the
  same throughput with the workers actually busy. The parked share is itself the useful reading:
  it is the server's spare capacity.

Open it with `samply load <file>`; the inverted call tree answers "where does the time go", the
flame graph answers "who called it".

### Hotspots land in the report, via a tool not a script

[`hotspots`](../../benchmark/hotspots) reads the samply profile and prints the top methods by self time
as records, which `profile.sh` appends and `report.sh` renders as a per scenario table. It exists
because reading a flame graph is a person's job, while "which method got slower" belongs next to the
throughput numbers.

It takes the profile on **stdin** (`gunzip -c` does the decompression) so it needs no gzip
dependency, and it resolves addresses through samply's `--unstable-presymbolicate` sidecar, taking
the **innermost inlined frame** — the one the sample is actually in.

Parked samples are counted and excluded. A parked worker is not spending time, and leaving those
samples in buries every real frame: the first http/1.1 profile was 62% `__psynch_cvwait`. The parked
share is reported alongside as the server's spare capacity.

## Running

```bash
./benchmark/run.sh --scenario api_post --concurrency 128 --duration 60
ALLOC_STATS=1 ./benchmark/run.sh --scenario get          # adds allocations per request
TOKIO_WORKER_THREADS=4 ./benchmark/profile.sh --scenario get --concurrency 64 --threads 6
./benchmark/report.sh spec/benchmark/report/2026-09-18_http_server.txt   # re-render by hand
```

`run.sh` builds both, starts the server, waits on `/health-check`, runs the client, stops the
server, prints the server's cpu per request and peak rss, and records the run. `profile.sh` does the
same while recording a cpu profile, and records nothing. `--help` on the client lists its options;
`PROFILE`, `ALLOC_STATS` and (on `profile.sh`) `OUT`, `RATE` are the env knobs, plus
`TOKIO_WORKER_THREADS` which tokio itself reads.

## Baseline

Runs live in [`spec/benchmark/report/`](report), one html report per date. The first is
2026-09-18: apple silicon, 12 cores, client and server on the same host, h2c, 64 concurrent streams,
server `TOKIO_WORKER_THREADS=4`, client `--threads 6`.

What it showed:

- **`#[api]` costs one allocation** over a plain controller and no measurable cpu. Allocation counts
  settled this; throughput could not, it moves by more than the difference between runs.
- **A post costs ~12 µs and 5 allocations more than a get**, for reading and parsing the body, and
  its throughput is a third lower.
- **Heap accounting is free** as sharded counters — tracked and untracked runs are
  indistinguishable — against 3.6x for the shared-atomics version.
- **Peak rss 8.5–9.1 MB, flat** across every run and scenario.
- **70 allocations and ~9 KB per request** on a get, unchanged by disabling tracing — the
  instrumentation never reached the point of formatting anything, so it cost instructions, not heap.
- **Third party `tracing` was ~8% of server cpu**, 32.7 µs → 29.5–30.4 µs per get request and
  44.7 µs → 41.3 µs per post, with throughput up ~7% on a post.

### The single h2c connection is the ceiling

Over half the server's samples are parked (51%) while throughput sits at 63-68k/s for a get — the
server has idle capacity it cannot reach. The profile says why: `kevent` is 28% of on-cpu time and
`__psynch_mutexdrop` another 7%, both of them the one h2 connection being driven from four worker
threads. Everything on a connection — framing, flow control, the write buffer — serializes behind
its lock, so worker threads queue instead of working.

For comparison, the same server on http/1.1 with 64 separate connections reached 70k/s for a get and
70k/s for a post, against 63k/s and 47k/s on h2c. h2c costs *less* cpu per request (32.7 µs vs
34.5 µs for a get, fewer syscalls thanks to multiplexing) and still delivers less, because that cpu
cannot be spread across cores.

**This is the shape of a real internal call path**, since `HttpClientConfig::internal_only()` gives
each caller one shared connection. It means a single client process cannot saturate a server no
matter how many requests it has in flight; scaling comes from more client processes, or from more
than one connection per peer.

The first run also showed `tracing::span::Span::log` and `record_all` at ~2% of on-cpu time, from
`h2`'s own instrumentation, which nothing in the framework reads. Compiling it out of release builds
took **~8% off server cpu per request** and removed both frames from the table — see
[`spec/third_party_instrumentation.md`](../third_party_instrumentation.md). The numbers below
are post-change; the pre-change run is still in the record file.

The per scenario method tables are in the report. The framework's own share is dominated by action
logging — the server layer formats every header and cookie into the action's buffer on every
request, and `TraceAppender` then discards it (`core::fmt::write` and `String::write_str` are ~2%
between them, plus the allocations behind them).

## Action allocation tuning

The action record now borrows what it can instead of rebuilding it: `ActionMessage` holds
`Cow<'static, str>` for `app`, `host`, `kind` and every context/stats key, context values are a
`SmallVec<[String; 1]>` that keeps the common single value inline, and completion **moves** the
context and stats vectors rather than collecting new ones. The wire format did not move — context
values are still json arrays, and gcloud still collapses a single value to a scalar at the edge. The
design and its rationale are in [`spec/action_log.md`](../action_log.md).

Measured as an A/B on 2026-09-19, same host and settings as the baseline (12 cores, h2c, 64 streams,
`TOKIO_WORKER_THREADS=4`, client `--threads 6`, 5 s warmup + 15 s measured, `ALLOC_STATS=1`).
Four rounds per side, run **interleaved** — after, baseline, baseline, after, then two more
baseline/after pairs — so machine drift cannot land on one side. Both sides are commit `9610644`;
the baseline side is that commit with the change stashed, which reproduced the 2026-09-18 numbers
exactly (70 allocations, 9,183 B for a get) and so is directly comparable to the baseline above.

| scenario | metric | baseline | after | delta |
|---|---|---|---|---|
| get | allocations / request | 70 | 51 | **−19 (−27%)** |
| get | bytes / request | 9,183 | 8,896 | −287 (−3.1%) |
| get | server cpu µs / request | 30.06 | 29.60 | −0.46 (−1.5%) |
| post | allocations / request | 75 | 55 | **−20 (−27%)** |
| post | bytes / request | 10,185 | 9,843 | −342 (−3.4%) |
| post | server cpu µs / request | 41.77 | 40.64 | −1.13 (−2.7%) |

Cpu is the mean of four runs. The get spread does not overlap at all — every after run (29.06–29.90)
came in under every baseline run (30.01–30.18); post overlaps on one pair of sixteen. That is the
only reason a sub-3% cpu claim is made at all, since the run to run spread on this host is wider
than the effect and no single pair would have shown it.

**Throughput and latency did not move**, as expected: 67.3k/s get and ~50.8k/s post on both sides,
p50 within 0.005 ms. The single h2c connection is the ceiling, so cpu freed on a worker thread has
nowhere to go — see the baseline section above.

The profile agrees with the allocation count: the three `libsystem_malloc` frames that held 5.1% of
on-cpu time between them on a get are down to two frames and 2.2%, with the third out of the top 15.
The `http_server_layer` frame itself is roughly unchanged (1.58% → 1.42%), which is the expected
shape — the work removed was in message construction, not in the layer's own body.

### Where the 19 went

Per action the change removes `2·(scalar contexts) + (stats keys) + 5` allocations: one vector and
one key string per scalar context, one key string per stat, plus `app`, `host`, `kind` and the two
rebuilt vectors. A benchmark get sets six contexts (`uri`, `method`, `client_ip`, `matched_path`,
`fn`, `response_status`) and two stats (`elapsed`, `response_content_length`) — 2·6 + 2 + 5 = 19. A
post adds `request_content_length` and saves 20. The counts are exact, not fitted.

The remaining ~9 KB per request is still unexplained; this change moved 3% of it.

### Consumer impact

These are public Rust field types, so an external appender that constructs or explicitly types an
`ActionMessage` needs updating even though nothing it writes or reads changed.
`log_processor_rs` was updated in the same change — alert helpers take the new slice types, and row
conversion moves owned strings out of the `Cow`s into the existing ClickHouse row types, with no
schema change. A regression test round-trips a message through json and asserts the deserialized,
owned form still converts.

## Known gaps

- **No baseline to compare against.** A number only means something next to another number: the same
  scenario on bare axum (no framework layer) would separate framework cost from axum and hyper cost.
  Plain vs `#[api]` is the only comparison the structure currently supports.
- **Client and server share the machine.** They compete for cores, so absolute throughput is
  understated and the client may become the limit. Fine for A/B comparisons on one host, not for
  absolute capacity.
- **Nothing compares runs automatically.** Results accumulate per date, but noticing a regression is
  still a human reading two reports.
- **~9 KB allocated per request is unexplained.** The count is solid, the composition is not — no
  allocation size histogram exists, so whether it is one big buffer or a long tail is a guess.
- **One connection, one client process.** Every measurement is 64 streams on a single h2c
  connection, which is what caps throughput. Nothing measures the server against several connections
  or several client processes, so its actual ceiling is unknown.
