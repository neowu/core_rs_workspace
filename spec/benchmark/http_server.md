# HTTP Server Benchmark

Code: [`benchmark/http_test_server`](../../benchmark/http_test_server),
[`benchmark/http_test_client`](../../benchmark/http_test_client),
[`benchmark/remote.sh`](../../benchmark/remote.sh),
[`benchmark/report`](../../benchmark/report) ·
results: [`report/`](../../report)

The workflow here is shared by every benchmark; the nats one is in [`nats_api_server.md`](nats_api_server.md).

Finds framework bottlenecks and compares designs. **Not micro benchmarks**: the unit is a whole
request over a real socket, so a run measures the framework path an app pays for (accept, http
layer, action log, routing, extractor, controller, serialization).

| process | role |
|---|---|
| `http_test_server` | target under test, a framework app with only the http server wired |
| `http_test_client` | measuring instrument, a closed loop load generator |

## Scenarios

| scenario | path | route |
|---|---|---|
| `get` | `/benchmark/get` | plain controller, `Query` |
| `post` | `/benchmark/post` | plain controller, `Json` |
| `api_get` | `/benchmark/api/get` | `#[api]` generated, `Query` |
| `api_post` | `/benchmark/api/post` | `#[api]` generated, `Json` |
| `db_select` | `GET /benchmark/db/select` | plain controller, `repository::select_one` by primary key |
| `db_insert_ignore` | `POST /benchmark/db/insert_ignore` | plain controller, `repository::insert_ignore` |

### DB scenarios

Postgres runs on the server host (trust auth, user `postgres`, empty password), the server
connects to `DB_URL` (default `postgres://localhost:5432/postgres`). The pool connects lazily, so
non-db scenarios need no postgres.

Before `verify`, the client calls `PUT /benchmark/init_db`, which drops and recreates the table and
seeds ids `1..=rows` (`--rows`, default 1000) through `repository::insert`, so every run starts
from the same state.

```sql
CREATE TABLE "benchmark_entity" (
    id      BIGINT PRIMARY KEY,
    name    TEXT NOT NULL,
    amount  BIGINT NOT NULL
)
```

| endpoint | request | response |
|---|---|---|
| `PUT /benchmark/init_db` | `{"rows":1000}` | `{"rows":1000}` |
| `GET /benchmark/db/select?id=7` | | `{"id":7,"name":"name-7","amount":700}` |
| `POST /benchmark/db/insert_ignore` | `{"id":1001,"name":"benchmark","amount":100}` | `{"id":1001,"inserted":true}` |

- `db_select` reads one fixed seeded id, a hot row: it measures the framework db path plus a round
  trip, not postgres disk reads.
- `db_insert_ignore` always sends id `rows + 1`: `verify` asserts the first insert lands, every
  later one conflicts (`inserted: false`), so the measured phase is the conflict path and the
  table never grows.
- A missing row is `NOT_FOUND` (404), which the measured loop counts as failed.

## Requirements

- Controllers do no work; plain and `#[api]` routes build responses through the same
  `GetResponse::new` / `PostResponse::new`, so they differ only in framework code.
- The server is a normal framework app (`System::init` / `start_logger` / `start_service`).
- Server and client run on two separate remote hosts (debian, over ssh), the local host only
  builds `report`, deploys and renders.
- The server exposes `GET /benchmark/info`: machine (host, ip, cpu, cores, memory, os), tokio
  workers, its own cpu time and peak rss at the moment of the call.
- The server binds `0.0.0.0:8080`, no override.

## Design decisions

- **Only `TraceAppender`**: action construction and channel send stay (real cost), nothing is
  written per request, so output never becomes the bottleneck.
- **h2c over one shared connection**, matching `framework::http::HttpClient` for internal calls.
  `verify` asserts HTTP/2. `--concurrency` is streams on one connection, which is also the ceiling.
- **Client uses `reqwest` directly, not `HttpClient`**, which logs every request and would make the
  instrument the bottleneck.
- **The measured loop never parses a response**; one `verify` request at startup parses and
  asserts, so a wrong url fails fast. Urls and bodies are prepared once.
- **Closed loop, fixed concurrency**: answers capacity and cost, not overload behaviour; latencies
  are subject to coordinated omission.
- **Every latency sample is kept** per worker, merged and sorted once: exact percentiles.
- **Warmup** is the same loop with the result discarded.
- **Cost is cpu per request**: the client calls `/benchmark/info` before and after the measured
  phase and divides the server's `getrusage` delta by requests. Throughput measures client and
  server together; cpu µs/req is the server alone.
- **Saturation is reported**: `cpu_pct` of both sides over the measured phase as a share of the
  host's cores, the side near 100 sets the rate.
- **No process wide heap accounting**: it would need its own `#[global_allocator]`, which
  conflicts with framework's. Per-action counts are in [`action_alloc_stats.md`](../action_alloc_stats.md).
- **Payload and info types live in the server's lib target**, which the client depends on, so
  shapes cannot drift. Machine info uses `std::process::Command` (`hostname`, `lscpu`,
  `/proc/meminfo`, `uname`), collected once before serving.
- **Crates are workspace members but not default members** and skip workspace lints.

## Results and report

- The client always writes one json result (`--output`, default `result.json`): `config`,
  `result`, `server`, `client` (and `broker` for nats). Console output is progress only.
- A day of one benchmark is a directory `report/<date>_<name>/` holding one result file per run,
  named by time (`HHMMSS.json`, `HHMMSS_profile.json`). The html `report/<date>_<name>.html` is
  derived from every file in it, so regenerating never loses anything.
- `report run <result.json> [key=value]...` adds what only the building host knows (`time`,
  `commit`, cargo `profile`) under `run`, then renders; `report render <dir>` re-renders by hand.
- The report shows one machine line per distinct server/client/build combination in the day.

## Remote workflow

`benchmark/remote.sh <run|profile> <http|nats_api> [client options]` with `SERVER` and `CLIENT` ssh
hosts (which hosts and what runs on them: [`server.md`](server.md)):

1. rsync the working tree to `/opt/build/src` on the server host and build both binaries there
   (native build, no cross toolchain; target dir kept for incremental builds)
2. copy each binary to `/opt/<binary>/`, the client via the local host (`scp -3`), so the two
   hosts need no ssh trust
3. start the server, wait until it is ready (http: `/health-check` from the client host)
4. run the client against the server's internal ip (`SERVER_IP` overrides)
5. download the result into the day's directory, stop the server, `report run` it

## Profiling

`remote.sh profile` builds `--profile profiling` with frame pointers, attaches `perf record -g` to
the server for the client run, runs `perf report` twice on the server (self time; total time with
children) and `report profile` stores the top methods under `profile` in the result file.

- perf, not samply: it samples only on-cpu threads, so parked workers never appear.
- Self time covers everything (libc, kernel); total time only methods of `framework*` crates and
  `*_test_server`, otherwise runtime and hyper frames take every row.
- Symbols are demangled with `rustc-demangle` and generic arguments stripped, so monomorphizations
  merge: self time sums, total time takes the largest.
- A profile renders below the runs, never as a run row, since the profiler skews throughput and cpu.

## Known gaps

- No bare-axum baseline to separate framework cost from axum/hyper.
- Nothing compares runs automatically.
- One connection, one client process: the server's real ceiling is unknown.
- DB scenarios: the framework pool holds at most 50 connections, a higher `--concurrency` queues
  on checkout. Postgres shares the server host, its cpu is not in the server's `cpu_us_per_request`
  but does compete for the same cores.
