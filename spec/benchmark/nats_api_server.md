# NATS API Benchmark

Code: [`benchmark/nats_api_test_server`](../../benchmark/nats_api_test_server),
[`benchmark/nats_api_test_client`](../../benchmark/nats_api_test_client),
[`benchmark/run_nats_api_test.sh`](../../benchmark/run_nats_api_test.sh),
[`benchmark/profile_nats_api_test.sh`](../../benchmark/profile_nats_api_test.sh),
[`benchmark/harness`](../../benchmark/harness), [`benchmark/report`](../../benchmark/report) ·
results: [`report/`](../../report)

The same workflow as [`http_server.md`](http_server.md) pointed at
[`framework_nats`](../../lib/framework_nats): a pair of processes, a closed loop client, cpu per
request and allocations per request as the numbers that mean something, one record file per date
(`<date>_nats_api_server.txt`) rendered to html beside it. **Only what differs is written down
here** — everything that document decides applies unchanged.

What a run measures is the framework path a nats request crosses: the queue subscription, the
semaphore and task spawn, the action log, header linkage (`ref_id`, `client`), payload decode, the
handler call, and the reply publish.

| process | role |
|---|---|
| `nats_api_test_server` | the target under test — a framework app with only the nats service wired |
| `nats_api_test_client` | the measuring instrument — a closed loop generator on raw `async-nats` |

## Subjects

| scenario | subject | payload |
|---|---|---|
| `get` | `api.benchmark.get` | one scalar |
| `post` | `api.benchmark.post` | an array sized by `--values` |

Both are `#[nats_api]` generated, because that is the only way a service is built — `Service`'s
constructor and handler registration are `#[doc(hidden)]` `__` methods the macro calls, not public
API. There is no hand registered counterpart to price the generated one against, the way the http
benchmark prices `#[api]` against a plain controller: on this side the generated handler *is* the
framework path, so a second registration style would be a benchmark of code nobody writes.

## Requirements

- A run needs a nats server (`container start nats`). This is the one way it departs from the http
  benchmark, which needs nothing: request/reply has a broker in the middle by definition, so the
  measurement includes a hop the http one does not have.
- Both sides default to `nats.test:4222` and take `NATS_URL`; the run script passes the same url to
  both, so client and server cannot end up on different brokers.
- Subjects live in `nats_api_test_server`'s lib target, which the client depends on, along with the
  payload types — the `#[subject]` attribute takes a literal, so the constants sit beside the trait
  and cannot be changed apart from it.
- The server wires nothing but `BenchmarkService::service(...)`. Anything the macro would not have
  generated is not the framework path an app runs.

## Design decisions

### One connection, mirroring a real caller

The client holds a single `async_nats::Client`, which is how a framework app holds
`framework_nats::connect` — every request is multiplexed onto it and every reply comes back on the
one shared inbox subscription. `--concurrency` is therefore outstanding requests on one connection,
not connections, the same shape (and the same ceiling) as the h2c http benchmark.

### The client is not `ServiceClient`

For the reason the http client is not `HttpClient`: `ServiceClient` opens a span, logs the request
and the whole reply payload, and records byte stats, which makes the instrument the bottleneck and
puts framework work on both sides of the wire. The client uses `async-nats` directly.

Benchmarking `ServiceClient` itself is a separate and legitimate target, and would need its own
client side scenario.

### The client sends the framework's link headers

`ServiceClient` puts `client` and `ref_id` on every call (`link_context`), and the service reads
both — `ref_id` becomes the action's ref id and `client` a context entry. A client that sent
a bare payload would measure a service nobody runs, so the headers go on every request. They are
built once and the per request cost is one clone of a two entry map, which is less than what a real
caller pays.

### The service semaphore is set out of the way

`ServiceConfig::max_concurrency` bounds in-flight handlers. Left at its default of 100 it would cap
a run at 128 concurrency and the report would show the semaphore, not the framework, so the server
defaults to 4096 and takes `MAX_CONCURRENCY`. Measuring the semaphore is a legitimate run; it should
be one that was asked for.

### Readiness is the absence of `NoResponders`

There is no health endpoint. Nats answers a request with `NoResponders` until something is
subscribed, so the client retries on exactly that error for 10 s and treats the first real reply as
readiness — the same request its `verify` step needs anyway. The run script additionally waits for
the service's own `start nats service` log line before starting the client, so a server that cannot
reach the broker fails the run with a message instead of a timeout.

### Errors are a header, not a status

The framework marks an error reply with an `error` header and puts `ErrorResponse` in the payload.
The measured loop checks that one header and never parses a body, which is the counterpart of the
http client's status check.

## Known gaps

Those of [`http_server.md`](http_server.md), plus:

- **The broker's cpu is not measured.** `cpu_us_per_request` is the server process alone, so the
  nats process — which handles every request twice, inbound and reply — is invisible, and the
  numbers are not comparable to the http ones, where nothing sits in between.
- **The broker is a container on the same host**, sharing cores with both processes and reached
  over its network. That is one more source of run to run spread than the http benchmark has.
- **Nothing measures jetstream, the producer or the consumer.** Only core request/reply is covered.

## First run

2026-09-22, apple silicon, 12 cores, client, server and broker on one host, 32 outstanding requests,
5 s measured after a 2 s warmup. Short runs — enough to establish the shape, not a baseline to
compare against.

| scenario | allocations / request | bytes / request | cpu µs / request |
|---|---|---|---|
| `get` | 33 | 5,177 | 19.4 |
| `post` | 35 | 5,276 | 20.3 |

What it shows:

- **A nats get costs ~33 allocations and ~5 KB** against 50 and ~8.9 KB for an http get on the same
  commit. The gap is the http server layer's own work: header and cookie parsing and the context it
  formats per request.
- **The generated handler's own share is two allocations**, measured against a hand registered
  handler before that path was removed: the `fn` context it sets formats a `String` from
  `type_name`. Kept here as a note, not as a scenario — see above.
- **60% of profile samples are parked** on both scenarios at this concurrency, the same single
  connection ceiling the http benchmark hits — throughput here is not the server's capacity. Of what
  is left, `kevent` alone is ~46% and `writev` another ~7%, so the top of both tables is the
  connection being driven from four worker threads rather than anything the framework does.
- **Peak rss 8.9 MB**, in line with the http server.
