# NATS API Benchmark

Code: [`benchmark/nats_api_test_server`](../../benchmark/nats_api_test_server),
[`benchmark/nats_api_test_client`](../../benchmark/nats_api_test_client),
[`benchmark/remote.sh`](../../benchmark/remote.sh),
[`benchmark/report`](../../benchmark/report) ·
results: [`report/`](../../report)

The workflow of [`http_server.md`](http_server.md) pointed at [`framework_nats`](../../lib/framework_nats)
(`remote.sh <run|profile> nats_api`), on the hosts in [`server.md`](server.md). **Only what differs
is written here.**

A run measures the path a nats request crosses: queue subscription, semaphore and task spawn,
action log, payload decode, handler, reply publish.

| process | role |
|---|---|
| `nats_api_test_server` | target under test, a framework app with only the nats service wired |
| `nats_api_test_client` | measuring instrument, a closed loop generator on the generated `BenchmarkServiceClient` |

## Subjects

| scenario | subject | payload |
|---|---|---|
| `get` | `api.benchmark.get` | one scalar |
| `post` | `api.benchmark.post` | an array sized by `--values` |

`api.benchmark.info` is the counterpart of `/benchmark/info`. All are `#[nats_api]` generated, the
only way a service is built, so there is no hand registered variant to compare against.

## Requirements

- A broker sits in the middle. It runs on the server host (`nats-server`, provisioned as a
  systemd service; `remote.sh` aborts if it is not running). The server reaches it on
  `localhost`, the client on the server's internal ip. The result records broker url and version.
- The `#[nats_api]` trait and payload types live in the server's lib target, which the client depends on.
- The server wires nothing but `BenchmarkService::service(...)`.

## Design decisions

- **One connection**, as a framework app holds `framework_nats::connect`; `--concurrency` is
  outstanding requests on it.
- **Client is the `#[nats_api]` generated one**, with no action context, so its logging is a no-op
  and a call costs request encode and reply decode on top of the raw request.
- **No link headers are sent**: `client` needs `System::init` and `ref_id` an action, the client
  has neither, so the service's header linkage is not exercised.
- **Service semaphore out of the way**: `max_concurrency` defaults to 4096 (`MAX_CONCURRENCY`
  overrides), otherwise a run measures the semaphore.
- **Readiness is the absence of `NoResponders`**: the client retries on it before `verify`;
  `remote.sh` also waits for the `start nats service` log line so a broker problem fails with a message.
- **`errors` are requests with no reply** (`NATS_NO_RESPONDERS`, `NATS_TIMEOUT`), **`failed` is any
  other exception**, an error reply included.

## Known gaps

Those of [`http_server.md`](http_server.md), plus:

- Broker cpu is not recorded and shares the server host's cores, so numbers are not comparable to http.
- Jetstream, producer and consumer are not covered, only core request/reply.
