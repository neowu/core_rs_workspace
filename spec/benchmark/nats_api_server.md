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
action log, header linkage (`ref_id`, `client`), payload decode, handler, reply publish.

| process | role |
|---|---|
| `nats_api_test_server` | target under test, a framework app with only the nats service wired |
| `nats_api_test_client` | measuring instrument, a closed loop generator on raw `async-nats` |

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
- Subjects and payload types live in the server's lib target, which the client depends on.
- The server wires nothing but `BenchmarkService::service(...)`.

## Design decisions

- **One connection**, as a framework app holds `framework_nats::connect`; `--concurrency` is
  outstanding requests on it.
- **Client is not `ServiceClient`**, which logs every call and would make the instrument the bottleneck.
- **The client sends the framework's link headers** (`client`, `ref_id`), so the service does its
  production header work.
- **Service semaphore out of the way**: `max_concurrency` defaults to 4096 (`MAX_CONCURRENCY`
  overrides), otherwise a run measures the semaphore.
- **Readiness is the absence of `NoResponders`**: the client retries on it before `verify`;
  `remote.sh` also waits for the `start nats service` log line so a broker problem fails with a message.
- **Errors are the `error` header**, checked without parsing the body.

## Known gaps

Those of [`http_server.md`](http_server.md), plus:

- Broker cpu is not recorded and shares the server host's cores, so numbers are not comparable to http.
- Jetstream, producer and consumer are not covered, only core request/reply.
