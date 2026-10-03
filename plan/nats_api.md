# NATS api overload: deadline, reject, retry

Status: idea, not started.

Code: [`framework_nats/src/service.rs`](../lib/framework_nats/src/service.rs),
[`framework_nats/src/lib.rs`](../lib/framework_nats/src/lib.rs) (`link_context`).
Contracts: [`nats.md`](../spec/nats.md), [`nats_api.md`](../spec/nats_api.md).

## Problem

A saturated `Service` stops reading, and requests pile up in the async-nats subscription buffer
(65,536 per subscription). Queue groups pick a member at random, not by load, so a busy instance
keeps getting its share. A request that waits in the buffer longer than the caller's request
timeout (async-nats default 10 s) is dead work: the handler runs, and the reply goes nowhere. On
release the drain runs that backlog too, delaying shutdown for nothing.

Measured: 100 permits, 100 ms handlers, 3000 req/s across two instances (both saturated), one
released mid-load: dropping the subscription failed 800 of 9000 requests, each only after the full
10 s timeout; draining failed none.

Capping the buffer (`ConnectOptions::subscription_capacity`) was rejected:

- async-nats drops a message silently when the buffer is full (it only emits `SlowConsumer`), so the
  caller still waits the full timeout.
- The setting applies to the whole client. The client is shared with JetStream batch inboxes and
  reply inboxes, so a cap could drop pulled JetStream messages, leaving them unacked until
  `ack_wait`.

## 1. Deadline header

- `ServiceClient` adds a `deadline` link header: absolute epoch ms, now + request timeout.
- Before decoding, the service checks it. If the deadline has passed, it replies with an error
  `NATS_DEADLINE_EXCEEDED` and does not call the handler. The caller is already gone, so this just
  skips the work. It applies in the main loop and in the shutdown drain.
- It has to be absolute: core NATS messages carry no timestamp, and the service can't see when a
  request entered the client buffer. This relies on clock sync (NTP skew is ms, against a 10 s
  timeout); allow a small grace.
- A request without the header (an older client) is handled as today.

## 2. Fast reject when saturated (HTTP 503 equivalent)

- Saturated past a threshold, the service replies `NATS_SERVICE_BUSY` right away instead of
  buffering. The caller learns in ms, not after 10 s.
- This needs reading while saturated: the main loop reads, then `try_acquire`s a permit, and on
  failure either rejects, or keeps a small bounded local wait queue and rejects beyond it.
  Today's loop takes the permit first, so the buffer absorbs the overflow instead.
- Open: the threshold. Options are reject at once when no permit is free, a fixed wait queue
  length, or a max queue time. Also open: whether `max_concurrency` alone is the right signal.

## 3. Client retry

In `ServiceClient`, by error:

| error | handler ran? | retry |
|---|---|---|
| `NATS_NO_RESPONDERS` | no, no subscriber | yes |
| `NATS_SERVICE_BUSY` | no, rejected before handler | yes |
| `NATS_DEADLINE_EXCEEDED` | no, but the caller's deadline is spent | no |
| `NATS_TIMEOUT` | unknown: not run, or reply lost | only if idempotent |

- A retry lands on a random queue group member, so with n instances it hits another one with
  probability (n-1)/n. A release (drain) or overload on one instance is then absorbed by the others.
- Backoff with jitter. All attempts share one overall deadline: each retry sends the remaining time
  as its `deadline`, rather than a fresh full timeout.
- `NATS_TIMEOUT` retry is opt-in per method, e.g. `#[idempotent]` on a `#[nats_api]` method. Being
  stateless does not make a method idempotent.

## Order

1 first: small, it reuses the existing link headers, and it removes the dead work under saturation
and in the shutdown drain. 2 and 3 together: a reject is only useful with a client that retries it.
