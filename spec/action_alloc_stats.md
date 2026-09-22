# Action allocation stats design

Code: [`lib/framework/src/log/alloc_stats.rs`](../lib/framework/src/log/alloc_stats.rs) · producer:
[`log.rs`](../lib/framework/src/log.rs) (`ActionFuture`) · process wide twin:
[`benchmark/http_test_server/src/alloc_stats.rs`](../benchmark/http_test_server/src/alloc_stats.rs) ·
siblings: [`action_log.md`](action_log.md), [`action_future_design.md`](action_future_design.md),
[`benchmark/http_server.md`](benchmark/http_server.md)

`alloc_count` and `alloc_bytes` are stats on every action record: how many allocations that action
cost and how many bytes they asked for. They make "which endpoint allocates" a query, where a cpu
profile and a process wide counter can only hint at it.

The feature is `framework/alloc_stats`, and it is a **default feature** — every app gets the numbers
without remembering a flag.

## On by default because it measured free

On the http server benchmark, with the server pinned to one saturated core so requests/sec is its
own cpu cost and nothing else, four interleaved runs per side:

| scenario | without | with | delta |
|---|---|---|---|
| `get` | 130,955 req/s | 129,630 req/s | −1.0% |
| `post` | 108,541 req/s | 108,976 req/s | +0.4% |

The signs disagree and each build's own run to run spread is wider than the gap, so the cost is
under what the harness resolves. An earlier pass on cpu per request agreed: 28.63 vs 28.52
µs/request on a `get`, ranges fully overlapping.

A build that wants the numbers gone, or that wants a different `#[global_allocator]` such as
jemalloc, takes framework with `default-features = false`.

## Attribution is per poll

`ActionFuture` reads the current thread's counters around every `inner.poll` and accumulates the
delta. A poll runs start to finish on one thread and two polls never interleave on one thread, so
the delta is that action's own work even though the task migrates between workers.

Taking it **per poll** is not optional: `poll` returns `Pending` on every poll but the last, and
everything allocated before each of those would be lost if the delta were taken behind `ready!`.

**What falls outside the poll window is charged to nobody.** Connection setup and header parsing
happen before the action opens; the `ActionMessage` conversion, `log_exception`'s backtrace and the
appender happen after the last poll. A benchmark `get` reports `alloc_count=30` against 51
allocations per request process wide. The number is comparable between actions, not a complete
memory bill.

## Actions do not overlap, by convention

Nothing opens a second `log::action` inside one, so a poll window belongs to exactly one action and
the counter delta needs no scope stack. A nested action would double count: the inner allocations
would appear in both records, and the inner finalization would be charged to the outer action.

The fix for that — a guard that pauses the parent scope on entry and restores it on exit — costs a
branch on every poll and buys nothing while the convention holds, so it is deliberately absent. The
convention is the simpler half of the trade, not an oversight.

## The counters are thread local and non atomic

They are only ever read by the thread that wrote them, which is what lets them be a plain `Cell`:
no sharing, no cache line contention, no atomics at all. The process wide twin in
`benchmark/http_test_server` needs 64 padded shards of `AtomicU64` for exactly the reason this does
not — it sums across threads.

## Deallocation is not tracked

There is no per-action live or peak bytes. A block allocated in an action is routinely freed by
another task — the `ActionMessage` is freed by the appender daemon — so per-action live bytes would
be meaningless and frequently negative, and stats are `u64`. Process rss stays
`MetricsCollector`'s job.

## The feature installs the allocator itself

An app therefore cannot enable it and silently record zeros. The cost is that the crate graph must
not declare a second `#[global_allocator]`, which the benchmark servers do under a feature of the
same name. Those crates take framework with `default-features = false` and
[`run_http_test.sh`](../benchmark/run_http_test.sh) /
[`run_nats_api_test.sh`](../benchmark/run_nats_api_test.sh) turn the per-action one back on for every
run that is not measuring the process wide counter, so the two stay mutually exclusive and every
recorded run says which it ran under.

For the same reason a framework companion crate propagates the feature rather than forcing it:
`framework_nats` takes framework with `default-features = false` and re-exports `alloc_stats` as its
own default. Depending on it with framework's defaults on would put the allocator back into any
graph that meant to replace it.

## Known gaps

- **No regression guard.** Allocations are per action and per endpoint on every build, but nothing
  fails when they go up; noticing is still a person comparing two reports.
- **The feature's own cpu cost is bounded, not measured.** Both harnesses above put it under what
  either resolves. It ships on by default on that bound, not on a resolved number.
