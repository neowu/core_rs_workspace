# Action allocation stats design

Code: [`lib/framework/src/log/alloc_stats.rs`](../lib/framework/src/log/alloc_stats.rs) · producer:
[`log.rs`](../lib/framework/src/log.rs) (`ActionFuture`) ·
siblings: [`action_log.md`](action_log.md), [`action_future_design.md`](action_future_design.md),
[`benchmark/http_server.md`](benchmark/http_server.md)

`alloc_count` and `alloc_bytes` are stats on every action record: how many allocations that action
cost and how many bytes they asked for. They make "which endpoint allocates" a query, where a cpu
profile can only hint at it.

It is **always on**, not a feature flag: the numbers proved useful in production and the overhead
is minimal, so every app gets them and there is no build without them.

## Always on because it measured free

On the http server benchmark, counting allocator on vs off (no `#[global_allocator]`, `poll` a
plain call), every scenario, 3 interleaved 20s rounds per side, server cpu µs per request (commit
097a106, 2 core hosts; the client saturates first, so throughput cannot show it):

| scenario | on | off | delta |
|---|---|---|---|
| `get` | 30.06 | 29.78 | +0.9% |
| `post` | 38.91 | 38.26 | +1.7% |
| `api_get` | 29.44 | 29.37 | +0.2% |
| `api_post` | 36.77 | 37.37 | −1.6% |
| `db_select` | 83.64 | 83.24 | +0.5% |
| `db_insert_ignore` | 87.0 | 87.5 | −0.6% |

`db_insert_ignore` "on" excludes one outlier run (98 µs, p99 twice the others). The signs disagree
and every scenario's on/off ranges overlap: the cost is under the harness's ~±2% resolution. The
estimate agrees — one thread local add per allocation, ~50 allocations on a 30 µs `get`, ~0.3%.

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
no sharing, no cache line contention, no atomics at all.

## Deallocation is not tracked

There is no per-action live or peak bytes. A block allocated in an action is routinely freed by
another task — the `ActionMessage` is freed by the appender daemon — so per-action live bytes would
be meaningless and frequently negative, and stats are `u64`. Process rss stays
`MetricsCollector`'s job.

## Framework owns the global allocator

Framework installs the counting `#[global_allocator]` unconditionally, so no app can record zeros
and no crate graph that depends on framework can declare another one — an app cannot swap in
jemalloc, and the benchmarks dropped their own process wide counter rather than juggle the two.
Companion crates such as `framework_nats` take framework as a plain dependency, with nothing to
propagate.

## Known gaps

- **No regression guard.** Allocations are per action and per endpoint on every build, but nothing
  fails when they go up; noticing is still a person comparing two reports.
- **Its own cpu cost is bounded, not measured.** The benchmark above puts it under what the
  harness resolves. It is always on by that bound, not by a resolved number.
