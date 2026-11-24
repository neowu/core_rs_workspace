# In-flight actions design

Code: [`lib/framework/src/log/inflight.rs`](../lib/framework/src/log/inflight.rs) ·
endpoint: [`web/diagnostic.rs`](../lib/framework/src/web/diagnostic.rs) · siblings:
[`action_log.md`](action_log.md), [`action_future.md`](action_future.md),
[`thread_dump.md`](thread_dump.md), [`http_server.md`](http_server.md)

An action only produces its record when it finishes, so a stuck one is invisible. `GET /sys/action`
lists the actions that have not finished, oldest first, each with the trace it has written so far:
the last line of a stuck action is usually what it is waiting on.

Apps serve `web::diagnostic::routes()` from the sys http server, bound to loopback, as every `/sys/`
endpoint: [`http_server.md`](http_server.md).

```
in_flight_actions=2

# [in-flight] id=.., kind=http, state=suspended, elapsed=12.5s, idle=12.4s, poll_count=3, poll_elapsed=1.2ms, alloc_count=40, alloc_bytes=9000
# [action] id=.., kind=http, date=.., ref_id=None
00:00.000081292 framework::web::server:231 [context] path=/slow
..

# [in-flight] id=.., kind=task, state=running, elapsed=3.1s
```

- `suspended`: waiting, not on any thread. `idle` is the time since its last poll returned.
- `running`: being polled right now, only id, kind and elapsed are shown, the rest is being written.
  The cell keeps its own copy of those three, written once, so they are read without touching the action.
  An action that stays `running` is holding a worker thread: blocking call, lock, busy loop,
  `/sys/thread` has its stack.
- The dump request itself is not listed. The trace is masked, as on finish.

## Design decisions

### The action lives in the future, at a fixed address

`ActionFuture` owns an `ActionCell` (the action and its poll stats), and the thread's current action
is a thread local pointer to it, set for the duration of each poll. A tokio task local was not
readable from outside: it moves the value between the future and the thread on every poll, and only
hands it back by taking it.

The cell registers its address on the first poll, when the future is pinned, and unregisters when
the action finishes or the future is dropped. An action created but never polled is not listed.

### Cost is paid per action and per poll, never per log line

| when | cost |
|---|---|
| first poll, finish / drop | one uncontended shard lock each |
| every poll | one compare-and-swap in, one store out, on a byte the task owns |
| `log!` / `context!` / `stats!` | nothing, still a thread local read and a `RefCell` borrow |

The registry is 16 shards, a thread registers into its own, so the lock is only shared when a task
finishes on another thread than it started on.

### The dump reads another task's action without its help

A stuck action is never polled again, so it cannot be asked to report itself. The dump reads the cell
directly, guarded by two things:

- the shard lock: unregistering takes it, so a listed cell is alive while it is read
- the cell state `IDLE` / `POLLING` / `DUMPING`: the dump only reads an `IDLE` cell, and a poll
  arriving meanwhile spins until the copy is done

A `POLLING` cell is reported as `running` from the cell's own id, kind and start time; the action
is not read, the task may hold a mutable borrow of it. All three are immutable and `Copy`, the id a
fixed `[u8; 20]`, so the cell holds plain copies taken when it is created.

### The dump is streamed, one action per chunk

Many stuck actions is exactly when the dump is needed, and each trace can be 512KB. The handler only
lists the cells (a pointer and a start time each), the response body then pulls one action at a time
as the connection is ready to write: lock the shard, check the cell is still registered, copy its
trace, unlock, mask. One trace is in memory at a time and a shard is locked for one trace at a time.

`Response::chunks` is the body for this, a synchronous iterator of chunks.

## Behaviour and invariants

- Starting and finishing actions on a shard wait while the dump copies one trace of that shard, a
  poll of the action being copied spins. Both last as long as copying that trace.
- The dump is a snapshot per action, not across actions. `in_flight_actions` is the count when the
  request was handled, an action that finishes before its chunk is pulled is left out.
- The task is dropped with its action as current, on completion and on cancellation, so a span or
  guard it holds never logs into an outer action.
- Nested actions restore the outer one as current when the inner poll returns.

## Verifying the unsafe code

The cell is read through raw pointers from two threads, so the aliasing and data race rules are
checked by Miri, not by the compiler. Run it after any change to `inflight.rs` or `ActionFuture`:

```
CARGO_BUILD_WARNINGS=warn MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-many-seeds=0..64" \
    cargo +nightly miri test -p framework --lib -- log::inflight log::tests log::span
```

- `CARGO_BUILD_WARNINGS=warn`: Miri builds its own sysroot, which the workspace's
  `build.warnings = "deny"` fails on an upstream warning.
- `-Zmiri-disable-isolation`: actions read the system clock.
- `-Zmiri-many-seeds`: reruns under different thread schedules, for the test that dumps from a
  second thread while the action is borrowed.
- `-Zmiri-tree-borrows` checks the same tests under the other aliasing model.

Last run 2026-11-18, macOS aarch64, miri 0.1.0 (5c543b0b8c): 13 tests clean under Stacked Borrows
and Tree Borrows, `log::inflight` clean across 64 seeds.

Not covered: a poll spinning while the dump copies its action (no test reaches it), and
`thread.rs`, Miri cannot run signals or the OS calls.

## Known gaps

- **Cost is bounded, not measured.** No benchmark run compares it with the task local it replaced.
- **No await point.** A suspended action shows its trace, not where its future is parked.
- **A running action shows no trace or context.** `GET /sys/thread` shows the stack of the thread
  polling it, matched by action id: [`thread_dump.md`](thread_dump.md).
