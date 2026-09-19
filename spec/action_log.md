# Action log design

Code: [`lib/framework/src/log.rs`](../lib/framework/src/log.rs),
[`log/action.rs`](../lib/framework/src/log/action.rs), [`log/span.rs`](../lib/framework/src/log/span.rs),
[`log/id_generator.rs`](../lib/framework/src/log/id_generator.rs),
[`appender.rs`](../lib/framework/src/appender.rs) · producers:
[`web/server.rs`](../lib/framework/src/web/server.rs), [`task.rs`](../lib/framework/src/task.rs),
[`framework_nats/src/consumer.rs`](../lib/framework_nats/src/consumer.rs) · consumers:
[`cloud/gcloud.rs`](../lib/framework/src/cloud/gcloud.rs),
[`framework_nats/src/appender.rs`](../lib/framework_nats/src/appender.rs),
[`log_processor_rs`](../app/log_processor_rs/src/nats/action_handler.rs) · siblings:
[`action_future_design.md`](action_future_design.md), [`benchmark/http_server.md`](benchmark/http_server.md)

An **action** is one unit of work an app performs — an http request, a consumed message, a scheduled
task — and it produces **exactly one** structured record when it finishes. That record is the app's
primary observability output: there is no separate per-line logging pipeline, no log level to
configure, and nothing an app has to remember to emit.

The design follows from a single constraint: **an action's record must be cheap enough to produce on
every request, in production, always on.** Everything below is a consequence — the record is built
in a task local with no locking, keys are static strings so nothing hashes, the trace lives in one
buffer, and the whole thing leaves the request path over a channel.

## The record

| field | source | notes |
|---|---|---|
| `id` | `id_generator::next_id` | 20 hex chars, timestamp + machine id + counter |
| `kind` | `log::action(kind, ..)` | `"http"`, `"message"`, `"task"`, `"nats"`, `"test"` |
| `timestamp`, `stats.elapsed` | the action's own clock | `elapsed` is nanos, always stats slot 0 |
| `severity`, `error_code`, `error_message` | promoted from log lines and exceptions | see severity promotion |
| `ref_ids` | the caller's id, off the transport header | how a call chain is reassembled |
| `context` | `context!(key = value)` | ordered key → **list** of values, queryable dimensions |
| `stats` | `stats!(key = value)`, `span!` | ordered key → `u64`, numbers that add up |
| `logs` | `log!`, `warn!`, `error!`, `span!` | the trace buffer, emitted only when it is worth keeping |

## Lifecycle

1. `log::action(kind, ref_ids, task)` builds an `Action` and scopes it as the `CURRENT_ACTION` task
   local for the task's duration. The future is hand written so the task is stored exactly once —
   that is its own document, [`action_future_design.md`](action_future_design.md).
2. Framework layers and app code write into it through the macros, which all go through
   `CURRENT_ACTION.try_with`. Outside an action `try_with` returns `Err` and the call is a no-op;
   this is the only "no current action" signal, and it is never an error.
3. When the task resolves, the `Action` is taken back out of the future's own slot, an `Err` result
   is logged as an exception, `finish` records `elapsed`, and the action converts into an
   `ActionMessage`.
4. The message goes onto the unbounded channel to the appender daemon and the request path is done
   with it.

## Design decisions

### The action is a task local, not a parameter

`log!` and `context!` are callable from anywhere inside the action's scope with no plumbing, which
is what makes it realistic for a framework layer, a controller and a library call to all write into
the same record. The cost is that it works only inside the scope — a `tokio::spawn` inside an action
starts a fresh, empty context, and code that needs one there uses `spawn_action!`, which opens its
own `"task"` action and links back to the parent through `ref_ids` rather than trying to inherit
the record itself.

`RefCell<Action>` rather than `Mutex`: an action belongs to one task, contention is impossible, and
a borrow panic would mean a genuine reentrancy bug. The one real hazard is documented at the macro
itself — never call `log!` from a `Display` impl that is being passed *as an argument to* `log!`,
which borrows the `RefCell` twice.

### Three shapes, because they are read three different ways

`context` is **dimensions you filter and group by** (uri, client_ip, matched_path). `stats` are
**numbers that sum and average** (elapsed, bytes, span counts). The trace is **prose you read when
something already went wrong**. Collapsing them into one bag of strings would mean the query side
has to guess which is which, so they stay separate all the way to storage —
[`log_processor_rs`](../app/log_processor_rs/src/nats/action_handler.rs) lands them in
`context`/`multi_context`, `stats` and a separate `trace_rs` table.

Every `context!` and `stats!` call also writes its own `[context]`/`[stats]` line into the trace, so
a trace read on its own is self-contained.

### Keys are compile-time literals, so nothing hashes and nothing allocates

`context!`, `stats!` and `span!` take an identifier or a literal and go through `stringify!` /
`concat!`; `span!("db")` builds `"db_elapsed"` and `"db_count"` at compile time. The storage is
therefore an ordered `Vec` of `(&'static str, _)` pairs, not a `HashMap`: an action carries on the
order of 20 keys, so `add_stat`'s linear scan beats hashing a string on every write, and the
insertion order that a person reading the record expects is preserved for free.

A dynamic key is deliberately not supported. It would force owned keys and hashing on every action
to serve a case that has not come up.

### Stats accumulate, context appends

`add_stat` adds into an existing key; two `span!("db")` blocks produce one `db_elapsed` total and
`db_count=2`. `context!` appends, so a repeated key appears twice rather than silently overwriting.
Numbers are aggregates; dimensions are observations.

Slot 0 of `stats` is reserved for `elapsed` at construction, so it always leads the record and the
vector allocates exactly once.

### The trace is always collected and rarely emitted

Every action accumulates its full trace. It is attached to the message only when
`flush_trace()` — the action ended in an error, or something called `log::trace()`. A successful
action's buffer is dropped.

Collection cannot be made conditional on the outcome, because **the outcome is known last**. An
error at the end of a request is exactly when the header and cookie lines from its start are worth
having, and those were written before anything knew the request would fail. `TraceAppender`
discarding every successful trace is not a reason to stop collecting — it is the reason the
collection has to be cheap, which is what the cost section below is about.

### One `String` buffer, not a `Vec<String>`

Lines are appended into a single `String` (initial capacity 1 KB) separated by `'\n'`. Emitting it
is then a move, not a join, and a trace of 200 lines costs one growing buffer instead of 200
allocations. Consumers that want lines back split on `'\n'` —
[`gcloud.rs`](../lib/framework/src/cloud/gcloud.rs) does, to emit one gcloud entry per line with an
`insertId` that keeps them ordered under the action's trace id.

`Span::clear()` exploits the same layout: a span records the buffer offset where it started, and
`clear()` truncates back to it, so a long loop can keep the last iteration's trace without letting
the buffer grow without bound. It is guarded on a char boundary — a crossed (non-nested) span can
hold a stale offset, and truncating mid-char would panic.

### Every limit truncates in place and says so

| limit | value | applies to |
|---|---|---|
| `MAX_LOG_BYTES` | 512 KB | the whole trace buffer, **soft** |
| `MAX_LOG_MESSAGE_LEN` | 10,000 | one log line's message |
| `MAX_CONTEXT_VALUE_LEN` | 1,000 | one context value |
| `MAX_ERROR_MESSAGE_LEN` | 200 | the record's `error_message` |

All of them cut on a char boundary and append `...(truncated)`, which is appended **only when
something was actually cut**, so a value at exactly the limit is not misreported as truncated.

The buffer cap is soft and deliberately so: once it is reached a `...(log limit reached)` marker is
written once, and after that **`Severity::Error` lines still go in** while everything else is
dropped. An action that produced a megabyte of trace and then failed is the case where the trace
matters most, and a hard cap would drop precisely the line that explains it.

### Severity is promoted, the highest one wins and keeps its error

An action starts at `Info`. A `warn!`, `error!` or logged exception promotes the action's severity
and sets `error_code`/`error_message`; a later, lower-severity line does not replace what a higher
one recorded. The action's severity is therefore the worst thing that happened in it, and
`error_code` identifies that same thing rather than whatever happened last.

An exception's `error_message` comes from the exception itself, not from the log line — the line
carries the backtrace too, which does not belong in a queryable field.

### The appender is a daemon behind an unbounded channel

The producing task does a `send` and moves on; a single daemon owns the `Appender` and awaits it. An
action is never written from the request path, so a slow log sink cannot become a slow app.

The channel is **unbounded** because the alternative is worse: a bounded channel either blocks the
request (the thing this design exists to avoid) or drops records silently. An app that produces
records faster than its appender drains them has a capacity problem that back-pressuring the log
would hide rather than fix.

Shutdown closes the channel and keeps looping until it is drained, then calls `flush()` — buffered
appenders (`NatsAppender` queues on the client's connection task) need that call or their last
messages die with the runtime.

`SENDER` is a `OnceLock` set by `start_logger`. Before it is set — early startup, unit tests — the
action is simply not emitted; nothing panics.

### The action id encodes where and when, not randomness

20 hex chars: 5 bytes of millisecond timestamp, 3 bytes of machine id (hostname hash mixed with a
random word, so two pods on one host still differ), 2 bytes of a wrapping counter. Ids are roughly
time-ordered, which is what makes them usable as a sort key and as a gcloud trace id, and they are
built into a fixed 20-byte buffer with no formatting machinery.

### Locally produced metadata is borrowed, scalar context values are inline

`ActionMessage` holds `Cow<'static, str>` for `app`, `host`, `kind` and every context/stats key, and
`ContextValues = SmallVec<[String; 1]>` for context values. The action's `context` and `stats`
vectors are then **moved** into the message rather than rebuilt.

This is the one place the record's shape is driven by cost rather than by how it is read. Everything
it borrows is already `'static` or process-lifetime: keys come from `stringify!`/`concat!`, `kind`
is a `&'static str`, and the system `Context` (`app`, `host`) is set once at `System::init` and
lives until exit. The `Cow` is what lets a *deserialized* message still own its strings, which is
what `log_processor_rs` receives off nats.

Per action this removes `2·(scalar contexts) + (stats keys) + 5` allocations — one vector and one
key string per scalar context, one key string per stat, plus `app`, `host`, `kind` and the two
rebuilt vectors. For a benchmark get (6 contexts, 2 stats) that is 19; a post adds
`request_content_length` and saves 20. Measured numbers are in
[`benchmark/http_server.md`](benchmark/http_server.md).

### The wire format is fixed: a context value is always an array

`ContextValues` serializes as a json array even for the single value that it stores inline, so the
representation change was invisible on the wire and no stored data or downstream schema moved.
Collapsing a single value to a scalar happens **at the edge, per consumer**, where it is a display
choice: `ConsoleAppender` prints `key=value`, gcloud's `serialize_key_value_tuple` writes a scalar
field, and `log_processor_rs` routes single values to `context` and the rest to `multi_context`.

Doing it in the message instead would make the field's json type depend on the data, which every
consumer would then have to handle — for a saving that only defers an allocation to the reader.

## Behaviour and invariants

- **An action that never resolves is never recorded.** A cancelled or dropped task emits nothing;
  the record is written from the completion path.
- **Every macro is a no-op outside an action**, including `log::trace()` and `Span`'s `Drop`.
- **`Span` is a guard**: its stats are added when it drops, so its `elapsed` includes everything up
  to the end of its scope, and an early return still records it.
- **`/health-check` opens no action at all.** A load balancer probing every second would otherwise
  dominate the record volume with nothing to learn from.
- **Context and stats keep insertion order** end to end. Only the final `HashMap` in ClickHouse
  drops it.
- **The record is produced exactly once per action**, even on error — the error path adds fields, it
  does not add a second record.

## Downstream

| appender | output |
|---|---|
| `ConsoleAppender` | one `ACTION: ..` line on stdout, trace on stderr |
| `TraceAppender` | nothing but the trace, on stderr — used where the record is not wanted |
| `GCloudAppender` | one json entry per action plus one per trace line, ordered by `insertId` |
| `NatsAppender` | the `ActionMessage` json on `log.action`, plus console output for errors |

`log_processor_rs` consumes `log.action` in batches and writes `action_rs` (one row per action) and
`trace_rs` (one row per traced action), splitting single from multi-valued context, and single from
multiple `ref_id`s, so the common case stays a scalar column.

## Cost

Measured on the http server benchmark, which exists for exactly this question; numbers and method in
[`benchmark/http_server.md`](benchmark/http_server.md). Two things are worth carrying here:

- Action logging is the framework's largest own share of a request. The header and cookie lines are
  formatted into the buffer on every request, and `core::fmt::write` plus `String::write_str` are
  ~2% of on-cpu time between them even when `TraceAppender` then discards the result.
- The allocation tuning above removed 19–20 allocations per request, about 27% of the total, for
  ~1.5% (get) to ~2.7% (post) less server cpu per request. Allocation counts are deterministic and
  settled it; cpu time alone could not have.

## Known gaps

- **No regression guard on the record's cost.** Allocations per request are measurable but nothing
  fails when they go up; noticing is still a person comparing two benchmark reports.
- **Per-line trace overhead is unamortized.** Every line re-reads the task local and re-formats the
  elapsed prefix. A header-block writer could share both across a block of lines, at the price of
  one timestamp per block.
- **The trace is collected even when no appender could ever emit it.** There is no way for an
  appender to declare that it never wants traces, and `flush_trace` is decided after the fact.
- **Context values are capped per value, not per action.** An action setting many large values can
  still produce a large record, and only the trace buffer has an overall ceiling.
