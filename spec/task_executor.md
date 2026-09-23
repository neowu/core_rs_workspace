# Task executor design

Code: [`lib/framework/src/task.rs`](../lib/framework/src/task.rs) · callers:
[`schedule.rs`](../lib/framework/src/schedule.rs),
[`schedule/controller.rs`](../lib/framework/src/schedule/controller.rs),
[`framework_nats/src/service.rs`](../lib/framework_nats/src/service.rs),
[`framework_nats/src/consumer.rs`](../lib/framework_nats/src/consumer.rs) · siblings:
[`action_log.md`](action_log.md), [`nats.md`](nats.md), [`benchmark/nats_api_server.md`](benchmark/nats_api_server.md)

Two layers with different jobs:

| type | job |
|---|---|
| `TaskExecutor` | lifecycle only — spawn, track, drain on shutdown, name what did not finish |
| `Executor` | the process-wide one behind `spawn_action!`; wraps `TaskExecutor` and adds the action |

A server spawns a task per request, per message and per job firing, so `TaskExecutor::spawn` sits on
the same hot path as the action log and is priced the same way: whatever it does, it does tens of
thousands of times a second.

## Design decisions

### A task name is `&'static str`, and the bound is the point

Every spawn registers a name so shutdown can say which tasks were still running. The name is
`&'static str`: a literal, or a `&'static str` the caller already holds (a subject, a job name).

The bound exists because the alternatives could not express the rule. A parameter that accepts an
owned or shared string also accepts `format!(...)` written inline at the call site, which compiles,
reads fine, and quietly allocates per request; that is what every caller here did before, and one of
them was doing it twice. `&'static str` makes the per-request case something you have to write on
purpose, in a form that is obvious in review and greppable in the tree.

It is also faster than the `Arc<str>` it replaced, by more than the instruction count suggests:
~4% of server cpu per request on the nats benchmark. One name per subject means one refcount, so
every worker thread doing an increment on spawn and a decrement on completion was two read-modify-
writes per request on a single shared cache line. A `&'static str` copy touches nothing, and the
registry loses its drop glue entirely.

### Names are a bounded set, reused as-is

Callers pass the `&'static str` they already key on: the scheduler its job name, nats service and
consumer the subject. Nothing is decorated or built for the executor; prefixes like `request:` added
nothing the shutdown warning did not already say, since each executor is owned by one component and
logs its own leftovers. If a name ever must be derived, it is built once at registration and passed
through `string::intern`, which leaks — **never per spawn**. A caller that cannot name its tasks from
a bounded set does not belong on this API.

`spawn_action!` needs no interning at all. Its name and location are both literals at the macro site,
so the macro concatenates them into one, which costs nothing at runtime. This is why the macro takes
a literal rather than an expression — a `const` or a variable would compile into a worse API.

### The name is the task's identity, not a description of the run

The name answers "which task hung", so it carries only what distinguishes one task from another:
a subject, a job name. It deliberately does not carry per-run values. The scheduler used to append
the scheduled time, which made every firing a distinct string and could not be interned — and was
redundant, because the job's own action already records `scheduled_time`. Anything variable belongs
in the action record, which is the thing built to hold it.

## Behaviour and invariants

- A spawned task is registered before it starts and removed by a guard when it finishes, so the
  registry is exactly the set of in-flight tasks whatever the task does or panics on.
- `shutdown` stops accepting new tasks, then waits up to its timeout. It returns `None` if everything
  drained, otherwise the names still in flight at the deadline — which callers log and the runtime
  then abandons. The list is names, so duplicates are expected and meaningful.
- Ordering is the runtime's. Nothing here promises tasks start, finish or drain in spawn order.

## Known gaps

- **Registration is one mutex, taken twice per task.** Insert on spawn, remove on completion, across
  every worker thread. Nothing has isolated its cost or contention yet; it is section 5 of
  `plan/nats_api_tuning.md`, to be measured across worker counts and concurrency before anything is
  sharded or made optional.
- **A task's name is the only shutdown diagnostic.** There is no age, no stack, no originating action
  id, so an overrun tells you which subject hung and not which request.
