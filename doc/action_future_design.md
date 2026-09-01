# `ActionFuture` design

Code: [`lib/framework/src/log.rs`](../lib/framework/src/log.rs) · consumers:
[`framework_nats/src/consumer.rs`](../lib/framework_nats/src/consumer.rs),
[`framework_kafka/src/consumer.rs`](../lib/framework_kafka/src/consumer.rs)

`log::action(kind, ref_ids, task)` wraps a future so an `Action` (id, kind, context, stats, log
buffer) is a task local for its duration and is emitted to the appender when it resolves.

It is a plain `fn` returning a hand-written `ActionFuture<F>` that stores `task` **exactly once**.
This document exists because the obvious implementations do not: writing it as an `async fn` costs
3x the task's size, and any `async` block that owns the task costs 2x. Both are structural
properties of rustc's coroutine layout, not something an optimiser removes.

## Why size matters here

`clippy::large_futures` denies futures over `future-size-threshold` (default 16384), and the
workspace sets `warnings = "deny"` in [`.cargo/config.toml`](../.cargo/config.toml). Every action
future contains its caller's future, so a multiplier in `log::action` scales with the biggest task
any app wraps. A 3x multiplier put `log_processor_rs`' jetstream setup at 18,032 B and failed the
build. Message-handler and web-request futures pay the same multiplier on every request.

## Root cause: coroutine prefix allocation

rustc keeps **parameters and upvars in the coroutine layout prefix — always live, never overlapped
with variant fields**. Only ordinary locals get liveness-based slot reuse. An `async fn` desugars to
a plain fn returning an `async move` block capturing the parameters, so parameters *are* upvars.

Written as an `async fn`, `log::action` therefore held `task` three times:

1. in the outer `async fn`'s prefix, as a parameter;
2. in the inner `CURRENT_ACTION.scope(.., async move { .. })` block's prefix, as an upvar;
3. in `__awaitee`, the `IntoFuture::into_future(task)` temporary held across the yield.

rustc cannot collapse 1 and 2 even though `task` is moved out of the parameter slot immediately.

### Rejected alternatives — do not re-litigate these

| approach | size | why not |
|---|---|---|
| `Box::pin(log::action(..))` at the call site | 8 B at the await | still allocates the full future, on the heap; per-call-site, easy to forget |
| `log::action(.., Box::pin(async { .. }))` | ~200 B | heap alloc + poll indirection per action, and every caller must remember |
| plain `fn` returning `impl Future` around an async block | 2x | removes copy 1 only |
| `pin!` + `as_mut().await` inside an async block | 2x | `pin!` on an upvar just adds a live local |

**The 2x floor is structural**: any `async` block that owns the task stores it as an upvar and again
as the awaitee. Reaching 1x requires no `async fn` and no `async` block in the path — hence a
hand-written `Future`.

## Current implementation

```rust
pin_project! {
    pub struct ActionFuture<F> {
        #[pin]
        inner: TaskLocalFuture<RefCell<Action>, F>,
    }
}

#[inline]
pub fn action<F: Future>(kind: &'static str, ref_ids: Option<Vec<String>>, task: F) -> ActionFuture<F> {
    let now = DateTime::now();
    let id = id_generator::next_id(now.unix_timestamp_millis());
    let action = Action::new(id, kind, ref_ids, now);
    ActionFuture { inner: CURRENT_ACTION.scope(RefCell::new(action), task) }
}

impl<F, R> Future for ActionFuture<F>
where
    F: Future<Output = Result<R, Exception>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let result = ready!(this.inner.as_mut().poll(cx));
        let mut current_action =
            this.inner.take_value().map(RefCell::into_inner).expect("current action must be within the scope");
        if let Err(e) = &result { current_action.log_exception(e); }
        current_action.finish();
        if let Some(sender) = SENDER.get() {
            let _result = sender.send(Message::Action(current_action.into()));
        }
        Poll::Ready(result)
    }
}
```

The whole design hangs on `TaskLocalFuture::take_value(self: Pin<&mut Self>) -> Option<T>` (tokio
1.53.1, exported as `tokio::task::futures::TaskLocalFuture`, *not* `tokio::task::TaskLocalFuture`).
It lets the finish logic recover the `Action` from the future's own slot **after** the scope
resolves, instead of from inside the scope via `CURRENT_ACTION.with(..)` — which is what forced the
extra async layer in the first place.

### The task local is `RefCell<Action>`, not `RefCell<Option<Action>>`

The `Option` only existed so the old in-scope `CURRENT_ACTION.with(|a| a.take())` had a hole to
leave behind. Nothing empties the slot any more, so the action is present for the whole scope by
construction and every accessor stops matching on an impossible case:

```rust
let _result = CURRENT_ACTION.try_with(|action| {
    action.borrow_mut().log(severity, error_code, Some(location), message);
});
```

`try_with` returning `Err` outside an action remains the real "no current action" signal. This saved
zero bytes and was not meant to — `Action` holds a `String`, so `Option<Action>` is niche-optimised
to the same 216 bytes. It is a readability change only.

### The `Result` bound lives on the `Future` impl, deliberately

`action` is bounded only by `F: Future`. Moving `Output = Result<R, Exception>` onto `action` breaks
callers: the old `-> F::Output` normalised through the bound and anchored `R`, but `-> ActionFuture<F>`
does not mention `R`, leaving it a free inference variable resolvable only after the inline async
block is checked — `E0282: type annotations needed` at `web/server.rs` and `task.rs`. On the impl it
is deferred to the `.await`, where `F::Output` is known.

### Dependency

`pin-project-lite`, already in the lockfile via tokio/reqwest/tower. A hand-rolled
`map_unchecked_mut` projection would work (the repo uses `unsafe` in `network.rs`,
`metrics/collector.rs`) but pin-project-lite is the safer default for a single-field structural pin.

## Behaviour and invariants

- **Finish work runs outside the task-local scope.** `log_exception`, `finish` and `sender.send`
  run after `scope_inner` returns. None touch `CURRENT_ACTION`, so behaviour is unchanged — and it
  removes the double-borrow hazard described above `macro_rules! log`.
- **Cancellation is unchanged.** A dropped, incomplete action is still not emitted.
- **`poll` after `Ready` panics** on the `expect`, as any non-fused future would. Every caller
  drives it through `.await`.
- **`RefCell` borrow discipline is unchanged** — accessors hold `borrow_mut()` for their closure
  body exactly as the old `if let Some(..) = action.borrow_mut().as_mut()` did. The warning above
  `macro_rules! log` (never call `log!` from a `Display` impl passed as a log argument) still
  applies.
- **`ActionFuture` is `pub`** to satisfy the workspace `unreachable-pub` lint; reachable as
  `framework::log::ActionFuture`. Its single field is private, so the `pub(crate) Action` does not
  leak.

## Caller-side rule: never wrap `log::action` in an `async fn`

`ActionFuture` removes the 3x inside `log::action`. A caller that is itself an `async fn` whose only
job is to `.await` a `log::action(..)` re-adds 2x on top of it, for exactly the reason above: the
`async fn`'s parameters are prefix upvars, and the inner block holds them (or references to them) a
second time. Such a function must be a plain `fn` returning the future:

```rust
fn handle_message<H, S, M, Fut>(raw: jetstream::Message, handler: H, state: S) -> impl Future<Output = ()>
where ...
{
    let ref_id = header(&raw, REF_ID).map(|id| vec![id.to_owned()]);
    log::action("message", ref_id, async move {
        let _counter = MESSAGE_COUNTER.get().map(Counter::increase);
        ...
    })
    .map(drop)
}
```

`.map(drop)` (`futures::FutureExt`) supplies the `Output = ()` the handler maps require; `Map`'s
only added state is an `Option<F>` over the ZST `drop` fn item.

What is saved is whatever the `async fn` owned by value — the outer prefix held it and the inner
block held it again. Functions owning a raw message win large even at `S = ()`; functions taking
only references save `48 + size_of::<S>()`.

**Three things are not mechanical. Getting any of them wrong changes behaviour:**

1. **`Counter` guards must move inside the async block.** Left in the `fn` body, `_counter` drops
   when the fn *returns* — the instant the future is constructed — and `active_message_handlers`
   reads ~0 forever. Inside the block it is created on first poll, which is when the `async fn`
   created it.
2. **`async` must become `async move`** wherever the inner block borrowed an owned parameter. With
   no enclosing coroutine there is no frame to borrow from. Compute anything needed from a reference
   (e.g. `ref_id` from `&raw`) before the move.
3. **The prologue becomes eager.** Header parsing, the `ref_id` allocation and `log::action`'s
   `DateTime::now()` / `next_id` / `Action::new` now run at *call* time, not on first poll, so the
   action's start stamp — and `elapsed` — begins one scheduling hop earlier. This only matters when
   the future is constructed somewhere other than where it is polled: of the four handlers, only
   nats `handle_message` qualifies (built in the pull loop, polled inside `executor.spawn`). Its
   semaphore permit is acquired *before* construction, so no queue wait leaks into `elapsed` — the
   shift is spawn latency only.

## Where `Box::pin` remains, and why it must

Every surviving `Box::pin` in the consumers erases a generic future at a
`Box<dyn Fn(..) -> Pin<Box<dyn Future<..> + Send>>>` boundary. **None of them is a size workaround.**
Each consumer keeps a `HashMap<&'static str, MessageHandler<S>>` of handlers registered with
different `H`/`M`/`Fut`; the map forces one concrete return type and `Pin<Box<dyn Future>>` is it.

| site | boundary |
|---|---|
| `framework_nats/src/consumer.rs:100` | `MessageHandler<S>` (`consumer.rs:73`), inside `add_handler`'s closure |
| `framework_nats/src/service.rs:79` | request handler map, same shape |
| `framework_kafka/src/consumer.rs:109` | `MessageHandler<S>` (`consumer.rs:50`), inside `add_handler`'s closure |
| `framework_kafka/src/consumer.rs:125` | same, inside `add_bulk_handler`'s closure |

Removing them was tried and does not compile: kafka gives
`error[E0271]: expected closure to return Pin<Box<dyn Future<..>>>, but it returns impl Future`,
nats gives `error[E0308]: expected Pin<Box<dyn Future<Output = ()> + Send>>, found future`. The box
only moves between callee and closure body — one allocation either way. Dropping it entirely would
mean replacing the handler maps with something non-`dyn`, which the per-subject/per-topic generic
handler API rules out.

**Erasure happens at the map, not in the helpers.** `handle_bulk_messages`, `handle_messages`,
`handle_message` all return `impl Future`; the four sites above are the only `Box::pin`s.
`handle_messages` needs `+ use<H, S, M, Fut>` on its RPIT — it takes `&S` and `&Arc<Counter>` (read
synchronously before returning), and edition 2024 would otherwise capture those lifetimes and make
the future non-`'static`, which `Box<dyn Future + Send>` rejects. The others do not need it.

## Measuring

Framework-level, per-`.await`, via a lowered clippy threshold:

```bash
printf 'future-size-threshold = 256\n' > /tmp/diag/clippy.toml
CLIPPY_CONF_DIR=/tmp/diag cargo clippy -p log_processor_rs --config 'build.warnings="warn"' 2>&1 | grep -A2 'large future'
```

For an opaque return type there is nothing to `size_of_val`, so instantiate the fn item through a
probe (delete it afterwards — `build.warnings = "deny"` makes stray helpers fail the build):

```rust
const fn ret_size<A, B, C, F: Future>(_f: &impl Fn(A, B, C) -> F) -> usize { size_of::<F>() }
```

## Current sizes

Framework, `log_processor_rs`, lowered-threshold clippy:

| await | size |
|---|---|
| bare `create_or_update_stream` (unwrapped baseline) | 5,520 B |
| the same wrapped in `log::action` | 6,152 B |
| `init_jetstream(..)` | 6,312 B |
| `init_clickhouse(..)` | 2,208 B |
| `log::action` + clickhouse execute | 1,592 B |

`log::action`'s overhead is now fixed — the 216-byte `Action` plus `TaskLocalFuture` bookkeeping —
rather than a multiplier. (The 632 B between rows 1 and 2 covers that overhead *and* the async
block's own locals.)

Consumer handlers, `ret_size` probe at `H = fn(..) -> BoxFuture`, `M = String`, `S = ()`:

| function | size | at `S = [u8; 256]` |
|---|---|---|
| `framework_nats` `handle_message` | 1,384 B | — |
| `framework_nats` `handle_batch` | 1,008 B | 1,264 B |
| `framework_kafka` `handle_message` | 456 B | 712 B |
| `framework_kafka` `handle_bulk_messages` | 360 B | — |

## Known gap

**No `large_futures` regression guard.** The threshold is only checked at `.await` sites, so a
framework change that reintroduces a copy would surface only in whichever app happens to wrap a big
enough task — not in the framework crate itself.
