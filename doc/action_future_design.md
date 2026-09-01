# `ActionFuture` design

Status: implemented (2026-09-01) · Code: [`lib/framework/src/log.rs`](../../lib/framework/src/log.rs)

## Summary

`log::action` wraps a caller's future so that an `Action` (id, kind, context, stats, log buffer) is
available as a task local for its duration and is emitted to the appender when it finishes.

It used to be an `async fn`. That construction stored the caller's future **three times over**,
making every action future ~3x the size of the work it wrapped. For `log_processor_rs`' jetstream
setup this produced an 18 KB future and a `clippy::large_futures` failure. `log::action` is now a
plain `fn` returning a hand-written `ActionFuture<F>` that stores the task **exactly once**, with no
allocation and no extra indirection.

## Problem

`cargo clippy -p log_processor_rs` failed (`build.warnings = "deny"` in `.cargo/config.toml`):

```
warning: large future with a size of 18032 bytes
   --> app/log_processor_rs/src/main.rs:103:5
```

The default `future-size-threshold` is 16384.

Sizes were measured by re-running clippy with the threshold lowered so every `.await` reports:

```bash
printf 'future-size-threshold = 256\n' > /tmp/diag/clippy.toml
CLIPPY_CONF_DIR=/tmp/diag cargo clippy -p log_processor_rs --config 'build.warnings="warn"' -- --cfg diag 2>&1 | grep -A2 'large future'
```

| await | size |
|---|---|
| `jetstream.create_or_update_stream(config)` alone | 5,520 B |
| the same, wrapped in `log::action(...)` | **18,032 B** |
| `init_jetstream(client.clone())` | 18,192 B |
| clickhouse `execute` awaits | 1,280 B each |
| those, wrapped in `log::action(...)` | 4,352 B |

Both wrapped/unwrapped pairs land on the same ratio (≈5.9K→18.0K, ≈1.4K→4.35K): **`log::action`
cost ~3x the size of the future handed to it.** async-nats' `create_or_update_stream` was simply
the one big enough that 3x crossed the threshold. Nothing about the call site caused this — the
warning was present in the committed code.

## Root cause: three copies of `F`

The old implementation:

```rust
#[inline]
pub async fn action<F, R>(kind, ref_ids, task: F) -> F::Output   // (1) task is a parameter
where
    F: Future<Output = Result<R, Exception>>,
{
    let action = Action::new(...);
    CURRENT_ACTION
        .scope(RefCell::new(Some(action)), async move {           // (2) task captured as an upvar
            let result = task.await;                              // (3) task moved into __awaitee

            let mut current_action = CURRENT_ACTION
                .with(|current_action| current_action.take().expect("..."));
            if let Err(e) = &result { current_action.log_exception(e); }
            current_action.finish();
            if let Some(sender) = SENDER.get() {
                let _result = sender.send(Message::Action(current_action.into()));
            }
            result
        })
        .await
}
```

rustc's coroutine layout keeps **upvars and parameters in the layout prefix — always live, never
overlapped with variant fields**. Only ordinary locals get liveness-based slot reuse. So:

1. `task` sits in the outer `async fn`'s prefix. An `async fn` desugars to a plain fn returning an
   `async move` block that captures the parameters, so parameters *are* upvars.
2. `task` sits again in the inner `async move` block's prefix.
3. `task` is moved a third time into `__awaitee`, the `IntoFuture::into_future(task)` temporary,
   which is a variant field of the inner block because it is live across the yield.

rustc cannot collapse 1 and 2 even though `task` is moved out of the parameter slot immediately.

## Options considered

| option | size | cost | verdict |
|---|---|---|---|
| `Box::pin(log::action(...))` at the call site | 8 B at the await | still allocates the full 18 KB, on the heap | rejected — treats the symptom; the existing repo idiom at `framework_kafka/src/consumer.rs:217` |
| `log::action(..., Box::pin(async { ... }))` — box the inner block | ~200 B | one heap alloc + a pointer indirection per poll, per action, at every large call site | rejected — every caller must remember |
| plain `fn` returning `impl Future` (drops copy 1 only) | **2x** | none | rejected — leaves half the problem |
| `pin!` + `as_mut().await` inside an async block | **2x** | none | rejected — `pin!` on an upvar just adds a live local; async blocks that own the task always pay upvar + awaitee |
| **hand-written future wrapping `TaskLocalFuture`** | **1x** | none | **chosen** |

The 2x floor is structural: *any* async block that owns the task stores it as an upvar and again as
the awaitee. Reaching 1x requires no `async fn` and no `async` block in the path.

### Why `Box::pin` works at all

`Box::pin(async { ... })` produces `Pin<Box<Coroutine>>` — a single 8-byte pointer that implements
`Future`. Substituting it for `F` makes all three copies 8 bytes each and moves the real coroutine
to the heap, where it is never moved or copied. It is a correct fix, just a per-call-site one that
buys a heap allocation and a poll indirection to work around a layout problem in the framework.

## Chosen design

Store `F` once, inside tokio's `TaskLocalFuture`, and run the finish logic in a hand-written `poll`.

This is possible because tokio exposes
`TaskLocalFuture::take_value(self: Pin<&mut Self>) -> Option<T>`, so the `Action` can be recovered
from the future's slot *after* the scoped future resolves, instead of from inside the scope via
`CURRENT_ACTION.with(...)`.

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

### The task-local type: `RefCell<Action>`

The task local is `RefCell<Action>`, not `RefCell<Option<Action>>`.

The `Option` existed because the old implementation extracted the action from *inside* the scope
with `CURRENT_ACTION.with(|a| a.take())`, which needs somewhere to leave a hole. `ActionFuture`
takes the action out of the `TaskLocalFuture`'s own slot after the scope has ended, so nothing ever
needs to empty it: the action is present for the entire scope, by construction.

Removing the layer means every accessor stops pattern-matching on a case that could not happen:

```rust
// before
let _result = CURRENT_ACTION.try_with(|action| {
    if let Some(action) = action.borrow_mut().as_mut() {
        action.log(severity, error_code, Some(location), message);
    }
});

// after
let _result = CURRENT_ACTION.try_with(|action| {
    action.borrow_mut().log(severity, error_code, Some(location), message);
});
```

`try_with` still returns `Err` outside an action, which remains the real "no current action" signal —
that behaviour is unchanged.

**This saved zero bytes**, and was not expected to. `Action` contains a `String`, so `Option<Action>`
is niche-optimised into the same size as `Action` — measured directly:
`size_of::<Action>() == size_of::<Option<Action>>() == 216`. The change is a readability one; every
future size in the table below is byte-for-byte identical before and after it.

### Where the `Result` bound lives

`action` itself is bounded only by `F: Future`; the `Output = Result<R, Exception>` requirement sits
on the `Future` impl. This is load-bearing, not stylistic.

The old signature returned `-> F::Output`, which normalises through the bound and anchored `R` at
the call site. `-> ActionFuture<F>` does not mention `R`, so keeping the bound on `action` left `R`
as a free inference variable resolvable only after the inline async block was type-checked. Callers
failed with `E0282: type annotations needed` (`web/server.rs:104`, `task.rs:62`). Moving the bound
to the impl defers it to the `.await`, where `F::Output` is known.

### Dependency

`pin-project-lite` added to `lib/framework/Cargo.toml`. It was already in the lockfile as a
transitive dependency of tokio/reqwest/tower, so it costs no additional compile time. A hand-rolled
`map_unchecked_mut` projection would also work — the repo uses `unsafe` elsewhere
(`network.rs:6`, `metrics/collector.rs:208`) — but pin-project-lite is the safer default for a
single-field structural pin.

## Invariants and behaviour

- **Finish work now runs outside the task-local scope.** `log_exception`, `finish` and
  `sender.send` run after `scope_inner` returns rather than inside it. None of them touch
  `CURRENT_ACTION`, so behaviour is unchanged — and it removes the double-borrow hazard described
  in the comment above `macro_rules! log`.
- **Cancellation is unchanged.** A dropped, incomplete action is still not emitted.
- **`poll` after `Ready` panics** on the `expect`, as it would for any non-fused future. Every
  caller drives it through `.await`, which never re-polls after `Ready`.
- **The `RefCell` borrow discipline is unchanged.** Accessors hold `borrow_mut()` for the body of
  their closure exactly as the old `if let Some(action) = action.borrow_mut().as_mut()` did
  (the scrutinee temporary lived for the whole `if let`). The warning above `macro_rules! log` —
  never call `log!` from a `Display` impl passed as a log argument — still applies.
- **All call sites are source-compatible.** `log::action(...).await` and
  `Box::pin(log::action(...))` both still compile: `framework_nats/src/service.rs:150`,
  `framework_nats/src/consumer.rs:200,348`, `framework/src/task.rs:62`,
  `framework/src/web/server.rs:104`, `framework_kafka/src/consumer.rs:217,324`,
  `framework_macro/src/integration_test.rs:44`.
- **`ActionFuture` is `pub`** to satisfy the workspace `unreachable-pub` lint; it is reachable as
  `framework::log::ActionFuture` via `pub mod log`. Its single field is private, so the
  `pub(crate) Action` type does not leak.

## Results

Measured with the same lowered-threshold diagnostic:

| await | before | after |
|---|---|---|
| `log::action` + jetstream (`main.rs:103`) | 18,032 B | **6,152 B** |
| `init_jetstream(...)` (`main.rs:59`) | 18,192 B | 6,312 B |
| `init_clickhouse(...)` (`main.rs:62`) | 4,968 B | 2,208 B |
| `log::action` + clickhouse (`main.rs:122`) | 4,352 B | 1,592 B |
| bare `create_or_update_stream` (`main.rs:113`) | 5,520 B | 5,520 B (unchanged, as expected) |

`log::action`'s wrapper is now a fixed overhead — the 216-byte `Action` plus `TaskLocalFuture`
bookkeeping — rather than a multiplier. (The 632-byte gap between rows 1 and 5 covers that overhead
*and* the async block's own locals, the `Config` and jetstream handle.) No `Box::pin` was needed at
any call site. The change also shrinks the kafka consumer's existing
`Box::pin(log::action(...))` allocation by 3x, and every web-request and message-handler action.

### Verification

- `cargo clippy --workspace --all-targets` — clean.
- `cargo test` across all library crates — 102 tests pass.
- `test/clickhouse_test` fails on `Connection refused`; `dev.internal:8123` and `:4222` were not
  running. Its output nevertheless exercises the new `poll` path end to end: `log_exception`
  recorded the error, `finish()` stamped `elapsed=1.010931459s`, and the `ACTION:` line reached the
  appender.
- Not verified: running `log_processor_rs` against live nats + clickhouse.

## Refactor history

| date | commit | change |
|---|---|---|
| 2026-08-28 | `9de4923` | redesign framework, check in milestone |
| 2026-08-28 | `95a3de7` | checkin before appender refactor |
| 2026-08-31 | `402213c` | redesign logger |
| 2026-08-31 | `116bab1` | add nats log appender, rename clickhouse action / event result column |
| 2026-09-01 | `d761d28` | redesign action log to improve performance — `Action` gains the single `logs: String` buffer, pre-sized `context`/`stats` vecs |
| 2026-09-01 | `5643834` | action: log error regardless max bytes |
| 2026-09-01 | *(this change)* | `log::action` → `ActionFuture`; 3x → 1x future size |
| 2026-09-01 | *(follow-up)* | task local `RefCell<Option<Action>>` → `RefCell<Action>` |

### This change, step by step

1. Confirmed the warning was pre-existing, not introduced by moving `init_jetstream`'s body around.
2. Lowered `future-size-threshold` to 256 via `CLIPPY_CONF_DIR` to get a per-await size table;
   derived the 3x ratio from two independent call sites.
3. Attributed the 3x to prefix-allocated coroutine upvars/parameters and enumerated the options
   above; established that async-block designs floor at 2x.
4. Confirmed `TaskLocalFuture::take_value` exists in the vendored tokio 1.53.1
   (`registry/src/*/tokio-1.53.1/src/task/task_local.rs:383`) and that the export path is
   `tokio::task::futures::TaskLocalFuture`, not `tokio::task::TaskLocalFuture`.
5. Implemented `ActionFuture`; hit `E0282` at two call sites and moved the `Result` bound to the
   `Future` impl.
6. Fixed a separate pre-existing bug found alongside: `app/log_processor_rs/src/main.rs:59`
   discarded the `Result` from `init_jetstream`, silently swallowing a jetstream stream-creation
   failure at startup. Now `init_jetstream(client.clone()).await?`.
7. Re-measured, re-ran clippy and tests.

### Follow-up: collapsing the `Option`

8. Collapsed the task local to `RefCell<Action>` across its nine accessors — six in `log.rs`
   (`current_action_id`, `trace`, `__log`, `__log_exception`, `__context`, `__stats`) and three in
   `log/span.rs` (`__span`, `Span::clear`, `Span::drop`) — plus the declaration, the `scope` call
   and `ActionFuture::poll`.
9. Reworked the `span.rs` test helper, which was the only remaining caller that emptied the slot.
   It now mirrors `ActionFuture` — `pin!` the scope, `block_on` it, then `take_value()`:

   ```rust
   fn with_action<F: FnOnce()>(task: F) -> Action {
       let mut scope = pin!(CURRENT_ACTION.scope(
           RefCell::new(Action::new("id".to_owned(), "test", None, DateTime::now())),
           async { task() },
       ));
       block_on(scope.as_mut());
       scope.take_value().map(RefCell::into_inner).expect("action must be in scope")
   }
   ```

10. Re-measured: future sizes byte-for-byte identical, as predicted by the niche optimisation.
    Clippy clean, 102 tests pass.

## Follow-ups

- **`framework_kafka/src/consumer.rs:217`** can likely drop its `Box::pin` now that the wrapped
  future is 3x smaller — worth re-measuring.
- **A `large_futures` regression guard.** The threshold is only checked at `.await` sites, so a
  future framework change that reintroduces a copy would only surface in whichever app happens to
  wrap a big enough task.
