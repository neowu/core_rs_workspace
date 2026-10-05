# `#[api]` route: leak the service instead of `Arc`

Status: undecided, `route(Arc<Self>)` kept for now. Leak only is not enough, see "Revision: freeable static
state" at the end, which supersedes "close, don't drop" as the only cleanup path.

## Background

2026-09-29 http benchmark (`report/2026-09-29_http`, c=384, 3 connections, one run each):

| scenario | req/s | server cpu µs/req |
|---|---|---|
| get (3 runs) | 81,991 – 83,292 | 22.80 – 23.33 |
| api_get | 81,366 | 23.47 |
| post | 66,866 | 28.47 |
| api_post | 65,690 | 29.14 |

`api_post` is +0.67 µs/req (+2.3%) over `post`, but the three identical `get` runs already spread
0.53 µs/req, and each `api_*` ran once, right after its plain counterpart. Not resolved from noise yet.

Per request differences between the generated route and a plain controller:

1. **`Arc` refcount per request (real).** axum clones the handler on every request
   (`axum-0.8.9/src/handler/service.rs:172`). A plain fn controller is zero sized, the `#[api]`
   closure captures `svc: Arc<Self>`, so each request does an atomic inc/dec on a cache line shared by
   all workers. Tens of ns.
2. **Bigger boxed future (real, tiny).** The closure future holds `svc`, `fn_name`, the service future
   and a `Result<T, Exception>`, so axum's `Box::pin` allocates and copies more.
3. Not differences: the macro uses `axum::routing::on` (skips framework's `Controller`) but calls
   `context!` inside the closure instead, same work; `fn_name` becomes a `String` in both paths;
   `__into_response`'s match; route path length.

Expected total from 1 + 2 is well under 0.1 µs/req, so most of the measured gap is likely noise.

Next step: A/B per `spec/benchmark/server.md` (alternate post / api_post, several rounds, compare mean
cpu µs/req). If the gap holds, profile `api_post` (only `post` has a profile).

## Proposal: `route(self)` leaks the service once

```rust
fn route(service: Self) -> Router where Self: Sized + Send + Sync + 'static {
    let svc: &'static Self = Box::leak(Box::new(service));
    // handlers capture `svc` and `fn_name`, both Copy, so axum's per request clone is free
}
```

- taking `Self` instead of `Arc<Self>` is the honest contract: the router owns the service for the rest
  of the process, the caller keeps no handle.
- the leak is bounded: one service per `route()` call, normally once at startup.
- no alternative removes the refcount while staying droppable: `with_state` / `State` also clones the
  state per request, only a `'static` ref makes the clone free.

## Concern: leaked state is never dropped

The service's `Drop` never runs, nor anything it owns (e.g. an `Arc<Pool>` keeps one strong count).

Today this changes little:

- demo already leaks `AppState` (`app/demo/src/lib.rs`), services hold `&'static AppState`.
- no lib resource cleans up in `Drop` (only `TaskGuard`, `CounterGuard`, `EventSource`, `Span` impl it),
  `Database` has none.
- relying on the router's drop was fragile anyway: any other `Arc` clone (scheduler, spawned task) keeps
  it alive, and `Drop` cannot await an async graceful close.

What breaks: a service that relies on being dropped as a signal, e.g. owns an `mpsc::Sender` whose
drop tells a background worker to finish. With the leak the channel never closes, shutdown waiting on
that worker hangs.

## Why not an explicit "drop leaked state" api

Freeing a `&'static` needs proof nothing still references it; the compiler cannot check that, so it
must be `unsafe fn`, and `system.wait()` does not give the proof:

- requests are fine: `axum::serve(...).with_graceful_shutdown` returns only after every connection task
  ends (`lib/framework/src/web/server.rs`).
- detached work is not: a `'static` service can be moved into `tokio::spawn` or the task executor, and
  `executor.shutdown(timeout)` (run after `system.wait()`) can give up on running tasks. Dropping then is
  use-after-free.

## Convention if adopted: close, don't drop

Resources expose `close(&self)`; a late caller gets an error, never UB.

```rust
let state: &'static AppState = Box::leak(Box::new(AppState { db }));
let app = app.merge(UserService::route(UserServiceImpl { state }));
system.start_service(|token| http_server.start(app, token));

system.wait().await;                               // requests drained
executor.shutdown(Duration::from_secs(15)).await;  // background tasks drained (or timed out)
state.close().await;                               // app defined, closes every resource
system.shutdown_logger().await;                    // last, so close errors are still logged
```

1. resources live in one leaked `AppState`, services hold `&'static AppState` and nothing needing
   cleanup, so leaking a service leaks a pointer.
2. every framework resource provides `async fn close(&self)`, afterwards refuses new work with an error.
3. the app writes `AppState::close()`, called after `system.wait()` + `executor.shutdown(..)`, before
   `shutdown_logger()`.
4. never rely on `Drop` for shutdown; signal workers with `CancellationToken`, not by dropping a `Sender`.

## Work if adopted

- `lib/framework_macro/src/api.rs`: `route(service: Self)`, leak once, handlers capture `svc: &'static
  Self`; update expansion tests and the `lib.rs` doc example.
- callers: `benchmark/http_test_server`, `app/demo/src/user/web.rs`, `test/http_test/tests/api_test.rs`
  (`route(Arc::new(x))` → `route(x)`).
- `ResourcePool::close` (`lib/framework/src/pool.rs`): `semaphore.close()` so checkouts fail with "pool
  closed", drain `storage`, discard (not return) resources checked in after close. `Database::close`
  delegates; same for other connection holding clients (clickhouse, kafka, nats) as needed.
- demo: `AppState::close()`, wired into the shutdown order above.
- consider the same for `#[nats_api]` `service(nats_client, Arc<Self>)`.
- spec: `spec/http_server_api.md` (route contract), a shutdown order / close-don't-drop section.
- verify with the A/B benchmark that the gain is real before paying the api change.

## Revision: freeable static state (2026-10-02)

### Why avoid `Arc<State>` per request

Under high concurrency every worker does inc/dec on the same cache line. `Arc::clone` is lock free
(`fetch_add`), it never blocks or parks a task; the cost is the cache line moving between cores, paid as
CPU stall on the worker, and it grows with core count and request rate. An io bound app has spare CPU,
so it hides until the cores saturate: a scalability ceiling, not latency from waiting.

On axum's routing path (0.8.9) we checked, `#[api]`'s `Arc<Self>` is the only shared counter touched per
request: `Router::call_with_state` borrows `inner`, `Route::call` clones the boxed handler service
(handler + state). Per connection axum clones the router and the graceful shutdown channels.

Goal: handlers reach state through a `Copy` handle, no per request refcount.

### Requirement: `Drop` based cleanup must still run

Leak only cannot be the only option given to framework users. The framework cannot wrap every third
party client, and some clean up only in `Drop` (redis pool, message queue client: close connections, stop
background tasks, flush). In house example: `framework_db` spawns the postgres connection task, which ends
only when the client is dropped (`lib/framework_db/src/connection.rs`).

So state must be freeable, and freed while the runtime is alive: such drops often need it (spawn, reactor),
and their background tasks need it to finish.

No safe building block exists in Rust:

- statics never drop.
- `std::thread::scope` frees borrowed data after joining, but is sync only.
- a scoped async spawn cannot be safe (scoped task trilemma: a future can be `mem::forget`ed, so the
  scope cannot guarantee it waits for children).
- `Arc` is the safe one, with the refcount.
- Rust 1.99 docs: do not "unleak" (`Box::from_raw` on a `Box::leak` ref is a grey area, many forms are
  UB); memory meant to be freed starts from `Box::into_non_null` / `Box::into_raw`.

### Design: `Owner<T>` + `Shared<T>` (framework owned, unsafe inside)

```rust
pub struct Owner<T>(NonNull<T>);   // unique, not Copy; dropping or forgetting it leaks (safe default)
pub struct Shared<T>(NonNull<T>);  // Copy handle given to services / handlers

impl<T> Clone for Shared<T> { fn clone(&self) -> Self { *self } }
impl<T> Copy for Shared<T> {}
unsafe impl<T: Sync> Send for Shared<T> {}        // behaves like &T
unsafe impl<T: Sync> Sync for Shared<T> {}
unsafe impl<T: Send + Sync> Send for Owner<T> {}  // free() drops T on the caller's thread

impl<T> Deref for Shared<T> {
    type Target = T;
    fn deref(&self) -> &T { unsafe { self.0.as_ref() } }  // borrow tied to the handle, never &'static
}

impl<T> Owner<T> {
    pub fn new(value: T) -> Self { Owner(Box::into_non_null(Box::new(value))) }
    pub fn handle(&self) -> Shared<T> { Shared(self.0) }
    /// # Safety
    /// no `Shared<T>`, nor a reference derived from one, is used after this call
    pub unsafe fn free(self) { drop(unsafe { Box::from_non_null(self.0) }) }
}
```

- `NonNull` is `!Send + !Sync` like raw pointers (no ownership meaning); the wrapper states it.
- a handler future owns the `Copy` handle and borrows through it, so it is still `'static` without
  fabricating a `&'static T`.
- `#[api]` shape: services become `Copy` structs holding `Shared<AppState>`, `route(service: Self) where
  Self: Copy + Send + Sync + 'static`. Nothing per service is heap allocated, so nothing leaks; only
  `AppState` is owned, by an `Owner`. Per service state moves into `AppState`.

### Free timing

`free` is sound only when no task can still use a `Shared`. The framework knows that after draining:

```rust
let state = Owner::new(AppState { db });
// ... routes / consumers / jobs get state.handle()
system.wait().await;                                         // http drained, consumers / scheduler stopped
let drained = executor.shutdown(timeout).await.is_none();    // None = every tracked task finished
if drained { unsafe { state.free() } }                       // third party Drop runs inside the runtime
else { /* warn, leak: Drop skipped beats use after free */ }
system.shutdown_logger().await;                              // last, so Drop errors are still logged
```

Contract (why `free` stays `unsafe`): every holder of a `Shared` runs in a framework tracked task (http
handler, executor, scheduler, consumer). A raw `tokio::spawn` / `std::thread::spawn` that captures one breaks
it, and the framework cannot enforce that (`Shared` is `Copy + Send`). Raw spawns today hold no app state:
`system.rs` daemons and signal listener, the db connection task, kafka handlers (joined before the consumer
returns).

`close(&self)` stays useful for async graceful close (`Drop` cannot await); `free` covers what only `Drop`
cleans.

### Timeout fallback, considered

- leak (chosen): a timeout already means unclean shutdown; skip `Drop`, log abandoned tasks.
- drop the runtime first, then free: abandoned tasks are dropped with it, so free is sound, but third party
  `Drop` then runs without a runtime (spawn panics, reactor gone, nothing flushes).
- sharded / per core refcount (Linux `percpu_ref`): automatic "last user gone" with no shared cache line on
  the hot path, but reading a consistent sum needs a mode switch at shutdown (RCU like). No std building
  block, complex. Not now.

### Work if adopted (replaces the leak only list above)

- `lib/framework`: `Owner` / `Shared`; `TaskExecutor` wrapper `shutdown` returns whether it drained (inner
  `TaskExecutor::shutdown` already returns `Option`).
- `lib/framework_macro/src/api.rs`: `route(self)` for `Copy` services; expansion tests, `lib.rs` doc example.
- callers: `benchmark/http_test_server`, `app/demo/src/user/web.rs`, `test/http_test/tests/api_test.rs`.
- demo: `AppState` from `Box::leak` to `Owner`, free in the shutdown order above.
- consider the same for `#[nats_api]` `service(nats_client, Arc<Self>)`.
- spec: `spec/http_server_api.md` (route contract), shutdown order + the `Shared` holder contract.
- verify with the A/B benchmark first; the gain must be real before paying the api change.
