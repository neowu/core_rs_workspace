# Rust & Framework Training — Outline

Audience: engineers coming from **Java** and/or **TypeScript** who will build and maintain services
on this workspace's framework (`lib/framework*`, apps under `app/`).

Goal of the program: by the end, every participant can independently add a feature to an existing
app in this repo — a new HTTP endpoint, a NATS handler, a scheduled job, a table + repository call —
with correct error handling, action logging, validation and tests, and get it through
`cargo clippy -- -D warnings`.

Non-goal: making everyone a Rust language expert. We teach the subset of Rust this framework
actually uses, and we teach it against real code in this repo rather than toy examples.

---

## 1. Format & logistics

| | |
|---|---|
| Sessions | 16 (0–15), plus a capstone |
| Length | 90 min each (Session 0 is 60 min) |
| Cadence | 2 per week → ~8 weeks |
| Shape | 30–40 min walkthrough of real code, 40–50 min hands-on lab, 10 min review |
| Prework | ~30–45 min reading per session, listed under **Read before** |
| Homework | Each lab has a "stretch" item; carried into the next session's review |
| Group size | ≤ 6 per instructor; pair programming during labs |

**Everything is done in this repo.** Labs land in a scratch crate `app/demo` (examples dir) or in a
personal branch. Nothing is merged to `main` during training except the capstone, by review.

**Reference material we use instead of writing our own:**
- *The Rust Programming Language* (the book) — chapters cited per session
- *Rust by Example* — for quick syntax lookups
- Tokio tutorial — for Sessions 6–8
- This repo: `doc/action_future_design.md`, `doc/config_design.md`, and the doc comments on
  `framework_macro`'s proc macros, which are the framework's own design write-ups

---

## 2. Phase map

| Phase | Sessions | Theme | Outcome |
|---|---|---|---|
| 0 | 0 | Toolchain & repo tour | Everyone can build, test, lint, run `demo` |
| 1 | 1–5 | Rust core for JVM/TS developers | Can read any file in `lib/framework` |
| 2 | 6–8 | Async Rust & Tokio | Understands why the framework looks the way it does |
| 3 | 9–14 | The framework itself | Can build a service end to end |
| 4 | 15 | Quality, testing, ops | Can ship it |
| — | Capstone | Build a small service | Reviewed and merged |

Phase 1 is deliberately front-loaded: the framework's API surface (`Exception`, `Field<E, V>`,
`ActionFuture<F>`, `#[api]` traits) is unreadable without traits, generics and ownership. Do not
skip ahead to Phase 3 for a team that has never written Rust.

---

## Phase 0 — Setup

### Session 0 — Toolchain, workspace, first build (60 min)

**Goal:** everyone has a green `cargo clippy` and can run `demo` locally.

**Content**
- Install: `rustup`, stable toolchain, `rustfmt` (note: `rustfmt.toml` sets
  `unstable_features = true`, so formatting needs the nightly `rustfmt`), `rust-analyzer` in the IDE.
- macOS native deps: `brew install pkgconf librdkafka` (see `README.md`) — Kafka binds to a C library,
  which is worth pointing out early to a Java/TS audience used to pure-managed dependency trees.
- Cargo vs Maven/Gradle vs npm: `Cargo.toml`, `Cargo.lock`, workspaces, `[workspace.dependencies]`,
  `path` dependencies, features, `dev-dependencies`, profiles (`release`, `profiling`).
- Repo layout per `CLAUDE.md`: `lib/` libraries, `app/` binaries, `test/` e2e tests that need
  external services running (see `docker/*/docker-compose.yml`).
- The commands we live by:

```bash
cargo build && cargo test && cargo clippy -- -D warnings && cargo +nightly fmt
```

- Why `.cargo/config.toml` sets `warnings = "deny"`, and what that means day to day.
- Run the demo app: `cargo run -p demo` (needs Postgres + NATS from `docker/`), and the standalone
  examples: `cargo run -p demo --example action_log`.

**Read before:** `README.md`, `CLAUDE.md`, root `Cargo.toml`.

**Lab:** clone, build, run `--example action_log`, read the console output out loud as a group and
guess what each line means. Deliberately break something (delete a `?`) and read the compiler error.

**Done when:** `cargo clippy -- -D warnings` is green on a fresh checkout and `--example action_log`
prints an action.

---

## Phase 1 — Rust core for Java/TypeScript developers

Every session in this phase follows the same rhythm: concept → the equivalent you already know →
where it shows up in `lib/framework`.

### Session 1 — Ownership, moves, borrows, lifetimes

**Detailed guide: [`01-session-1.md`](01-session-1.md)**

**Goal:** read and predict compiler errors about ownership without guessing.

**Rust concepts:** stack vs heap; move semantics; `Copy` vs `Clone`; `&T` / `&mut T`; the borrow
rules (many readers xor one writer); `Drop`; lifetimes as a *description* of existing scope, not a
GC knob; `'static`.

**Coming from Java/TS:** everything you have is a reference to a GC'd object; here, a variable owns
its value and assignment moves it. `String` vs `&str` is roughly "owned buffer" vs "borrowed view" —
Java's `String` is neither. There is no `null`; there is `Option`.

**Read in this repo:**
- `lib/framework/src/string.rs` — `truncate_to_max(&self) -> &str` returns a borrow of the input,
  which is the whole point of `&str`; also `intern()` and `Box::leak` producing a `&'static str`.
- `lib/framework/src/log/span.rs` — `Span` does its real work in `Drop`. This is RAII, the pattern
  Java tries to get with try-with-resources.
- `app/demo/src/lib.rs` — `let state: &'static AppState = Box::leak(Box::new(AppState { db }));`
  Discuss why an app-wide singleton is leaked on purpose (and see `TODO.md`).

**Lab:** write a function that takes `&str`, returns `&str`, and make it compile; then make it
return `String` and explain the difference in allocations. Fix five prepared borrow-checker errors.

**Pitfalls to call out:** reaching for `.clone()` to silence the borrow checker; `String` where
`&str` would do (clippy's `str_to_string`, `implicit_clone`, `inefficient_to_string` are all denied
in this workspace).

**Book:** ch. 4, 10.3.

### Session 2 — Structs, enums, pattern matching, `Option`

**Goal:** model data the Rust way; stop reaching for `null` and inheritance.

**Rust concepts:** structs, tuple structs, `impl` blocks, associated functions vs methods; enums
with data; `match` and exhaustiveness; `if let`, `let else`, let-chains; `Option<T>` and its
combinators (`map`, `and_then`, `unwrap_or`, `is_some_and`, `map_or`); `Default`.

**Coming from Java/TS:** an enum with data is a sealed interface / discriminated union, and `match`
is `switch` that the compiler proves is exhaustive. There is no class inheritance at all.

**Read in this repo:**
- `lib/framework/src/log.rs` — `enum Severity` with explicit discriminants and `serde` renames.
- `lib/framework_db/src/field.rs` — `enum CondInner { Eq, In, NotNull }` and how `build_conditions`
  matches on it to build SQL.
- `lib/framework/src/config.rs` — `EnvString`, a newtype over `String` with a custom `Deserialize`.
- `lib/framework/src/schedule/trigger.rs` — `Trigger::FixedRate | Trigger::Daily`.

**Lab:** add a `Cond::Gt` variant to a copy of `field.rs` and make `build_conditions` handle it;
observe that the compiler lists every place you must update. Then add an `Option<i32>` field to a
struct and thread it through with combinators instead of `if x != null`.

**Pitfalls:** `unwrap()` — banned outside tests by `clippy::unwrap_used`; wildcard match arms —
`wildcard_enum_match_arm` is denied, so exhaustive matching is enforced.

**Book:** ch. 5, 6, 18.

### Session 3 — Traits and generics

**Goal:** read the framework's signatures. This is the highest-leverage session of Phase 1.

**Rust concepts:** traits vs Java interfaces; `impl Trait` in argument and return position; generic
bounds and `where` clauses; associated types; blanket impls; trait objects (`dyn Trait`, `Box<dyn
Trait>`) and object safety; orphan rule; `From`/`Into`; `Deref`; `Display`/`Debug`; marker traits;
`PhantomData`.

**Coming from Java/TS:** generics are monomorphized, not erased — `Vec<i32>` really is a distinct
type at runtime. Traits can be implemented for types you did not define (the extension-method
problem TS solves with declaration merging). `dyn Trait` is the closest thing to a Java interface
reference, and it costs a vtable.

**Read in this repo:**
- `lib/framework/src/exception.rs` — the blanket `impl<T: Error + 'static> From<T> for Exception`.
  This is the single most important impl in the codebase: it is what makes `?` work everywhere.
- `lib/framework/src/pool.rs` — `trait ResourceManager` with an associated type `Target`, and
  `ResourcePool<R: ResourceManager>`.
- `lib/framework_db/src/field.rs` — `Field<E, V>` and `PhantomData<(E, V)>`: a zero-sized type
  parameter that makes `User::NAME.eq(...)` type-check only against `User`.
- `lib/framework/src/appender.rs` — `trait Appender` with `-> impl Future<Output = ()> + Send`.
- `lib/framework_clickhouse/src/lib.rs` — the comment explaining why params are `&[&dyn QueryParam]`
  and not the sealed, non-object-safe upstream trait. A concrete lesson in when `dyn` is available.

**Lab:** implement `ResourceManager` for a fake resource and drive it through `ResourcePool`;
implement `Display` for a small type and see `to_string()` appear for free.

**Pitfalls:** trying to "extend" a struct; reaching for `dyn` when a generic would do; forgetting
`Send + 'static` on things that cross a task boundary (previewed here, hammered in Session 7).

**Book:** ch. 10, 17.2, 19.

### Session 4 — Collections, strings, iterators, closures

**Goal:** write idiomatic data-shuffling code the lints will accept.

**Rust concepts:** `Vec`, `HashMap`, `HashSet`, `VecDeque`; `String` vs `&str` vs `Cow`; iterator
adapters and laziness; `collect` into `Result<Vec<_>, _>`; `Fn`/`FnMut`/`FnOnce`; `move` closures;
`format!` and `write!`; slices and indexing.

**Coming from Java/TS:** iterators are Java streams / TS array methods, but zero-cost and lazy by
default. `collect::<Result<Vec<_>, _>>()` is the trick that replaces a try/catch inside a `.map()`.

**Read in this repo:**
- `lib/framework_db/src/repository.rs` — `rows.into_iter().map(T::try_from).collect::<Result<Vec<_>, _>>()`.
- `lib/framework/src/log/action.rs` — `add_stat` linear-scans a `Vec<(&'static str, u64)>` instead of
  using a `HashMap`, with the reasoning in a comment. Good discussion of "know your N".
- `lib/framework/src/string.rs` — `write_str!` and why the framework appends into one `String` buffer
  rather than building intermediate strings.

**Lab:** rewrite an imperative loop from one of the apps as an iterator chain; then measure and
discuss whether it is actually clearer.

**Pitfalls:** `format_collect`, `format_push_string`, `needless_for_each`, `explicit_iter_loop`,
`indexing_slicing` — all denied here; the lab is partly about learning what the lint list wants.

**Book:** ch. 8, 13.

### Session 5 — Error handling, modules, crates

**Goal:** use `Result` and this framework's `Exception` correctly.

**Rust concepts:** `Result<T, E>`; the `?` operator and the `From` conversion it performs;
`std::error::Error` and `source()` chains; panic vs recoverable error; modules, `pub`,
`pub(crate)`, `use`, re-exports; how a crate's public surface is designed.

**Framework specifics — this is where Phase 1 starts paying off:**
- `lib/framework/src/exception.rs` — `Exception { severity, code, message, location, source }`, the
  `exception!` macro capturing `file!()`/`line!()`, and `backtrace()` walking the `source` chain.
- `lib/framework/src/exception/error_code.rs` — `VALIDATION_ERROR`, `BAD_REQUEST`, `NOT_FOUND`,
  `FORBIDDEN`, and how `lib/framework/src/web/error.rs` maps them to HTTP status codes.
- Convention: **fail fast at startup with `panic!`/`expect`** (config, asset paths, binding a port),
  **return `Exception` at request time**. `lib/framework/src/config.rs` states this explicitly.
- `app/demo/examples/error.rs` as a runnable exhibit.

**Coming from Java/TS:** `Exception` here is a value, not a control-flow mechanism; there is no
stack unwinding to a `catch` block far away, and no checked-exception ceremony — `?` is the
propagation. The `location` field plus `backtrace()` is what replaces a JVM stack trace, and it only
records the frames that actually used `?`-style construction.

**Lab:** take a function that returns `Result<_, Box<dyn Error>>` and convert it to
`Result<_, Exception>` with `?`; add an `exception!` with a `code` and see it become a 400 in the
web layer. Deliberately produce a nested `source` chain and read `backtrace()`.

**Pitfalls:** `unwrap_used`, `todo`, `unimplemented`, `unreachable`, `try_err`, `map_err_ignore` are
all denied; `unwrap_in_result` too. Panicking in a handler kills the request, not the process, but
we still don't do it.

**Book:** ch. 7, 9.

**Checkpoint (end of Phase 1):** a 30-minute written/pairing exercise — read
`lib/framework/src/pool.rs` cold and explain, function by function, what it does and why the types
are what they are.

---

## Phase 2 — Async Rust and Tokio

### Session 6 — Futures, `async`/`await`, the Tokio runtime

**Goal:** understand that a future is an inert state machine that someone must poll.

**Rust concepts:** `Future` trait, `poll`, `Poll::Pending`/`Ready`; `async fn` desugaring; futures
are lazy (nothing runs until awaited — unlike a JS `Promise`, which is already running); `#[tokio::main]`;
`tokio::spawn` and `JoinHandle`; `select!`, `join_all`, `JoinSet`; `Pin` and why it exists; `pin_project`.

**Coming from Java/TS:** closest to Project Loom / `CompletableFuture` and to JS promises, but with
two differences worth 20 minutes of discussion: (1) laziness, (2) you can be dropped mid-flight —
cancellation is real and happens at await points.

**Read in this repo:**
- `app/demo/src/main.rs` + `app/demo/src/lib.rs` — the smallest complete async app.
- `lib/framework/src/log.rs` — `ActionFuture<F>`, a **hand-written** `Future` impl.
- `doc/action_future_design.md` — read this in full. It explains coroutine layout, why an `async fn`
  wrapper stored the task three times, and why `clippy::large_futures` failed the build at 18 KB.
  This is the single best document in the repo for teaching how async Rust actually compiles.

**Lab:** write two async functions, call them sequentially and then with `join!`; measure. Then use
`tokio::select!` with a timeout. Then add `#[allow]`-free code that trips `large_futures` and fix it.

**Pitfalls:** blocking calls inside async (no `std::thread::sleep`, no blocking file I/O on the
runtime); `.await` inside a lock guard's scope; assuming an async block starts on creation.

### Session 7 — Shared state: `Send`, `Sync`, `Arc`, `Mutex`, `&'static`

**Goal:** know which of the four state-sharing patterns this codebase uses, and when.

**Rust concepts:** `Send`/`Sync` as auto traits and what the compiler is really proving; `Arc<T>`;
`Mutex`/`RwLock` (std vs tokio — and why std's is usually right for short critical sections);
`OnceLock`, `LazyLock`; atomics; interior mutability (`Cell`, `RefCell`) and why `RefCell` is
allowed in a task-local.

**Read in this repo — the four patterns, side by side:**
1. `&'static` via `Box::leak` — `app/demo/src/lib.rs` (`AppState`), the default for app singletons.
2. `Arc<T>` — `app/log_processor_rs/src/main.rs` (`Arc<AppState>` cloned into two consumers).
3. `OnceLock` — `lib/framework/src/system.rs` (`CONTEXT`, `SENDER`), `lib/framework/src/task.rs`
   (`EXECUTOR`), `lib/framework/src/string.rs` (`LazyLock<Mutex<HashSet<&'static str>>>`).
4. `Arc<Mutex<..>>` — `lib/framework/src/task.rs` (`TaskExecutor::tasks`),
   `lib/framework/src/pool.rs` (`Mutex<VecDeque<Resource<..>>>`).

Also: `lib/framework/src/metrics/counter.rs` and the `CounterGuard` pattern
(`let _counter = counter.increase();`) — RAII again, this time for metrics.

**Lab:** take a struct shared between two tasks and make it compile three ways (`Arc`, `Arc<Mutex>`,
`Box::leak`); explain the trade-off of each in one sentence.

**Pitfalls:** holding a `std::sync::MutexGuard` across `.await` (not `Send`, won't compile — good);
`clone_on_ref_ptr` is denied, so write `Arc::clone(&x)` not `x.clone()`; `rc_mutex`, `mutex_atomic`,
`mutex_integer` are denied.

### Session 8 — Cancellation, graceful shutdown, task locals

**Goal:** understand the lifecycle every app in this repo follows.

**Rust concepts:** dropping a future cancels it; `CancellationToken`; `TaskTracker`;
`tokio::task_local!` and `TaskLocalFuture`; timeouts; structured concurrency in practice.

**Read in this repo:**
- `lib/framework/src/system.rs` — `System<Init>` → `System<Running>` (a **typestate**: `start_service`
  simply does not exist before `start_logger`, enforced by the type system — worth highlighting to a
  Java audience used to runtime "illegal state" exceptions). Also `listen_shutdown_signal` on SIGTERM
  and Ctrl-C, and the appender drain loop with `select!` + `draining` flag.
- `lib/framework/src/task.rs` — `Executor`, `TaskExecutor`, `TaskGuard` (Drop-based deregistration),
  `shutdown(timeout)` reporting aborted task names.
- `lib/framework/src/web/server.rs` — `with_graceful_shutdown` and `shutdown_grace_period`.
- `lib/framework/src/log.rs` — `task_local! { CURRENT_ACTION }`, the mechanism behind Session 10.

**Lab:** add a service to a scratch app that loops until its `CancellationToken` is cancelled, and
verify Ctrl-C drains it; then make it hang and watch `executor.shutdown` report it as aborted.

**Pitfalls:** forgetting to pass the token down; work started with bare `tokio::spawn` that the
tracker does not know about (use `spawn_action!`); `infinite_loop` is a denied lint.

**Checkpoint (end of Phase 2):** in pairs, whiteboard the full startup and shutdown sequence of
`app/demo` from `main()` to the last drained log line.

---

## Phase 3 — The framework

### Session 9 — Application skeleton: `System`, config, assets

**Goal:** stand up a new app crate from scratch.

**Content**
- `System::init(env!("CARGO_PKG_NAME"), DefaultEnv)`, `add_metrics`, `start_logger(appender)`,
  `start_service(|token| ..)`, `wait()`, `shutdown_logger()` — and why they are in that order.
- `trait Env` / `DefaultEnv` / `CloudRunEnv` (`lib/framework/src/cloud/gcloud.rs`) — resolving a
  host name on a managed platform.
- `load_config!("assets/conf.json")` and `load_config!(.., env = "CONFIG")`; `EnvString` with the
  `"env:NAME"` convention for secrets. **Read `doc/config_design.md` in full** — resolution order,
  why env beats the filesystem instead of being a fallback, why blank counts as unset, why it must
  be a macro (`CARGO_MANIFEST_DIR` has to expand in the *calling* crate).
- `asset_path!` and the same debug/release split for static files.
- Appenders: `ConsoleAppender`, `NatsAppender`, `GCloudAppender` — pick per deployment.

**Read:** `app/demo/src/lib.rs`, `app/log_processor_rs/src/main.rs`, `app/log_collector/src/main.rs`,
`lib/framework/src/config.rs`, `lib/framework/src/asset.rs`, `doc/config_design.md`.

**Lab:** create `app/training_<name>` — a binary that loads a config struct, starts a `System` with
`ConsoleAppender`, registers one service that ticks every second, and shuts down cleanly. This crate
is the base for every remaining lab and for the capstone.

### Session 10 — Observability: the action log

**Goal:** this is the framework's centerpiece — one structured record per unit of work.

**Content**
- The model: an **action** (`kind`, `id`, `ref_ids`, `context`, `stats`, `logs`, `severity`, `error`)
  is a task-local created by `log::action(..)` and emitted to the appender when the future resolves.
- Macros, in the order you reach for them:
  - `console!` — before the logger exists, or outside any action (startup, shutdown).
  - `log!("..")` / `log!(exception = e)` — a trace line inside the current action.
  - `warn!(error_code = "..", "..")` / `error!(error_code = "..", "..")` — promotes action severity.
  - `context!(key = value)` — indexed, searchable dimensions.
  - `stats!(key = number)` — numeric aggregates.
  - `span!("db")` — timing + `db_elapsed`/`db_count` stats via `Drop`; `span.clear()` to keep a hot
    loop from filling the buffer.
  - `spawn_action!("name", async { .. })` — background work that gets its own action, linked to the
    parent by `ref_id`.
- Why the trace buffer is only shipped on error or when `log::trace()` was called
  (`Action::flush_trace`), and the `MAX_LOG_BYTES` soft cap.
- Metrics: `Metrics`, `Counter`/`CounterGuard`, `MetricsCollector`, and the
  `fn metrics(&self) -> impl Fn(&mut Metrics)` convention every component exposes.
- Correlation across processes: `ref-id`/`client` HTTP headers, `ref_id`/`client` NATS and Kafka
  headers — this is how one action id follows a request through the whole system.
- Where it lands: `NatsAppender` → `log_processor_rs` → ClickHouse → Grafana (`docker/grafana/`).

**Read:** `lib/framework/src/log.rs`, `log/action.rs`, `log/span.rs`, `lib/framework/src/metrics.rs`,
`lib/framework/src/appender.rs`, `app/demo/examples/action_log.rs`, `app/demo/examples/stats_collector.rs`.

**Lab:** run `--example action_log` and annotate every line of its output. Then instrument your
Session 9 app: a `span!`, a `context!`, a `stats!`, and one deliberate failure; find your action in
the console output and explain each field.

**Pitfalls:** never call `log!` inside a `Display` impl whose output you then pass to `log!` — the
task-local is borrowed twice and it panics (there is a comment saying exactly this in `log.rs`);
`span!` takes a **literal** because the name becomes a compile-time stats key.

### Session 11 — HTTP: axum, routes, `#[api]`, validation

**Goal:** add an endpoint with proper request/response types, validation and error mapping.

**Content**
- `HttpServer` / `HttpServerConfig`, the `http_server_layer` middleware (what it logs, the
  `/health-check` short-circuit, client IP resolution via `max_forwarded_ips`).
- Routing: `framework::web::route::{get, post, ..}` wrap axum's so the handler's name lands in
  `context!(fn = ..)`. `ServeDir`/`ServeFile` for static assets.
- Extractors and bodies: `web::body::{Json, TextBody}`, `client_info`, and how a rejection becomes an
  `Exception` with `BAD_REQUEST`.
- `HttpError` / `HttpResult<T>` and the code → status mapping in `web/error.rs`.
- **`#[api]`** on a trait generates *both* the axum router (`UserService::route(Arc::new(impl))`) and
  a typed HTTP client (`UserServiceClient`) — one declaration, two sides of the wire. Compare with
  Spring's `@RestController` + Feign, or a TS route file + a generated OpenAPI client.
- **`#[derive(Validate)]`** — `#[not_blank]`, `#[range(min, max)]`, `#[length(min, max)]`,
  `#[validate]` for nesting; `Validator::validate()` returns `validation_error!`, which is
  `VALIDATION_ERROR` at `Warn` severity → HTTP 400.
- `HttpClient` / `HttpClientConfig` for outbound calls: timeouts, retry, `internal_only()`.

**Read:** `lib/framework/src/web/*.rs`, `lib/framework/src/http.rs`, `lib/framework/src/validate.rs`,
`app/demo/src/user.rs`, `app/demo/src/user/web.rs`, `app/demo/src/web.rs`,
`app/demo/examples/api_client.rs`, `app/demo/examples/http_client.rs`, `app/demo/examples/validator.rs`.

**Lab:** define an `#[api]` trait with two endpoints, implement it, mount it, call it with the
generated client. Add `#[derive(Validate)]` to the request and verify the 400 body shape.

### Session 12 — Postgres: `Entity`, repository, connection pool

**Goal:** persist and query without hand-writing SQL plumbing.

**Content**
- `Database` / `DbConfig`; the pool (`ResourcePool<ConnectionManager>`, capacity 50, validity
  window, max lifetime, checkout timeout) and its `metrics()`.
- **`#[derive(Entity)]`**: `#[table(name)]`, `#[column(name)]`, `#[primary_key]` and
  `#[primary_key(auto_increment)]`. It generates the insert/select SQL plus a typed const per
  column, so `User::NAME.eq(name)` and `User::RATING.update(v)` are checked against `User`.
- `repository::{insert, insert_ignore, upsert, insert_with_auto_increment_id, select_one, select_all,
  update, delete}` and what each records in `stats!` (`db_read_rows`, `db_write_rows`, `db_*` span).
- Types: `framework_db::types::{Timestamp, Date}`, `Json<T>` for jsonb columns, `Option<T>` for
  nullable columns, `Uuid` (`Uuid::now_v7()` — time-ordered, index-friendly).
- Prepared statement caching (`conn.prepared_statement`) and per-query timeouts.
- Migrations: `app/demo/examples/db_migration.rs`.

**Read:** `lib/framework_db/src/*.rs`, `lib/framework_macro/src/entity.rs` (skim the generated shape),
`app/demo/src/user.rs`, `app/demo/src/user/web.rs`, `test/db_test/tests/*.rs`.

**Lab:** add a table + entity to your training app, write insert / select / update through
`repository`, and back it with an e2e test in `test/` against the Docker Postgres.

### Session 13 — Messaging and analytics: NATS, Kafka, ClickHouse

**Goal:** pick the right transport and wire a handler.

**Content**
- **NATS** (`framework_nats`): one shared `Client` per process.
  - Request/reply: `Service` + `#[nats_api]` traits (`#[subject = ".."]`) generating
    `Trait::service(client, Arc::new(impl), config)` and `TraitClient`; queue groups for load
    balancing; `max_concurrency` semaphore; the `error` header carrying `ErrorResponse`.
  - JetStream: `MessageConsumer` (per-message, bounded concurrency) vs `BatchConsumer`
    (batch → one downstream write); ack policies, `DeliverPolicy`, `batch_max_messages`/`batch_max_wait`.
  - `Producer`, `Subject<T>` as a typed subject name, `NatsAppender`.
- **Kafka** (`framework_kafka`): `Topic<T>`, `MessageConsumer` with `poll_max_records`/
  `poll_max_wait_time`, manual commit, the rdkafka/librdkafka native dependency.
- **ClickHouse** (`framework_clickhouse`): `ClickHouse::new`, `execute`/`select_one`/`select_all`,
  `insert` batching with `async_insert=1` + `wait_for_async_insert=0`, `#[derive(Enum8)]`, the
  `types::{Date, DateTime, Decimal}` wrappers.
- Choosing: request/reply vs stream; NATS vs Kafka in our deployments; batch size vs latency (the
  comment in `log_processor_rs/src/main.rs` is a good worked example).
- **Scheduling**: `Scheduler::new(Offset)`, `schedule_fixed_rate`, `schedule_daily`, `JobContext`,
  and `scheduler.routes(state)` exposing a manual trigger endpoint.

**Read:** `lib/framework_nats/src/{service,consumer,producer,appender}.rs`,
`lib/framework_kafka/src/{consumer,producer}.rs`, `lib/framework_clickhouse/src/lib.rs`,
`lib/framework/src/schedule.rs`, `app/log_processor_rs/src/`, `app/log_collector/src/`,
`app/demo/examples/{nats_service,kafka_producer,kafak_consumer,scheduler}.rs`.

**Lab:** add a `#[nats_api]` service and a JetStream consumer to your training app; publish from one
example binary and consume in another; confirm the `ref_id` header links the two actions in the log.

### Session 14 — Procedural macros: how `framework_macro` works

**Goal:** be able to read, debug, and (carefully) extend the macros the whole framework rests on.

**Content**
- Declarative (`macro_rules!`) vs derive vs attribute macros; when each is the right tool. Reuse the
  examples already seen: `exception!`, `log!`, `context!`, `stats!`, `span!`, `console!`,
  `load_config!`, `asset_path!`, `write_str!`, `spawn_action!`.
- Why some of these *must* be macros: capturing `file!()`/`line!()`/`module_path!()` at the call
  site, and expanding `env!("CARGO_MANIFEST_DIR")` in the caller's crate.
- `proc_macro` crates: `TokenStream`, `syn`, `quote`, `proc_macro2`; parsing, error reporting via
  `syn::Error::into_compile_error`.
- Walk through `lib/framework_macro/src/`: `validate.rs` (simplest), `entity.rs`, `api.rs`,
  `nats_api.rs`, `enum8.rs`, `integration_test.rs`.
- Debugging generated code: `cargo expand`, and the token-stream unit tests already in these files.
- Cost: compile time, and why we keep generated code small and boring.

**Read:** all of `lib/framework_macro/src/`, plus the doc comments in its `lib.rs` (they are the
public contract for each macro).

**Lab:** add one validation attribute (e.g. `#[email]`) to `Validate`, with a token-stream test.
Then run `cargo expand` on `app/demo/src/user.rs` and read what `#[api]` actually generated.

**Note for the instructor:** the `#[derive(Entity)]` doc comment says it generates a `FIELD_<NAME>`
const, while the code generates a bare uppercase const (`User::NAME`). Good live example of reading
the implementation rather than trusting the comment — and a cheap first PR for a participant.

---

## Phase 4 — Quality and operations

### Session 15 — Lints, formatting, testing, build, run

**Goal:** get work through review and into production.

**Content**
- The lint policy: root `Cargo.toml` `[workspace.lints]` (nursery + pedantic + a large restriction
  set), `clippy.toml` test relaxations, `.cargo/config.toml` `warnings = "deny"`. Walk the list of
  the ~15 lints that will actually bite a newcomer (`unwrap_used`, `indexing_slicing`,
  `absolute_paths`, `wildcard_imports`, `str_to_string`, `clone_on_ref_ptr`, `print_stdout`,
  `needless_pass_by_value`, `uninlined_format_args`, `missing_const_for_fn`, `too_many_lines`, …).
  Policy on `#[allow]`: rare, local, and commented.
- Formatting: `rustfmt.toml` (`max_width = 120`, `use_small_heuristics = "Max"`,
  `imports_granularity = "Item"`, `group_imports = "StdExternalCrate"`) — requires nightly rustfmt.
- Tests: `#[cfg(test)] mod tests` inline (see `config.rs`, `span.rs`, `route.rs`, `exception.rs` for
  the house style); `#[tokio::test]`; e2e tests in `test/*` with `#[integration_test]` — read its doc
  comment for the one-test-per-file rule and why (the action sender is a process-wide `OnceLock`).
  Bringing up dependencies from `docker/*/docker-compose.yml`.
- CI: `.github/workflows/build.yml` (`cargo clippy -- -D warnings`, `cargo test --all-features`),
  `codeql.yml`, `docker.yml`.
- Build & deploy: `build/*.sh`, `cloud_build_*.sh`, per-app `Dockerfile`, assets copied next to the
  binary, Cloud Run config via the `CONFIG` env var.
- Profiling: the `profiling` profile, `debug = "line-tables-only"` in release, `large_futures`,
  and where to look when memory or latency is off.

**Lab:** run the full gate on your training app, fix every finding, open a PR against a training
branch and review a peer's.

---

## Capstone (1–2 weeks, self-paced, reviewed)

Build a small but complete service in this workspace. Required elements:

1. Its own crate under `app/`, config via `load_config!` with one `EnvString` secret.
2. `System` lifecycle: metrics registered, logger started, graceful shutdown verified.
3. At least one `#[api]` HTTP endpoint with `#[derive(Validate)]` on the request.
4. At least one Postgres entity with `#[derive(Entity)]` and repository calls.
5. One asynchronous path: a NATS handler, a Kafka consumer, or a scheduled job.
6. Action logging that a reviewer can follow end to end, including a cross-process `ref_id`.
7. Unit tests inline, one e2e test in `test/`.
8. Green `cargo clippy -- -D warnings` and `cargo +nightly fmt --check`.

Reviewed as a normal PR. Demo in a final 60-minute session: each participant walks through one
action log record produced by their service and explains every field.

---

## Appendix A — Mental-model translation table

| Java / TypeScript | Rust here | Notes |
|---|---|---|
| GC reference | ownership + borrow | Session 1 |
| `null` / `undefined` | `Option<T>` | no null at all |
| checked exception / `throw` | `Result<T, Exception>` + `?` | Session 5 |
| stack trace | `Exception::backtrace()` over `source` chain | records `?` sites, not frames |
| interface | trait | can be implemented for foreign types |
| abstract class / inheritance | trait + composition | no inheritance |
| generics (erased) | generics (monomorphized) | `dyn Trait` when you need erasure |
| `synchronized` / `AtomicX` | `Mutex`, atomics, or `&'static` | Session 7 |
| `CompletableFuture` / `Promise` | `Future` | lazy; cancellable by drop |
| `@PostConstruct` / DI container | explicit wiring in `main`/`run` | no framework magic |
| Spring `@RestController` | `#[api]` trait + `route()` | also generates the client |
| Feign / generated OpenAPI client | `<Trait>Client` from `#[api]` | same declaration |
| JPA entity | `#[derive(Entity)]` | no lazy loading, no session |
| Bean Validation `@NotBlank` | `#[not_blank]` via `#[derive(Validate)]` | |
| MDC / trace context | task-local `Action`, `context!` | Session 10 |
| SLF4J logger | `log!` / `warn!` / `error!` inside an action | `console!` outside one |
| Micrometer | `Metrics`, `Counter`, `metrics()` fns | |
| try-with-resources | `Drop` (`Span`, `CounterGuard`, `TaskGuard`) | |
| Maven module / npm package | crate; workspace member | |
| `application.yml` profiles | `load_config!` + `EnvString` | `doc/config_design.md` |

## Appendix B — Macro index (what to reach for)

| Macro | Use |
|---|---|
| `console!` | log outside an action (startup, shutdown) |
| `log!` | trace line in the current action; `log!(exception = e)` |
| `warn!` / `error!` | promote action severity, with an `error_code` |
| `context!` | searchable key/value dimension on the action |
| `stats!` | numeric aggregate on the action |
| `span!("name")` | timed sub-operation; adds `name_elapsed` / `name_count` |
| `exception!` | build an `Exception` with location, `severity`, `code`, `source` |
| `validation_error!` | `VALIDATION_ERROR` at `Warn` → HTTP 400 |
| `spawn_action!` | background task with its own action, linked by `ref_id` |
| `load_config!` | startup config from file or env |
| `asset_path!` | resolve a static asset in dev and in the image |
| `write_str!` | infallible `write!` into a `String` |
| `#[api]` | HTTP service + client from a trait |
| `#[nats_api]` | NATS request/reply service + client from a trait |
| `#[derive(Entity)]` | Postgres table mapping + typed column consts |
| `#[derive(Validate)]` | request validation |
| `#[derive(Enum8)]` | ClickHouse `Enum8` column mapping |
| `#[integration_test]` | e2e test wrapped in a `System` + action |

## Appendix C — Suggested reading order of the codebase

For anyone joining after the program, read in this order:

1. `app/demo/src/lib.rs` — the shape of an app
2. `lib/framework/src/exception.rs` — the error type everything returns
3. `lib/framework/src/log.rs` + `doc/action_future_design.md` — the observability core
4. `lib/framework/src/system.rs` — lifecycle
5. `lib/framework/src/config.rs` + `doc/config_design.md` — startup
6. `lib/framework/src/web/` — the HTTP layer
7. `lib/framework_db/` — persistence
8. `lib/framework_nats/` — messaging
9. `lib/framework_macro/` — how the sugar is made
