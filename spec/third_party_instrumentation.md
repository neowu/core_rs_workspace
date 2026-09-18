# Third party instrumentation is compiled out

Code: [`lib/framework/Cargo.toml`](../lib/framework/Cargo.toml),
[`lib/framework/src/lib.rs`](../lib/framework/src/lib.rs) · measured by
[`spec/benchmark/http_server.md`](benchmark/http_server.md)

`axum`, `hyper`, `h2`, `tower` and `reqwest` are instrumented with `tracing`. This framework never
reads any of it: it installs no tracing subscriber and no `log` logger, because it has its own
action log and appenders. The instrumentation therefore runs on every request and its output is
discarded — it measured **~8% of server cpu per request** on the http benchmark.

`framework` depends on `tracing` and `log` for one reason, to turn on their compile time max level:

```toml
tracing = { version = "*", features = ["release_max_level_off"] }
log = { version = "*", features = ["release_max_level_off"] }
```

Neither crate is used in code, only `use {::log as _, tracing as _};` to satisfy
`unused_crate_dependencies`. The leading `::` is required and is not cosmetic: `framework::log` is a
module of this crate, so a bare `use log as _;` in the crate root silently resolves to that module,
leaves the `log` *crate* unused, and trips the very lint it was written to satisfy.

## Why both crates

They gate two independent paths, and disabling only one leaves the other running:

| feature | compiles out |
|---|---|
| `tracing/release_max_level_off` | span and event construction — `span!`, `trace!`, `record_all` |
| `log/release_max_level_off` | the `tracing` → `log` bridge — `Span::log`, `__tracing_log!` |

The bridge is a separate path on purpose: `tracing`'s own docs state that its static max level
features do *not* control the `log` records emitted when `tracing/log` is on, so that a binary can
compile out tracing entirely and still collect `log` records. Here nothing collects them.

Nothing in this workspace asks for `tracing/log` directly — `axum`'s default `tower-log` feature
pulls it in through `tower/log`.

## Why `release_max_level_*` and not `max_level_*`

The `release_` variants are `cfg(not(debug_assertions))`, so debug builds keep full instrumentation.
Someone debugging an `h2` or `hyper` problem can drop in `tracing-subscriber` and see everything;
only shipped builds pay nothing.

## The cost of the decision

Cargo features are additive and cannot be turned off by a dependent, so this disables release-build
tracing for **every** crate that depends on `framework`, including any app that wanted a subscriber
of its own. That is the intended posture — the framework owns logging — but it is a workspace wide
decision, not a per app one. An app that genuinely needs release tracing has to drop these two
dependencies from `framework`, not override them.

Without a subscriber the runtime cost was already small per call — `Span::log` bails after an atomic
load of `log::max_level()`, which is `Off` — and it showed up only because `h2` creates spans per
stream and per frame. Allocation counts are unchanged — the discarded instrumentation never got as
far as formatting. The win is instruction count and lock traffic, not heap.
