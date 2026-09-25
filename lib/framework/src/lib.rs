// axum/hyper/h2/tower carry `tracing` instrumentation this framework never reads: it installs no
// tracing subscriber and no `log` logger, it has its own action log and appenders. Depending on
// both crates here only to turn on their `release_max_level_off` features, which compiles that
// instrumentation out of release builds. Cargo features are additive, so this applies to every
// crate in the graph. `::log` must stay absolute — a bare `log` resolves to the module below.
use ::log as _;
use tracing as _;

pub mod api;
pub mod appender;
pub mod asset;
#[macro_use]
pub mod exception;
pub mod config;
pub mod fs;
pub mod http;
pub mod json;
#[macro_use]
pub mod log;
pub mod cloud;
pub mod metrics;
pub mod network;
pub mod number;
pub mod pool;
pub mod schedule;
pub mod shell;
pub mod string;
pub mod system;
pub mod task;
pub mod time;
pub mod validate;
pub mod web;
