use std::cell::RefCell;
use std::env;
use std::sync::OnceLock;
use std::thread;
use std::time::Instant;

use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use indexmap::IndexMap;
use tokio::task_local;
use tracing::Instrument as _;
use tracing::info_span;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer as _;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

use crate::exception::Exception;
use crate::exception::Severity;
use crate::log::appender::APPENDER;
use crate::log::appender::ActionAppender;
use crate::log::layer::ActionLogLayer;

pub mod appender;
pub mod id_generator;
mod layer;

pub fn init() {
    // SAFETY:
    // init only be called once on startup, no threading issue
    unsafe {
        env::set_var("RUST_BACKTRACE", "1");
    }

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .compact()
                .with_ansi(false) // generally cloud log console doesn't support color
                .with_line_number(true)
                .with_thread_ids(true)
                .with_filter(LevelFilter::INFO),
        )
        .with(ActionLogLayer)
        .init();
}

static APP: OnceLock<&'static str> = OnceLock::new();

pub fn init_action_log_appender(appender: &str, app: &'static str) -> Result<(), Exception> {
    let value = match appender {
        "console" => ActionAppender::Console,
        "gcloud" => ActionAppender::GoogleCloud,
        _ => return Err(exception!("unknown appender, value={appender}")),
    };
    APPENDER.set(value).map_err(|_err| exception!("appender was already initialized"))?;
    APP.set(app).map_err(|_err| exception!("appender was already initialized"))
}

#[inline]
pub async fn start_action<T, R>(kind: &'static str, ref_id: Option<String>, task: T) -> Result<R, Exception>
where
    T: Future<Output = Result<R, Exception>>,
{
    let action_id = id_generator::random_id();
    let action_span = info_span!("action", kind, action_id, ref_id);
    let mut action = Action {
        start_time: Instant::now(),
        id: action_id,
        app: APP.get().unwrap_or(&"unknown"),
        kind,
        date: Utc::now(),
        ref_id,
        severity: None,
        error_code: None,
        error_message: None,
        context: IndexMap::new(),
        stats: IndexMap::new(),
        logs: Vec::with_capacity(32),
    };
    action.logs.push(format!(
        "# action begin, kind={}, id={}, date={}, thread={:?}, ref_id={:?}",
        &action.kind,
        &action.id,
        &action.date.to_rfc3339_opts(SecondsFormat::Nanos, true),
        thread::current().id(),
        &action.ref_id
    ));
    CURRENT_ACTION
        .scope(RefCell::new(action), async move {
            let result = task.instrument(action_span).await;
            if let Err(e) = &result {
                e.log();
            }
            CURRENT_ACTION.with(|current_action| {
                let mut current_action = current_action.borrow_mut();
                let elapsed = current_action.start_time.elapsed();
                current_action.stats.insert("elapsed".to_owned(), elapsed.as_nanos());
                if current_action.severity.is_some() {
                    current_action.logs.push(format!("# action end, elapsed={elapsed:?}"));
                }
                if let Some(appender) = APPENDER.get() {
                    appender.append(&current_action);
                }
            });
            result
        })
        .await
}

task_local! {
    static CURRENT_ACTION: RefCell<Action>;
}

#[inline]
pub fn current_action_id() -> Option<String> {
    CURRENT_ACTION.try_with(|action| Some(action.borrow().id.clone())).unwrap_or(None)
}

pub const CONTEXT: &str = "__context";
pub const STATS: &str = "__stats";

#[macro_export]
macro_rules! context {
    ($($key:ident = $value:expr),+ $(,)?) => {
        ::tracing::event!(
            name: $crate::log::CONTEXT,
            ::tracing::Level::DEBUG,
            $($key = $value),+
        )
    };
}

#[macro_export]
macro_rules! stats {
    ($($key:ident = $value:expr),+ $(,)?) => {
        ::tracing::event!(
            name: $crate::log::STATS,
            ::tracing::Level::DEBUG,
            $($key = $value as u128),+
        )
    };
}

struct Action {
    start_time: Instant,
    id: String,
    app: &'static str,
    kind: &'static str,
    date: DateTime<Utc>,
    ref_id: Option<String>,
    severity: Option<Severity>,
    error_code: Option<String>,
    error_message: Option<String>,
    context: IndexMap<&'static str, String>,
    stats: IndexMap<String, u128>,
    logs: Vec<String>,
}
