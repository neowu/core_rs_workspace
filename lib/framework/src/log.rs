use std::env;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Instant;

use chrono::DateTime;
use chrono::Utc;
use indexmap::IndexMap;
use serde::Serialize;
use tokio::task_local;
use tracing::Instrument as _;
use tracing::info_span;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer as _;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

use crate::exception::Exception;
use crate::log::appender::APPENDER;
use crate::log::appender::ActionLogAppender;
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
        "console" => ActionLogAppender::Console,
        "gcloud" => ActionLogAppender::GoogleCloud,
        _ => return Err(exception!("unknown appender, value={appender}")),
    };
    APPENDER.set(value).map_err(|_err| exception!("appender was already initialized"))?;
    APP.set(app).map_err(|_err| exception!("appender was already initialized"))
}

#[inline]
pub async fn start_action<T>(action: &str, ref_id: Option<String>, task: T)
where
    T: Future<Output = Result<(), Exception>>,
{
    let action_id = id_generator::random_id();
    let action_span = info_span!("action", action, action_id, ref_id);
    CURRENT_ACTION_ID
        .scope(
            action_id,
            async {
                let result = task.await;
                if let Err(e) = result {
                    e.log();
                }
            }
            .instrument(action_span),
        )
        .await;
}

task_local! {
    static CURRENT_ACTION_ID: String;

    static CURRENT_ACTION_LOG: Arc<Mutex<ActionLog>>;
}

#[inline]
pub fn current_action_id() -> Option<String> {
    CURRENT_ACTION_ID.try_with(|current_action_id| Some(current_action_id.clone())).unwrap_or(None)
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

// TODO: rethink fields, like result

struct ActionLog {
    id: String,
    app: &'static str,
    action: String,
    date: DateTime<Utc>,
    start_time: Instant,
    result: ActionResult,
    ref_id: Option<String>,
    error_code: Option<String>,
    error_message: Option<String>,
    context: IndexMap<&'static str, String>,
    stats: IndexMap<String, u128>,
    logs: Vec<String>,
}

#[derive(PartialEq, Serialize, Debug)]
pub enum ActionResult {
    #[serde(rename = "OK")]
    Ok,
    #[serde(rename = "WARN")]
    Warn,
    #[serde(rename = "ERROR")]
    Error,
}

impl ActionResult {
    const fn level(&self) -> u32 {
        match *self {
            ActionResult::Ok => 0,
            ActionResult::Warn => 1,
            ActionResult::Error => 2,
        }
    }
}
