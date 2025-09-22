use std::env;
use std::fmt::Debug;

pub use appender::ConsoleAppender;
use chrono::DateTime;
use chrono::Utc;
use indexmap::IndexMap;
use layer::ActionLogLayer;
use serde::Serialize;
use tokio::task_local;
use tracing::Instrument;
use tracing::Level;
use tracing::info_span;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::exception::CoreRsResult;
use crate::exception::Exception;
use crate::exception::Severity;

mod appender;
pub mod id_generator;
mod layer;

pub trait ActionLogAppender {
    fn append(&self, action_log: ActionLogMessage);
}

task_local! {
    static CURRENT_ACTION_ID: String
}

pub fn init() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_ansi(false) // generally cloud log console doesn't support color
                .with_line_number(true)
                .with_thread_ids(true)
                .with_filter(LevelFilter::INFO),
        )
        .init();
}

pub fn init_with_action<T>(appender: T)
where
    T: ActionLogAppender + Send + Sync + 'static,
{
    unsafe { env::set_var("RUST_BACKTRACE", "1") };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_ansi(false) // generally cloud log console doesn't support color
                .with_line_number(true)
                .with_thread_ids(true)
                .with_filter(LevelFilter::INFO),
        )
        .with(ActionLogLayer { appender })
        .init();
}

macro_rules! log_event {
    (level = $level:ident, error_code = $error_code:expr, $($arg:tt)+) => {
        match $level {
            ::tracing::Level::TRACE => {},
            ::tracing::Level::DEBUG => {},
            ::tracing::Level::INFO => {},
            ::tracing::Level::WARN => {
                match $error_code {
                    Some(ref error_code) => ::tracing::warn!(error_code, $($arg)+),
                    None => ::tracing::warn!($($arg)+),
                }
            },
            ::tracing::Level::ERROR => {
                match $error_code {
                    Some(ref error_code) => ::tracing::error!(error_code, $($arg)+),
                    None => ::tracing::error!($($arg)+),
                }
            }
        }
    };
}

pub async fn start_action<T>(action: &str, ref_id: Option<String>, task: T)
where
    T: Future<Output = CoreRsResult<()>>,
{
    let action_id = id_generator::random_id();
    let action_span = info_span!("action", action, action_id, ref_id);
    CURRENT_ACTION_ID
        .scope(
            action_id,
            async {
                let result = task.await;
                if let Err(e) = result {
                    log_exception(&e);
                }
            }
            .instrument(action_span),
        )
        .await;
}

pub(crate) fn log_exception(e: &Exception) {
    let level = match e.severity {
        Severity::Warn => Level::WARN,
        Severity::Error => Level::ERROR,
    };
    let message = &e.message;
    log_event!(
        level = level,
        error_code = e.code,
        backtrace = e.to_string(),
        "{message}"
    );
}

pub fn current_action_id() -> Option<String> {
    CURRENT_ACTION_ID
        .try_with(|current_action_id| Some(current_action_id.clone()))
        .unwrap_or(None)
}

#[derive(Serialize, Debug)]
pub struct ActionLogMessage {
    pub id: String,
    pub date: DateTime<Utc>,
    pub action: String,
    pub result: ActionResult,
    pub ref_id: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub context: IndexMap<&'static str, String>,
    pub stats: IndexMap<String, u128>,
    pub trace: Option<String>,
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
    fn level(&self) -> u32 {
        match self {
            ActionResult::Ok => 0,
            ActionResult::Warn => 1,
            ActionResult::Error => 2,
        }
    }
}
