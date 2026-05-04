use std::env;
use std::fmt::Debug;

use chrono::DateTime;
use chrono::Utc;
use indexmap::IndexMap;
use layer::ActionLogLayer;
use serde::Serialize;
use tokio::task_local;
use tracing::Instrument as _;
use tracing::error;
use tracing::info_span;
use tracing::level_filters::LevelFilter;
use tracing::warn;
use tracing_subscriber::Layer as _;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

use crate::exception::Exception;
use crate::exception::Severity;

pub mod appender;
pub mod id_generator;
mod layer;

pub trait ActionLogAppender {
    fn append(&self, action_log: ActionLogMessage);
}

task_local! {
    static CURRENT_ACTION_ID: String
}

#[inline]
pub fn init() {
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .compact()
                .with_ansi(false) // generally cloud log console doesn't support color
                .with_line_number(true)
                .with_thread_ids(true)
                .with_filter(LevelFilter::INFO),
        )
        .init();
}

#[inline]
pub fn init_with_action<T>(appender: T)
where
    T: ActionLogAppender + Send + Sync + 'static,
{
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
        .with(ActionLogLayer { appender })
        .init();
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
                    log_exception(&e);
                }
            }
            .instrument(action_span),
        )
        .await;
}

pub(crate) fn log_exception(e: &Exception) {
    let backtrace = e.to_string();
    let message = &e.message;
    match (&e.severity, e.code.as_ref()) {
        (&Severity::Warn, Some(error_code)) => warn!(error_code, backtrace, "{message}"),
        (&Severity::Warn, None) => warn!(backtrace, "{message}"),
        (&Severity::Error, Some(error_code)) => error!(error_code, backtrace, "{message}"),
        (&Severity::Error, None) => error!(backtrace, "{message}"),
    }
}

#[inline]
pub fn current_action_id() -> Option<String> {
    CURRENT_ACTION_ID.try_with(|current_action_id| Some(current_action_id.clone())).unwrap_or(None)
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
        match *self {
            ActionResult::Ok => 0,
            ActionResult::Warn => 1,
            ActionResult::Error => 2,
        }
    }
}
