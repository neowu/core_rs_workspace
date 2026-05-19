use std::env;
use std::fmt::Debug;

use chrono::DateTime;
use chrono::Utc;
use indexmap::IndexMap;
use layer::ActionLogLayer;
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
                    e.log();
                }
            }
            .instrument(action_span),
        )
        .await;
}

#[inline]
pub fn current_action_id() -> Option<String> {
    CURRENT_ACTION_ID.try_with(|current_action_id| Some(current_action_id.clone())).unwrap_or(None)
}

pub const CONTEXT: &str = "__context";
pub const STATS: &str = "__stats";

#[macro_export]
macro_rules! context {
    ($($fields:tt)+) => {
        ::tracing::event!(
            name: $crate::log::CONTEXT,
            ::tracing::Level::DEBUG,
            $($fields)+
        )
    };
}

#[macro_export]
macro_rules! stats {
    ($($fields:tt)+) => {
        ::tracing::event!(
            name: $crate::log::STATS,
            ::tracing::Level::DEBUG,
            $($fields)+
        )
    };
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
    const fn level(&self) -> u32 {
        match *self {
            ActionResult::Ok => 0,
            ActionResult::Warn => 1,
            ActionResult::Error => 2,
        }
    }
}
