use std::borrow::Cow;
use std::sync::OnceLock;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use smallvec::SmallVec;
use tokio::sync::mpsc::UnboundedSender;

use crate::exception::Severity;
use crate::time::DateTime;
use crate::write_str;

/// Writes action and metrics messages to the underlying log storage.
/// An app has a single appender, owned by the system daemon.
pub trait Appender: Send + 'static {
    fn append_action(&self, action: ActionMessage) -> impl Future<Output = ()> + Send;

    fn append_metrics(&self, metrics: MetricsMessage) -> impl Future<Output = ()> + Send;

    /// Called once after the daemon drained the channel, before the process exits.
    /// The default does nothing, an appender that buffers must flush here.
    fn flush(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// Carried on the single channel feeding the appender daemon.
pub(crate) enum Message {
    Action(ActionMessage),
    Metrics(MetricsMessage),
}

pub(crate) static SENDER: OnceLock<UnboundedSender<Message>> = OnceLock::new();

/// Stamped on every message, set once by `System::init`.
pub(crate) struct Context {
    pub(crate) app: &'static str,
    pub(crate) host: String,
}

pub(crate) static CONTEXT: OnceLock<Context> = OnceLock::new();

/// Context values serialize as an array, with the common single value stored inline.
pub type ContextValues = SmallVec<[String; 1]>;

/// Locally produced metadata is borrowed; deserialized messages own their strings.
/// Context values remain arrays on the wire, including single values.
#[derive(Debug, Serialize, Deserialize)]
pub struct ActionMessage {
    pub id: String,
    pub timestamp: DateTime,
    pub app: Cow<'static, str>,
    pub host: Cow<'static, str>,
    pub kind: Cow<'static, str>,
    pub severity: Severity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub context: Vec<(Cow<'static, str>, ContextValues)>,
    pub stats: Vec<(Cow<'static, str>, u64)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetricsMessage {
    pub id: String,
    pub timestamp: DateTime,
    pub app: Cow<'static, str>,
    pub host: Cow<'static, str>,
    pub severity: Severity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub stats: Vec<(Cow<'static, str>, u64)>,
    pub info: Vec<(Cow<'static, str>, String)>,
}

pub struct ConsoleAppender;

impl Appender for ConsoleAppender {
    async fn append_action(&self, action: ActionMessage) {
        write_action(action);
    }

    async fn append_metrics(&self, metrics: MetricsMessage) {
        write_metrics(metrics);
    }
}

#[allow(clippy::print_stdout, clippy::print_stderr)]
fn write_action(action: ActionMessage) {
    let date = action.timestamp.to_rfc3339();
    let severity = action.severity.as_str();
    let kind = &action.kind;
    let id = &action.id;
    let app = &action.app;
    let host = &action.host;
    let mut log = format!("ACTION: {date} | {severity} | {kind} | id={id} | app={app} | host={host}");

    if let Some(error_code) = action.error_code {
        write_str!(&mut log, " | error_code={error_code}");
    }
    if let Some(error_message) = action.error_message {
        write_str!(&mut log, " | error_message={error_message}");
    }

    if let Some(ref ref_id) = action.ref_ids {
        if ref_id.len() == 1
            && let Some(ref_id) = ref_id.first()
        {
            write_str!(&mut log, " | ref_id={ref_id}");
        } else {
            write_str!(&mut log, " | ref_id={ref_id:?}");
        }
    }

    for (key, values) in action.context {
        if values.len() == 1
            && let Some(value) = values.first()
        {
            write_str!(&mut log, " | {key}={value}");
        } else {
            write_str!(&mut log, " | {key}={values:?}");
        }
    }

    for (key, value) in action.stats {
        if key.ends_with("elapsed") {
            write_str!(&mut log, " | {key}={:?}", Duration::from_nanos(value));
        } else {
            write_str!(&mut log, " | {key}={value}");
        }
    }

    println!("{log}");

    if let Some(logs) = action.logs {
        eprintln!("{logs}");
    }
}

#[allow(clippy::print_stdout)]
fn write_metrics(metrics: MetricsMessage) {
    let date = metrics.timestamp.to_rfc3339();
    let severity = metrics.severity.as_str();
    let app = metrics.app;
    let host = metrics.host;
    let mut log = format!("METRICS: {date} | {severity} | app={app} | host={host}");

    if let Some(error_code) = metrics.error_code {
        write_str!(&mut log, " | error_code={error_code}");
    }
    if let Some(error_message) = metrics.error_message {
        write_str!(&mut log, " | error_message={error_message}");
    }

    for (key, value) in metrics.stats {
        write_str!(&mut log, " | {key}={value}");
    }

    for (key, value) in metrics.info {
        write_str!(&mut log, " | {key}={value}");
    }

    println!("{log}");
}

// do not log action, only print trace to stderr if error happened
pub struct TraceAppender;

impl Appender for TraceAppender {
    #[allow(clippy::print_stderr)]
    async fn append_action(&self, action: ActionMessage) {
        if let Some(logs) = action.logs {
            eprintln!("{logs}");
        }
    }

    async fn append_metrics(&self, _metrics: MetricsMessage) {}
}
