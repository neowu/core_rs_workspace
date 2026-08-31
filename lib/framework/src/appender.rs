use serde::Deserialize;
use serde::Serialize;

use crate::log::Severity;
use crate::log::action::Action;
use crate::metrics::Metrics;
use crate::system::CONTEXT;
use crate::time::DateTime;

pub(crate) mod console;
pub(crate) mod gcloud;

pub use console::ConsoleAppender;
pub use gcloud::GCloudAppender;

/// Writes action and metrics messages to the underlying log storage.
/// An app has a single appender, owned by the system daemon.
pub trait Appender: Send + 'static {
    fn append_action(&self, action: ActionMessage) -> impl Future<Output = ()> + Send;

    fn append_metrics(&self, metrics: MetricsMessage) -> impl Future<Output = ()> + Send;
}

/// Carried on the single channel feeding the appender daemon.
pub(crate) enum Message {
    Action(ActionMessage),
    Metrics(MetricsMessage),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActionMessage {
    pub id: String,
    pub timestamp: DateTime,
    pub app: String,
    pub host: String,
    pub kind: String,
    pub severity: Severity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub context: Vec<(String, Vec<String>)>,
    pub stats: Vec<(String, u64)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs: Option<String>,
}

impl From<Action> for ActionMessage {
    fn from(action: Action) -> Self {
        let context = CONTEXT.get().expect("context must be initialized");

        let logs = action.flush_trace().then(|| action.logs.join("\n"));

        let (error_code, error_message) = match action.error {
            Some(error) => (error.code.map(str::to_owned), Some(error.message)),
            None => (None, None),
        };

        ActionMessage {
            id: action.id,
            timestamp: action.timestamp,
            app: context.app.to_owned(),
            host: context.host.clone(),
            kind: action.kind.to_owned(),
            severity: action.severity,
            ref_ids: action.ref_ids,
            error_code,
            error_message,
            context: action.context,
            stats: action.stats.into_iter().collect::<Vec<_>>(),
            logs,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetricsMessage {
    pub id: String,
    pub timestamp: DateTime,
    pub app: String,
    pub host: String,
    pub severity: Severity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub stats: Vec<(String, u64)>,
    pub info: Vec<(String, String)>,
}

impl From<Metrics> for MetricsMessage {
    fn from(metrics: Metrics) -> Self {
        let (error_code, error_message) = match metrics.error {
            Some(error) => (error.code.map(str::to_owned), Some(error.message)),
            None => (None, None),
        };

        let context = CONTEXT.get().expect("context must be initialized");

        MetricsMessage {
            id: metrics.id,
            timestamp: metrics.timestamp,
            app: context.app.to_owned(),
            host: context.host.clone(),
            severity: metrics.severity,
            error_code,
            error_message,
            stats: metrics.stats.into_iter().map(|(key, value)| (key.to_owned(), value)).collect(),
            info: metrics.info.into_iter().map(|(key, value)| (key.to_owned(), value)).collect(),
        }
    }
}
