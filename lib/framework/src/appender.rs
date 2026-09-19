use std::borrow::Cow;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

use crate::log::ContextValues;
use crate::log::Severity;
use crate::log::action::Action;
use crate::metrics::Metrics;
use crate::system::CONTEXT;
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

impl From<Action> for ActionMessage {
    fn from(action: Action) -> Self {
        let context = CONTEXT.get().expect("context must be initialized");

        // the buffer already carries the lines joined by '\n', so this is a move
        let logs = action.flush_trace().then_some(action.logs);

        let (error_code, error_message) = match action.error {
            Some(error) => (error.code.map(str::to_owned), Some(error.message)),
            None => (None, None),
        };

        ActionMessage {
            id: action.id,
            timestamp: action.timestamp,
            app: Cow::Borrowed(context.app),
            host: Cow::Borrowed(&context.host),
            kind: Cow::Borrowed(action.kind),
            severity: action.severity,
            ref_ids: action.ref_ids,
            error_code,
            error_message,
            context: action.context,
            stats: action.stats,
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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::ActionMessage;
    use crate::log::ScalarContextValue as _;
    use crate::log::action::Action;
    use crate::system::CONTEXT;
    use crate::system::Context;
    use crate::time::DateTime;

    #[test]
    fn action_message_preserves_storage_and_wire_format() {
        let context = CONTEXT.get_or_init(|| Context { app: "test", host: "host".to_owned() });
        let mut action = Action::new("id".to_owned(), "http", None, DateTime::now());
        action.context.push((Cow::Borrowed("method"), "GET".__into_context_value()));
        action.add_stat("count", 2);
        let context_buffer = action.context.as_ptr();
        let stats_buffer = action.stats.as_ptr();

        let message = ActionMessage::from(action);
        assert_eq!(message.context.as_ptr(), context_buffer);
        assert_eq!(message.stats.as_ptr(), stats_buffer);
        assert!(matches!(message.app, Cow::Borrowed(_)));
        assert!(matches!(message.host, Cow::Borrowed(_)));
        assert!(matches!(message.kind, Cow::Borrowed(_)));
        assert!(message.context.iter().all(|(key, values)| matches!(key, Cow::Borrowed(_)) && !values.spilled()));
        assert!(message.stats.iter().all(|(key, _)| matches!(key, Cow::Borrowed(_))));

        let encoded = serde_json::to_value(&message).unwrap();
        assert_eq!(
            encoded,
            serde_json::json!({
                "id": "id", "timestamp": message.timestamp, "app": context.app,
                "host": context.host, "kind": "http", "severity": "INFO",
                "context": [["method", ["GET"]]], "stats": [["elapsed", 0], ["count", 2]]
            })
        );
        let decoded: ActionMessage = serde_json::from_value(encoded.clone()).unwrap();
        assert!(matches!(decoded.app, Cow::Owned(_)));
        assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
    }
}
