use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use serde::Serialize;
use serde::Serializer;
use serde::ser::SerializeMap as _;

use crate::exception::Severity;
use crate::json;
use crate::log::Action;
use crate::log::action::Error;
use crate::log::metrics::Metrics;
use crate::network::hostname;
use crate::write_str;

pub enum Appender {
    Console,
    GoogleCloud,
}

impl Appender {
    // appender must not emit log event!(), it could trigger layer on_event, make CURRENT_ACTION.borrow_mut() panic
    pub(crate) fn append_action(&self, action: &Action, app: &'static str) {
        match self {
            Appender::Console => append_console(action),
            Appender::GoogleCloud => append_gcloud(action, app),
        }
    }

    pub(crate) fn append_metrics(&self, metrics: &Metrics, app: &'static str) {
        match self {
            Appender::Console => append_metrics_console(metrics),
            Appender::GoogleCloud => append_metrics_gcloud(metrics, app),
        }
    }
}

#[allow(clippy::print_stdout, clippy::print_stderr)]
fn append_console(action: &Action) {
    let date = action.date.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let severity = severity(action.error.as_ref());
    let kind = action.kind;
    let id = &action.id;
    let mut log = format!("ACTION: {date} | {severity} | {kind} | id={id}");

    if let Some(error) = &action.error {
        if let Some(error_code) = error.code {
            write_str!(&mut log, " | error_code={error_code}");
        }
        write_str!(&mut log, " | error_message={}", error.message);
    }

    if let Some(ref ref_id) = action.ref_id {
        write_str!(&mut log, " | ref_id={ref_id:?}");
    }

    for (key, values) in &action.context {
        if values.len() == 1
            && let Some(value) = values.first()
        {
            write_str!(&mut log, " | {key}={value}");
        } else {
            write_str!(&mut log, " | {key}={values:?}");
        }
    }

    for (key, value) in &action.stats {
        if key.ends_with("elapsed") {
            write_str!(&mut log, " | {key}={:?}", Duration::from_nanos(*value));
        } else {
            write_str!(&mut log, " | {key}={value}");
        }
    }

    println!("{log}");

    if action.flush_trace() {
        for line in &action.logs {
            eprintln!("{line}");
        }
    }
}

#[allow(clippy::print_stdout)]
fn append_metrics_console(metrics: &Metrics) {
    let date = metrics.date.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let mut log = format!("METRICS: {date}");

    if let Some(error) = &metrics.error {
        let severity = severity(metrics.error.as_ref());
        write_str!(&mut log, " | {severity}");
        if let Some(error_code) = error.code {
            write_str!(&mut log, " | error_code={error_code}");
        }
        write_str!(&mut log, " | error_message={}", error.message);
    }

    for (key, value) in &metrics.stats {
        write_str!(&mut log, " | {key}={value}");
    }

    for (key, value) in &metrics.info {
        write_str!(&mut log, " | {key}={value}");
    }

    println!("{log}");
}

#[allow(clippy::print_stdout)]
fn append_gcloud(action: &Action, app: &'static str) {
    let id = action.id.to_string();
    let id = id.as_str();
    let time = action.date;
    let severity = severity(action.error.as_ref());
    let error_code = action.error.as_ref().and_then(|e| e.code);
    let error_message = action.error.as_ref().map(|e| e.message.as_str());

    println!(
        "{}",
        json::to_json(&ActionEntry {
            id,
            time,
            kind: action.kind,
            app,
            host: hostname(),
            severity,
            ref_id: action.ref_id.as_deref(),
            error_code,
            error_message,
            context: action.context.as_ref(),
            stats: &action.stats,
            label: LogLabel { log: "action" },
            trace_id: id,
        })
        .expect("serialize to json cannot fail")
    );

    if action.flush_trace() {
        for line in &action.logs {
            println!(
                "{}",
                json::to_json(&TraceEntry {
                    id,
                    time,
                    app,
                    severity,
                    message: line,
                    label: LogLabel { log: "trace" },
                    trace_id: id,
                })
                .expect("serialize to json cannot fail")
            );
        }
    }
}

#[allow(clippy::print_stdout)]
fn append_metrics_gcloud(metrics: &Metrics, app: &'static str) {
    let error_code = metrics.error.as_ref().and_then(|e| e.code);
    let error_message = metrics.error.as_ref().map(|e| e.message.as_str());

    println!(
        "{}",
        json::to_json(&MetricsEntry {
            id: metrics.id.to_string().as_str(),
            time: metrics.date,
            app,
            host: hostname(),
            severity: severity(metrics.error.as_ref()),
            error_code,
            error_message,
            stats: &metrics.stats,
            info: &metrics.info,
            label: LogLabel { log: "metrics" },
        })
        .expect("serialize to json cannot fail")
    );
}

const fn severity(error: Option<&Error>) -> &'static str {
    if let Some(error) = error {
        match error.severity {
            Severity::Warn => "WARN",
            Severity::Error => "ERROR",
        }
    } else {
        "INFO"
    }
}

#[derive(Debug, Serialize)]
struct LogLabel {
    log: &'static str,
}

#[derive(Debug, Serialize)]
struct ActionEntry<'a> {
    id: &'a str,
    time: DateTime<Utc>,
    kind: &'a str,
    app: &'static str,
    host: &'static str,
    severity: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    ref_id: Option<&'a [String]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_message: Option<&'a str>,
    #[serde(flatten, serialize_with = "vec_to_map")]
    context: &'a [(&'static str, Vec<String>)],
    #[serde(flatten)]
    stats: &'a HashMap<Cow<'static, str>, u64>,
    #[serde(rename = "logging.googleapis.com/labels")]
    label: LogLabel,
    #[serde(rename = "logging.googleapis.com/trace")]
    trace_id: &'a str,
}

#[derive(Debug, Serialize)]
struct MetricsEntry<'a> {
    id: &'a str,
    time: DateTime<Utc>,
    app: &'static str,
    host: &'static str,
    severity: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_message: Option<&'a str>,
    #[serde(flatten, serialize_with = "vec_to_map")]
    stats: &'a [(&'static str, u64)],
    #[serde(serialize_with = "vec_to_map")]
    info: &'a [(&'static str, String)],
    #[serde(rename = "logging.googleapis.com/labels")]
    label: LogLabel,
}

#[derive(Debug, Serialize)]
struct TraceEntry<'a> {
    id: &'a str,
    time: DateTime<Utc>,
    app: &'static str,
    message: &'a str,
    severity: &'static str,
    #[serde(rename = "logging.googleapis.com/labels")]
    label: LogLabel,
    #[serde(rename = "logging.googleapis.com/trace")]
    trace_id: &'a str,
}

// Custom serialization function
fn vec_to_map<S, V>(vec: &[(&'static str, V)], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    V: Serialize,
{
    // Initialize the map serializer with the exact size
    let mut map = serializer.serialize_map(Some(vec.len()))?;
    for (k, v) in vec {
        map.serialize_entry(k, v)?;
    }
    map.end()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::DateTime;
    use serde_json::Value;

    use super::ActionEntry;
    use super::LogLabel;
    use crate::json;

    #[test]
    fn serialize_action_entry() {
        let context = vec![("user_id", vec!["u1".to_owned()])];
        let mut stats = HashMap::new();
        stats.insert("count".into(), 42);

        let entry = ActionEntry {
            id: "action-1",
            time: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            kind: "http",
            app: "test-app",
            host: "host-1",
            severity: "ERROR",
            ref_id: Some(&["ref-1".to_owned()]),
            error_code: Some("BAD_REQUEST"),
            error_message: Some("invalid input"),
            context: &context,
            stats: &stats,
            label: LogLabel { log: "action" },
            trace_id: "action-1",
        };

        let json = json::to_json(&entry).unwrap();
        let value: Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["id"], "action-1");
        assert_eq!(value["kind"], "http");
        assert_eq!(value["app"], "test-app");
        assert_eq!(value["host"], "host-1");
        assert_eq!(value["severity"], "ERROR");
        assert_eq!(value["ref_id"][0], "ref-1");
        assert_eq!(value["error_code"], "BAD_REQUEST");
        assert_eq!(value["error_message"], "invalid input");
        assert_eq!(value["time"], "2023-11-14T22:13:20Z");
        // context/stats are flattened into the top-level object
        assert_eq!(value["user_id"][0], "u1");
        assert_eq!(value["count"], 42);
        assert_eq!(value["logging.googleapis.com/labels"]["log"], "action");
        assert_eq!(value["logging.googleapis.com/trace"], "action-1");
    }
}
