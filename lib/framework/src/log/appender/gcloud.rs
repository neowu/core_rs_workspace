use std::any::Any;

use serde::Serialize;
use serde::Serializer;
use serde::ser::SerializeMap as _;

use crate::json;
use crate::log::appender::ActionAppender;
use crate::log::appender::ActionMessage;
use crate::metrics::MetricsAppender;
use crate::metrics::MetricsMessage;
use crate::time::DateTime;

pub struct GCloudAppender;

impl ActionAppender for GCloudAppender {
    async fn append(&self, action: ActionMessage) {
        write_action(&action);
    }
}

impl MetricsAppender for GCloudAppender {
    async fn append(&self, metrics: MetricsMessage) {
        write_metrics(&metrics);
    }
}

#[allow(clippy::print_stdout)]
fn write_action(action: &ActionMessage) {
    let id = action.id.as_str();
    let time = action.timestamp;
    let severity = action.severity.as_str();
    let error_code = action.error_code.as_deref();
    let error_message = action.error_message.as_deref();

    println!(
        "{}",
        json::to_json(&ActionEntry {
            id,
            time,
            kind: &action.kind,
            app: &action.app,
            host: &action.host,
            severity,
            ref_id: action.ref_ids.as_deref(),
            error_code,
            error_message,
            context: action.context.as_ref(),
            stats: &action.stats,
            label: LogLabel { log: "action" },
            trace_id: id,
        })
        .expect("serialize to json cannot fail")
    );

    if let Some(logs) = &action.logs {
        for (i, line) in logs.lines().enumerate() {
            // all trace lines share the action timestamp, gcloud orders by insertId, so use actionId + line number (0001 to 2000) to keep order
            let insert_id = format!("{id}-{:04}", i + 1);
            println!(
                "{}",
                json::to_json(&TraceEntry {
                    id,
                    time,
                    app: &action.app,
                    severity,
                    message: line,
                    label: LogLabel { log: "trace" },
                    trace_id: id,
                    insert_id: &insert_id,
                })
                .expect("serialize to json cannot fail")
            );
        }
    }
}

#[allow(clippy::print_stdout)]
fn write_metrics(metrics: &MetricsMessage) {
    let error_code = metrics.error_code.as_deref();
    let error_message = metrics.error_message.as_deref();

    println!(
        "{}",
        json::to_json(&MetricsEntry {
            id: metrics.id.as_str(),
            time: metrics.timestamp,
            app: &metrics.app,
            host: &metrics.host,
            severity: metrics.severity.as_str(),
            error_code,
            error_message,
            stats: &metrics.stats,
            info: &metrics.info,
            label: LogLabel { log: "metrics" },
        })
        .expect("serialize to json cannot fail")
    );
}

#[derive(Debug, Serialize)]
struct LogLabel {
    log: &'static str,
}

#[derive(Debug, Serialize)]
struct ActionEntry<'a> {
    id: &'a str,
    time: DateTime,
    kind: &'a str,
    app: &'a str,
    host: &'a str,
    severity: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    ref_id: Option<&'a [String]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_message: Option<&'a str>,
    #[serde(flatten, serialize_with = "serialize_key_value_tuple")]
    context: &'a [(String, Vec<String>)],
    #[serde(flatten, serialize_with = "serialize_key_value_tuple")]
    stats: &'a [(String, u64)],
    #[serde(rename = "logging.googleapis.com/labels")]
    label: LogLabel,
    #[serde(rename = "logging.googleapis.com/trace")]
    trace_id: &'a str,
}

#[derive(Debug, Serialize)]
struct MetricsEntry<'a> {
    id: &'a str,
    time: DateTime,
    app: &'a str,
    host: &'a str,
    severity: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_message: Option<&'a str>,
    #[serde(flatten, serialize_with = "serialize_key_value_tuple")]
    stats: &'a [(String, u64)],
    #[serde(serialize_with = "serialize_key_value_tuple")]
    info: &'a [(String, String)],
    #[serde(rename = "logging.googleapis.com/labels")]
    label: LogLabel,
}

#[derive(Debug, Serialize)]
struct TraceEntry<'a> {
    id: &'a str,
    time: DateTime,
    app: &'a str,
    message: &'a str,
    severity: &'static str,
    #[serde(rename = "logging.googleapis.com/labels")]
    label: LogLabel,
    #[serde(rename = "logging.googleapis.com/trace")]
    trace_id: &'a str,
    #[serde(rename = "logging.googleapis.com/insertId")]
    insert_id: &'a str,
}

/// Serializes an ordered key/value list as a json object, a single value collapses to a scalar.
/// Only for the gcloud entries, which are written and never read back.
fn serialize_key_value_tuple<S, K, V>(vec: &[(K, V)], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    K: Serialize,
    V: Serialize + 'static,
{
    // Initialize the map serializer with the exact size
    let mut map = serializer.serialize_map(Some(vec.len()))?;
    for (k, v) in vec {
        if let Some(values) = (v as &dyn Any).downcast_ref::<Vec<String>>()
            && values.len() == 1
            && let Some(first) = values.first()
        {
            map.serialize_entry(k, first)?;
        } else {
            map.serialize_entry(k, v)?;
        }
    }
    map.end()
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::ActionEntry;
    use super::LogLabel;
    use crate::json;
    use crate::time::Date;
    use crate::time::DateTime;
    use crate::time::Time;

    #[test]
    fn serialize_action_entry() {
        let context = vec![("user_id".to_owned(), vec!["u1".to_owned()])];
        let stats = vec![("count".to_owned(), 42)];

        let entry = ActionEntry {
            id: "action-1",
            time: DateTime::new(Date::new(2023, 11, 14), Time::new(22, 13, 20)),
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
        assert_eq!(value["user_id"], "u1");
        assert_eq!(value["count"], 42.0);
        assert_eq!(value["logging.googleapis.com/labels"]["log"], "action");
        assert_eq!(value["logging.googleapis.com/trace"], "action-1");
    }
}
