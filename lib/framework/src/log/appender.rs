use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use serde::Serialize;

use crate::exception::Severity;
use crate::json;
use crate::log::Action;
use crate::network::hostname;
use crate::write_str;

pub enum Appender {
    Console,
    GoogleCloud,
}

impl Appender {
    // appender must not emit log event!(), it could trigger layer on_event, make CURRENT_ACTION.borrow_mut() panic
    pub(super) fn append_action(&self, action: &Action, app: &'static str) {
        match self {
            Appender::Console => append_console(action),
            Appender::GoogleCloud => append_gcloud(action, app),
        }
    }
}

#[allow(clippy::print_stdout, clippy::print_stderr)]
fn append_console(action: &Action) {
    let date = action.date.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let severity = severity(action);
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
        write_str!(&mut log, " | ref_id={ref_id}");
    }

    for (key, value) in &action.context {
        write_str!(&mut log, " | {key}={value}");
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
fn append_gcloud(action: &Action, app: &'static str) {
    let id = &format!("{}", action.id);
    let time = action.date;
    let severity = severity(action);
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
            context: &action.context,
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

const fn severity(action: &Action) -> &'static str {
    if let Some(error) = &action.error {
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
    ref_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_message: Option<&'a str>,
    context: &'a HashMap<&'static str, String>,
    stats: &'a HashMap<Cow<'static, str>, u64>,
    #[serde(rename = "logging.googleapis.com/labels")]
    label: LogLabel,
    #[serde(rename = "logging.googleapis.com/trace")]
    trace_id: &'a str,
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
