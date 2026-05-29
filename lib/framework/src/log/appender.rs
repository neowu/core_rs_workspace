use std::borrow::Cow;
use std::io;
use std::io::BufWriter;
use std::io::Write as _;
use std::sync::OnceLock;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use indexmap::IndexMap;
use serde::Serialize;

use crate::exception::Severity;
use crate::json;
use crate::log::Action;
use crate::write_str;

pub(crate) static APPENDER: OnceLock<Appender> = OnceLock::new();

pub enum Appender {
    Console,
    GoogleCloud,
}

impl Appender {
    // appender must not emit log event!(), it could trigger layer on_event, make CURRENT_ACTION.borrow_mut() panic
    pub(super) fn append_action(&self, action: &Action) {
        match self {
            Appender::Console => append_console(action),
            Appender::GoogleCloud => append_gcloud(action),
        }
    }
}

fn append_console(action: &Action) {
    let date = action.date.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let severity = severity(action);
    let kind = action.kind;
    let id = &action.id;
    let mut log = format!("{date} | {severity} | {kind} | id={id}");

    if let Some(ref error_code) = action.error_code {
        write_str!(&mut log, " | error_code={error_code}");
    }

    if let Some(ref error_message) = action.error_message {
        write_str!(&mut log, " | error_message={error_message}");
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

    writeln!(io::stdout(), "{log}").expect("write to stdout cannot fail");

    if action.flush_trace() {
        let mut stderr = BufWriter::with_capacity(32 * 1024, io::stderr().lock());
        for line in &action.logs {
            writeln!(stderr, "{line}").expect("write to stderr cannot fail");
        }
    }
}

fn append_gcloud(action: &Action) {
    let id = &action.id;
    let time = action.date;
    let severity = severity(action);

    writeln!(
        io::stdout(),
        "{}",
        json::to_json(&ActionEntry {
            id,
            time,
            app: action.app,
            kind: action.kind,
            severity,
            ref_id: action.ref_id.as_deref(),
            error_code: action.error_code.as_deref(),
            error_message: action.error_message.as_deref(),
            context: &action.context,
            stats: &action.stats,
            label: LogLabel { log: "action" },
            trace_id: id,
        })
        .expect("serialize to json cannot fail")
    )
    .expect("write to stdout cannot fail");

    if action.flush_trace() {
        let mut stdout = BufWriter::with_capacity(32 * 1024, io::stdout().lock());
        for line in &action.logs {
            writeln!(
                stdout,
                "{}",
                json::to_json(&TraceEntry {
                    id,
                    time,
                    app: action.app,
                    severity,
                    message: line,
                    label: LogLabel { log: "trace" },
                    trace_id: id,
                })
                .expect("serialize to json cannot fail")
            )
            .expect("write to stdout cannot fail");
        }
    }
}

const fn severity(action: &Action) -> &'static str {
    match action.severity {
        Some(Severity::Warn) => "WARN",
        Some(Severity::Error) => "ERROR",
        None => "INFO",
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
    app: &'static str,
    kind: &'a str,
    severity: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    ref_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_message: Option<&'a str>,
    context: &'a IndexMap<&'static str, String>,
    stats: &'a IndexMap<Cow<'static, str>, u64>,
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
