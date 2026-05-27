use std::io;
use std::io::Write as _;
use std::sync::OnceLock;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use indexmap::IndexMap;
use serde::Serialize;

use crate::exception::Exception;
use crate::exception::Severity;
use crate::json;
use crate::log::Action;
use crate::write_str;

pub(crate) static APPENDER: OnceLock<ActionAppender> = OnceLock::new();

pub enum ActionAppender {
    Console,
    GoogleCloud,
}

impl ActionAppender {
    pub(super) fn append(&self, action: &Action) {
        match self {
            ActionAppender::Console => append_console(action),
            ActionAppender::GoogleCloud => {
                if let Err(err) = append_gcloud(action) {
                    err.log();
                }
            }
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
            write_str!(&mut log, " | {key}={:?}", Duration::from_nanos(u64::try_from(*value).unwrap_or(0)));
        } else {
            write_str!(&mut log, " | {key}={value}");
        }
    }

    writeln!(io::stdout(), "{log}").expect("write to stdout cannot fail");

    if action.severity.is_some() {
        writeln!(io::stderr(), "{}", action.logs.join("\n")).expect("write to stderr cannot fail");
    }
}

fn append_gcloud(action: &Action) -> Result<(), Exception> {
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
        })?
    )?;

    if action.severity.is_some() {
        for line in &action.logs {
            writeln!(
                io::stdout(),
                "{}",
                json::to_json(&TraceEntry {
                    id,
                    time,
                    app: action.app,
                    severity,
                    message: line,
                    label: LogLabel { log: "trace" },
                    trace_id: id,
                })?
            )?;
        }
    }
    Ok(())
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
    stats: &'a IndexMap<String, u128>,
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
