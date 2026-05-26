use std::io;
use std::io::Write as _;
use std::sync::OnceLock;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use indexmap::IndexMap;
use serde::Serialize;

use super::ActionResult;
use crate::json;
use crate::log::ActionLog;
use crate::write_str;

pub(crate) static APPENDER: OnceLock<ActionLogAppender> = OnceLock::new();

pub enum ActionLogAppender {
    Console,
    GoogleCloud,
}

impl ActionLogAppender {
    pub(super) fn append(&self, action_log: ActionLog) {
        match self {
            ActionLogAppender::Console => append_console(action_log),
            ActionLogAppender::GoogleCloud => append_gcloud(&action_log),
        }
    }
}

fn append_console(action_log: ActionLog) {
    let mut log = format!(
        "{} | {} | {} | id={}",
        action_log.date.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        json::to_json_value(&action_log.result),
        action_log.action,
        action_log.id
    );

    if let Some(error_code) = action_log.error_code {
        write_str!(&mut log, " | error_code={error_code}");
    }

    if let Some(error_message) = action_log.error_message {
        write_str!(&mut log, " | error_message={error_message}");
    }

    if let Some(ref_id) = action_log.ref_id {
        write_str!(&mut log, " | ref_id={ref_id}");
    }

    for (key, value) in action_log.context {
        write_str!(&mut log, " | {key}={value}");
    }

    for (key, value) in action_log.stats {
        if key.ends_with("elapsed") {
            write_str!(&mut log, " | {key}={:?}", Duration::from_nanos(u64::try_from(value).unwrap_or(0)));
        } else {
            write_str!(&mut log, " | {key}={value}");
        }
    }

    writeln!(io::stdout(), "{log}").expect("write to stdout cannot fail");

    if action_log.result != ActionResult::Ok {
        writeln!(io::stderr(), "{}", action_log.logs.join("\n")).expect("write to stderr cannot fail");
    }
}

fn append_gcloud(action_log: &ActionLog) {
    let severity = severity(&action_log.result);
    let action_entry = ActionLogEntry {
        id: &action_log.id,
        time: action_log.date,
        app: action_log.app,
        action: &action_log.action,
        severity,
        ref_id: action_log.ref_id.as_deref(),
        error_code: action_log.error_code.as_deref(),
        error_message: action_log.error_message.as_deref(),
        context: &action_log.context,
        stats: &action_log.stats,
        labels: Labels { log: "action" },
        trace_id: &action_log.id,
    };
    let json = serde_json::to_string(&action_entry).expect("serialize action log cannot fail");
    writeln!(io::stdout(), "{json}").expect("write to stdout cannot fail");

    if action_log.result != ActionResult::Ok {
        for line in &action_log.logs {
            let trace_entry = TraceEntry {
                message: line,
                severity,
                id: &action_log.id,
                labels: Labels { log: "trace" },
                trace_id: &action_log.id,
            };
            let trace_json = serde_json::to_string(&trace_entry).expect("serialize trace log cannot fail");
            writeln!(io::stdout(), "{trace_json}").expect("write to stdout cannot fail");
        }
    }
}

const fn severity(result: &ActionResult) -> &'static str {
    match *result {
        ActionResult::Ok => "INFO",
        ActionResult::Warn => "WARNING",
        ActionResult::Error => "ERROR",
    }
}

#[derive(Serialize)]
struct Labels {
    log: &'static str,
}

#[derive(Serialize)]
struct ActionLogEntry<'a> {
    id: &'a str,
    time: DateTime<Utc>,
    app: &'static str,
    action: &'a str,
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
    labels: Labels,
    #[serde(rename = "logging.googleapis.com/trace")]
    trace_id: &'a str,
}

#[derive(Serialize)]
struct TraceEntry<'a> {
    message: &'a str,
    severity: &'static str,
    id: &'a str,
    #[serde(rename = "logging.googleapis.com/labels")]
    labels: Labels,
    #[serde(rename = "logging.googleapis.com/trace")]
    trace_id: &'a str,
}
