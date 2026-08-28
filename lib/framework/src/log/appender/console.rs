use std::time::Duration;

use crate::log::appender::ActionAppender;
use crate::log::appender::ActionMessage;
use crate::metrics::MetricsAppender;
use crate::metrics::MetricsMessage;
use crate::write_str;

pub struct ConsoleAppender;

impl ActionAppender for ConsoleAppender {
    async fn append(&self, action: ActionMessage) {
        write_action(action);
    }
}

impl MetricsAppender for ConsoleAppender {
    async fn append(&self, metrics: MetricsMessage) {
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
        println!("{logs}");
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
