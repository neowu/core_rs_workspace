use std::io;
use std::io::Write as _;
use std::time::Duration;

use super::ActionLogAppender;
use super::ActionLogMessage;
use super::ActionResult;
use crate::json;
use crate::write_str;

pub struct ConsoleAppender;

impl ActionLogAppender for ConsoleAppender {
    fn append(&self, action_log: ActionLogMessage) {
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

        io::stdout().write_all(log.as_bytes()).expect("write to stdout cannot fail");

        if action_log.result != ActionResult::Ok
            && let Some(trace) = action_log.trace
        {
            io::stderr().write_all(trace.as_bytes()).expect("write to stderr cannot fail");
        }
    }
}
