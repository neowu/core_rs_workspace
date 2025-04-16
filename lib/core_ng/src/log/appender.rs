use std::time::Duration;

use super::ActionLogAppender;
use super::ActionLogMessage;
use super::ActionResult;
use crate::json;

pub struct ConsoleAppender;

impl ActionLogAppender for ConsoleAppender {
    fn append(&self, action_log: ActionLogMessage) {
        let mut log = format!(
            "{} | {} | {} | id={} | elapsed={:?}",
            action_log.date.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            json::to_json_value(&action_log.result),
            action_log.action,
            action_log.id,
            Duration::from_nanos(action_log.elapsed as u64),
        );

        if let Some(ref_id) = action_log.ref_id {
            log.push_str(&format!(" | ref_id={ref_id}"));
        }

        for (key, value) in action_log.context {
            log.push_str(&format!(" | {key}={value}"));
        }

        println!("{log}");

        if action_log.result != ActionResult::Ok {
            if let Some(trace) = action_log.trace {
                eprintln!("{trace}");
            }
        }
    }
}
