use std::collections::HashMap;
use std::time::Instant;

use crate::exception::Exception;
use crate::log::Severity;
use crate::log::elapsed;
use crate::log::truncate;
use crate::time::DateTime;
use crate::write_str;

const MAX_LOGS: usize = 2000;

pub(crate) struct Action {
    pub start_time: Instant,
    pub id: String,
    pub kind: &'static str,
    pub timestamp: DateTime,
    pub ref_ids: Option<Vec<String>>,
    pub severity: Severity,
    pub error: Option<Error>,
    pub context: Vec<(String, Vec<String>)>,
    pub stats: HashMap<String, u64>,
    pub logs: Vec<String>,
    pub trace: bool,
}

pub(crate) struct Error {
    pub code: Option<&'static str>,
    pub message: String,
}

impl Action {
    pub(crate) fn new(id: String, kind: &'static str, ref_ids: Option<Vec<String>>, timestamp: DateTime) -> Self {
        let mut action = Action {
            start_time: Instant::now(),
            id,
            kind,
            timestamp,
            ref_ids,
            severity: Severity::Info,
            error: None,
            context: Vec::new(),
            stats: HashMap::new(),
            logs: Vec::with_capacity(32),
            trace: false,
        };

        action.push_log(format!(
            "# [action] id={}, kind={kind}, date={}, ref_id={:?}",
            action.id,
            action.timestamp.to_rfc3339(),
            action.ref_ids
        ));

        action
    }

    pub(crate) const fn flush_trace(&self) -> bool {
        self.error.is_some() || self.trace
    }

    /// The only way to append a log line, enforces MAX_LOGS for every writer.
    pub(crate) fn push_log(&mut self, log: String) {
        if self.logs.len() >= MAX_LOGS {
            return;
        }
        if self.logs.len() == MAX_LOGS - 1 {
            self.logs.push("...(truncated)".to_owned());
            return;
        }
        self.logs.push(log);
    }

    pub(crate) fn log(&mut self, message: &str, location: &'static str) {
        let mut log = String::with_capacity(256);
        let (minutes, seconds, nanos) = elapsed(self.start_time);
        write_str!(log, "{minutes:02}:{seconds:02}.{nanos:09} {location} {message}");
        self.push_log(log);
    }

    pub(crate) fn log_with_severity(
        &mut self,
        message: &str,
        severity: Option<Severity>,
        error_code: Option<&'static str>,
        location: &'static str,
    ) {
        let mut log = String::with_capacity(256);
        let (minutes, seconds, nanos) = elapsed(self.start_time);
        write_str!(log, "{minutes:02}:{seconds:02}.{nanos:09} {location} ");
        if let Some(severity) = severity {
            write_str!(log, "{} ", severity);
        }
        if let Some(error_code) = error_code {
            write_str!(log, "[{error_code}] ");
        }
        write_str!(log, "{message}");
        self.push_log(log);

        if let Some(severity) = severity {
            self.update_error(severity, error_code, message);
        }
    }

    fn update_error(&mut self, severity: Severity, error_code: Option<&'static str>, error_message: &str) {
        const MAX_ERROR_MESSAGE_LEN: usize = 200;
        if self.error.as_ref().is_none() || self.severity < severity {
            self.severity = severity;
            self.error = Some(Error {
                code: error_code,
                message: truncate(error_message.to_owned(), MAX_ERROR_MESSAGE_LEN, None),
            });
        }
    }

    pub(crate) fn log_exception(&mut self, exception: &Exception) {
        let (minutes, seconds, nanos) = elapsed(self.start_time);
        let mut log = String::with_capacity(256);
        write_str!(log, "{minutes:02}:{seconds:02}.{nanos:09} ");
        if let Some(location) = exception.location {
            write_str!(log, "{location} ");
        }
        write_str!(log, "{} ", exception.severity);
        if let Some(error_code) = exception.code {
            write_str!(log, "[{error_code}] ");
        }
        write_str!(log, "{}\n{}", exception.message, exception.backtrace());
        self.push_log(log);

        self.update_error(exception.severity, exception.code, &exception.message);
    }

    pub(crate) fn finish(&mut self) {
        let elapsed = self.start_time.elapsed();
        self.stats.insert("elapsed".to_owned(), elapsed.as_nanos() as u64);
        if self.flush_trace() {
            self.push_log(format!("# [action] elapsed={elapsed:?}"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Action;
    use super::MAX_LOGS;
    use crate::time::DateTime;

    fn action() -> Action {
        Action::new("id".to_owned(), "test", None, DateTime::now())
    }

    #[test]
    fn push_log_below_max_logs() {
        let mut action = action();
        action.log("message", "location");
        // the header line pushed by new(), plus ours
        assert_eq!(action.logs.len(), 2);
        assert!(action.logs[1].ends_with("location message"));
    }

    #[test]
    fn push_log_stops_at_max_logs() {
        let mut action = action();
        for i in 0..MAX_LOGS * 2 {
            action.log(&format!("message {i}"), "location");
        }
        assert_eq!(action.logs.len(), MAX_LOGS);
        assert_eq!(action.logs.last().map(String::as_str), Some("...(truncated)"));
    }

    #[test]
    fn finish_respects_max_logs() {
        let mut action = action();
        action.trace = true;
        for i in 0..MAX_LOGS * 2 {
            action.log(&format!("message {i}"), "location");
        }
        action.finish();

        // the elapsed footer is dropped once capped, but the value is still in stats
        assert_eq!(action.logs.len(), MAX_LOGS);
        assert_eq!(action.logs.last().map(String::as_str), Some("...(truncated)"));
        assert!(action.stats.contains_key("elapsed"));
    }
}
