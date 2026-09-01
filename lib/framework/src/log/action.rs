use std::fmt;
use std::time::Instant;

use crate::exception::Exception;
use crate::log::Severity;
use crate::log::elapsed;
use crate::string::StringExt as _;
use crate::time::DateTime;
use crate::write_str;

const MAX_LOG_BYTES: usize = 512 * 1024; // soft limitation, framework may still write few lines after hits limit
const MAX_LOG_MESSAGE_LEN: usize = 10_000;
const MAX_ERROR_MESSAGE_LEN: usize = 200;

pub(crate) struct Action {
    pub start_time: Instant,
    pub id: String,
    pub kind: &'static str,
    pub timestamp: DateTime,
    pub ref_ids: Option<Vec<String>>,
    pub severity: Severity,
    pub error: Option<Error>,
    pub context: Vec<(&'static str, Vec<String>)>,
    pub stats: Vec<(&'static str, u64)>,
    pub logs: String, // All log lines in one buffer, separated by '\n'
    pub trace: bool,
}

pub(crate) struct Error {
    pub code: Option<&'static str>,
    pub message: String,
}

impl Action {
    pub(crate) fn new(id: String, kind: &'static str, ref_ids: Option<Vec<String>>, timestamp: DateTime) -> Self {
        let mut logs = String::with_capacity(1024);
        write_str!(logs, "# [action] id={id}, kind={kind}, date={}, ref_id={ref_ids:?}\n", timestamp.to_rfc3339());

        Action {
            start_time: Instant::now(),
            id,
            kind,
            timestamp,
            ref_ids,
            severity: Severity::Info,
            error: None,
            context: Vec::with_capacity(8),
            // slot 0 is reserved so elapsed always leads the stats and the vec allocates exactly once
            stats: {
                let mut stats = Vec::with_capacity(16);
                stats.push(("elapsed", 0));
                stats
            },
            logs,
            trace: false,
        }
    }

    pub(crate) const fn flush_trace(&self) -> bool {
        self.error.is_some() || self.trace
    }

    /// Accumulates into an existing key or appends it. Keys come from stringify!/concat! and an action
    /// carries at most ~20, so a linear scan beats hashing a string on every write.
    pub(crate) fn add_stat(&mut self, key: &'static str, value: u64) {
        if let Some(entry) = self.stats.iter_mut().find(|(existing, _)| *existing == key) {
            entry.1 += value;
        } else {
            self.stats.push((key, value));
        }
    }

    /// The only way to append a prefixed log line, promotes the action severity when one is given.
    pub(crate) fn log(
        &mut self,
        severity: Option<Severity>,
        error_code: Option<&'static str>,
        location: Option<&'static str>,
        message: fmt::Arguments<'_>,
    ) {
        self.log_line(severity, error_code, location, message, MAX_LOG_BYTES, MAX_LOG_MESSAGE_LEN);

        if let Some(severity) = severity
            && severity >= Severity::Warn
        {
            self.update_error(severity, error_code, &message.to_string());
        }
    }

    pub(crate) fn log_exception(&mut self, exception: &Exception) {
        self.log_line(
            Some(exception.severity),
            exception.code,
            exception.location,
            format_args!("{}\n{}", exception.message, exception.backtrace()),
            MAX_LOG_BYTES,
            MAX_LOG_MESSAGE_LEN,
        );

        // the line carries the backtrace too, so the error message comes from the exception itself
        self.update_error(exception.severity, exception.code, &exception.message);
    }

    pub(crate) fn finish(&mut self) {
        let elapsed = self.start_time.elapsed();
        self.add_stat("elapsed", elapsed.as_nanos() as u64);
        if self.flush_trace() {
            let message = format_args!("# [action] elapsed={elapsed:?}");
            write_str!(self.logs, "{message}");
        }
    }

    // TODO: always log if severity == ERROR
    fn log_line(
        &mut self,
        severity: Option<Severity>,
        error_code: Option<&'static str>,
        location: Option<&'static str>,
        message: fmt::Arguments<'_>,
        max_log_bytes: usize,
        max_message_len: usize,
    ) {
        if self.logs.len() >= max_log_bytes {
            return;
        }

        let (minutes, seconds, nanos) = elapsed(self.start_time);
        let logs = &mut self.logs;
        write_str!(logs, "{minutes:02}:{seconds:02}.{nanos:09} ");
        if let Some(location) = location {
            write_str!(logs, "{location} ");
        }
        if let Some(severity) = severity {
            write_str!(logs, "{severity} ");
        }
        if let Some(error_code) = error_code {
            write_str!(logs, "[{error_code}] ");
        }

        let start = logs.len();
        write_str!(logs, "{message}\n");
        if logs.len() - start > max_message_len {
            let end = start + logs[start..].truncate_to_max(max_message_len).len();
            logs.truncate(end); // end is a char boundary by construction
            logs.push_str("...(truncated)\n");
        }

        if logs.len() >= max_log_bytes {
            logs.push_str("...(log limit reached)\n");
        }
    }

    fn update_error(&mut self, severity: Severity, error_code: Option<&'static str>, error_message: &str) {
        if self.error.is_none() || self.severity < severity {
            self.severity = severity;
            self.error = Some(Error {
                code: error_code,
                message: error_message.truncate_to_max(MAX_ERROR_MESSAGE_LEN).to_owned(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Action;
    use super::MAX_LOG_BYTES;
    use crate::log::Severity;
    use crate::time::DateTime;

    fn action() -> Action {
        Action::new("id".to_owned(), "test", None, DateTime::now())
    }

    #[test]
    fn log_line_truncates_message() {
        let mut action = action();
        // 3 bytes per char, so the 100 byte cap lands mid char and walks back to 99
        action.log_line(None, None, Some("location"), format_args!("{}", "老".repeat(10)), MAX_LOG_BYTES, 10);
        assert!(action.logs.ends_with("...(truncated)\n"));
        let message = action.logs.rsplit("location ").next().expect("message must be present");
        assert_eq!(message.len(), 9 + "...(truncated)\n".len());
    }

    #[test]
    fn log_line_stops_at_max_log_bytes() {
        let mut action = action();
        let max_log_bytes = action.logs.len() + 1; // the next line overshoots the limit

        action.log_line(None, None, Some("location"), format_args!("first"), max_log_bytes, 100);
        assert!(action.logs.contains("location first\n"));
        assert!(action.logs.ends_with("...(log limit reached)\n"));

        let logs = action.logs.clone();
        action.log_line(None, None, Some("location"), format_args!("second"), max_log_bytes, 100);
        assert_eq!(action.logs, logs); // dropped, the limit was already reached
    }

    #[test]
    fn log_error() {
        let mut action = action();
        action.log(Some(Severity::Error), Some("CODE"), Some("location"), format_args!("boom"));

        assert_eq!(action.severity, Severity::Error);
        let error = action.error.as_ref().expect("error must be set");
        assert_eq!(error.code, Some("CODE"));
        assert_eq!(error.message, "boom");
        assert!(action.logs.ends_with("location ERROR [CODE] boom\n"));
    }

    #[test]
    fn update_error_keeps_the_highest_severity() {
        let mut action = action();
        action.log(Some(Severity::Error), Some("FIRST"), Some("location"), format_args!("first"));
        action.log(Some(Severity::Warn), Some("SECOND"), Some("location"), format_args!("second"));

        assert_eq!(action.severity, Severity::Error);
        assert_eq!(action.error.as_ref().and_then(|error| error.code), Some("FIRST"));
    }
}
