use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Instant;

use crate::exception::Exception;
use crate::log::Severity;
use crate::log::elapsed;
use crate::log::truncate;
use crate::time::DateTime;
use crate::write_str;

pub struct Action {
    pub(crate) start_time: Instant,
    pub id: String,
    pub(crate) kind: &'static str,
    pub(crate) timestamp: DateTime,
    pub app: &'static str,
    pub(crate) host: &'static str,
    pub(crate) ref_id: Option<Vec<String>>,
    pub severity: Severity,
    pub(crate) error: Option<Error>,
    pub(crate) context: Vec<(&'static str, Vec<String>)>,
    pub(crate) stats: HashMap<Cow<'static, str>, u64>,
    pub(crate) logs: Vec<String>,
    pub(crate) trace: bool,
}

pub struct Error {
    pub code: Option<&'static str>,
    pub message: String,
}

impl Action {
    pub(crate) fn new(
        id: String,
        kind: &'static str,
        ref_id: Option<Vec<String>>,
        timestamp: DateTime,
        app: &'static str,
        host: &'static str,
    ) -> Self {
        let mut action = Action {
            start_time: Instant::now(),
            id,
            kind,
            timestamp,
            app,
            host,
            ref_id,
            severity: Severity::Info,
            error: None,
            context: Vec::new(),
            stats: HashMap::new(),
            logs: Vec::with_capacity(32),
            trace: false,
        };

        action.logs.push(format!(
            "# [action] id={}, kind={kind}, date={}, app={app}, host={host}, ref_id={:?}",
            action.id,
            action.timestamp.to_rfc3339(),
            action.ref_id
        ));

        action
    }

    pub(crate) const fn flush_trace(&self) -> bool {
        self.error.is_some() || self.trace
    }

    pub(crate) fn log(&mut self, message: &str, location: &'static str) {
        const MAX_LOGS: usize = 2000;
        if self.logs.len() >= MAX_LOGS {
            return;
        }

        let mut log = String::with_capacity(256);
        let (minutes, seconds, nanos) = elapsed(self.start_time);
        write_str!(log, "{minutes:02}:{seconds:02}.{nanos:09} {location} {message}");
        self.logs.push(log);
    }

    pub(crate) fn log_with_severity(
        &mut self,
        message: &str,
        severity: Option<Severity>,
        error_code: Option<&'static str>,
        location: &'static str,
    ) {
        const MAX_LOGS: usize = 2000;
        if self.logs.len() >= MAX_LOGS {
            return;
        }

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
        self.logs.push(log);

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
        self.logs.push(log);

        self.update_error(exception.severity, exception.code, &exception.message);
    }

    pub(crate) fn finish(&mut self) {
        let elapsed = self.start_time.elapsed();
        self.stats.insert(Cow::Borrowed("elapsed"), elapsed.as_nanos() as u64);
        if self.flush_trace() {
            self.logs.push(format!("# [action] elapsed={elapsed:?}"));
        }
    }
}
