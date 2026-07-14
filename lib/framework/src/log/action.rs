use std::borrow::Cow;
use std::collections::HashMap;
use std::thread;
use std::time::Instant;

use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;

use crate::exception::Exception;
use crate::exception::Severity;
use crate::log::elapsed;
use crate::log::id_generator::LogId;
use crate::log::truncate;
use crate::network::hostname;
use crate::write_str;

pub(crate) struct Action {
    pub(crate) start_time: Instant,
    pub(crate) id: LogId,
    pub(crate) kind: &'static str,
    pub(crate) date: DateTime<Utc>,
    pub(crate) ref_id: Option<Vec<String>>,
    pub(crate) error: Option<Error>,
    pub(crate) context: Vec<(&'static str, Vec<String>)>,
    pub(crate) stats: HashMap<Cow<'static, str>, u64>,
    pub(crate) logs: Vec<String>,
}

pub struct Error {
    pub severity: Severity,
    pub code: Option<&'static str>,
    pub message: String,
}

impl Action {
    pub(crate) fn new(id: LogId, kind: &'static str, ref_id: Option<Vec<String>>, date: DateTime<Utc>) -> Self {
        let mut action = Action {
            start_time: Instant::now(),
            id,
            kind,
            date,
            ref_id,
            error: None,
            context: Vec::new(),
            stats: HashMap::new(),
            logs: Vec::with_capacity(32),
        };

        let date_string = action.date.to_rfc3339_opts(SecondsFormat::Nanos, true);

        action.logs.push(format!(
            "# [action] id={}, date={date_string}, kind={kind}\nthread={:?}\nhost={}\nref_id={:?}",
            action.id,
            thread::current().id(),
            hostname(),
            action.ref_id,
        ));

        action
    }

    pub(crate) const fn flush_trace(&self) -> bool {
        self.error.is_some()
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
        if self.error.as_ref().is_none_or(|error| error.severity < severity) {
            self.error = Some(Error {
                severity,
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
