use crate::log::Severity;
use crate::time::DateTime;

pub mod appender;
mod collector;
mod counter;

pub use appender::MetricsAppender;
pub use appender::MetricsMessage;
pub use collector::MetricsCollector;
pub use counter::Counter;
pub use counter::CounterGuard;

pub struct Metrics {
    pub id: String,
    pub timestamp: DateTime,
    pub severity: Severity,
    pub error: Option<Error>,
    pub stats: Vec<(&'static str, u64)>,
    pub info: Vec<(&'static str, String)>,
}

pub struct Error {
    pub code: Option<&'static str>,
    pub message: String,
}

impl Metrics {
    fn update_error(&mut self, severity: Severity, error_code: &'static str, error_message: String) {
        if self.error.as_ref().is_none() || self.severity < severity {
            self.severity = severity;
            self.error = Some(Error { code: Some(error_code), message: error_message });
        }
    }
}
