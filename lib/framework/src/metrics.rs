use std::borrow::Cow;

use crate::log::Severity;
use crate::time::DateTime;

mod collector;
mod counter;

pub(crate) use collector::MetricsCollector;
pub use counter::Counter;
pub use counter::CounterGuard;

pub struct Metrics {
    pub id: String,
    pub timestamp: DateTime,
    pub severity: Severity,
    pub error: Option<Error>,
    // keys are borrowed so the move into MetricsMessage allocates nothing, add_stat/add_info keep them static
    pub(crate) stats: Vec<(Cow<'static, str>, u64)>,
    pub(crate) info: Vec<(Cow<'static, str>, String)>,
}

pub struct Error {
    pub code: Option<&'static str>,
    pub message: String,
}

impl Metrics {
    pub fn add_stat(&mut self, key: &'static str, value: u64) {
        self.stats.push((Cow::Borrowed(key), value));
    }

    pub fn add_info(&mut self, key: &'static str, value: String) {
        self.info.push((Cow::Borrowed(key), value));
    }

    fn update_error(&mut self, severity: Severity, error_code: &'static str, error_message: String) {
        if self.error.as_ref().is_none() || self.severity < severity {
            self.severity = severity;
            self.error = Some(Error { code: Some(error_code), message: error_message });
        }
    }
}
