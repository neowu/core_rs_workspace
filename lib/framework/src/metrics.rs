use std::borrow::Cow;

use crate::appender::CONTEXT;
use crate::appender::MetricsMessage;
use crate::exception::Severity;
use crate::time::DateTime;

mod collector;
mod counter;

pub(crate) use collector::MetricsCollector;
pub use counter::Counter;
pub use counter::CounterGuard;

pub struct Metrics {
    id: String,
    timestamp: DateTime,
    severity: Severity,
    error: Option<Error>,
    // keys are borrowed so the move into MetricsMessage allocates nothing, add_stat/add_info keep them static
    stats: Vec<(Cow<'static, str>, u64)>,
    info: Vec<(Cow<'static, str>, String)>,
}

struct Error {
    code: Option<&'static str>,
    message: String,
}

impl Metrics {
    pub fn add_stat(&mut self, key: &'static str, value: u64) {
        self.stats.push((Cow::Borrowed(key), value));
    }

    fn add_info(&mut self, key: &'static str, value: String) {
        self.info.push((Cow::Borrowed(key), value));
    }

    fn update_error(&mut self, severity: Severity, error_code: &'static str, error_message: String) {
        if self.error.as_ref().is_none() || self.severity < severity {
            self.severity = severity;
            self.error = Some(Error { code: Some(error_code), message: error_message });
        }
    }
}

impl From<Metrics> for MetricsMessage {
    fn from(metrics: Metrics) -> MetricsMessage {
        let (error_code, error_message) = match metrics.error {
            Some(error) => (error.code.map(str::to_owned), Some(error.message)),
            None => (None, None),
        };

        let context = CONTEXT.get().expect("context must be initialized");

        MetricsMessage {
            id: metrics.id,
            timestamp: metrics.timestamp,
            app: Cow::Borrowed(context.app),
            host: Cow::Borrowed(&context.host),
            severity: metrics.severity,
            error_code,
            error_message,
            stats: metrics.stats,
            info: metrics.info,
        }
    }
}
