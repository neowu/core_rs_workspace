use serde::Deserialize;
use serde::Serialize;

use crate::log::Severity;
use crate::metrics::Metrics;
use crate::system::CONTEXT;
use crate::time::DateTime;

pub trait MetricsAppender: Send + 'static {
    fn append(&self, metrics: MetricsMessage) -> impl Future<Output = ()> + Send;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetricsMessage {
    pub id: String,
    pub timestamp: DateTime,
    pub app: String,
    pub host: String,
    pub severity: Severity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub stats: Vec<(String, u64)>,
    pub info: Vec<(String, String)>,
}

impl From<Metrics> for MetricsMessage {
    fn from(metrics: Metrics) -> Self {
        let (error_code, error_message) = match metrics.error {
            Some(error) => (error.code.map(str::to_owned), Some(error.message)),
            None => (None, None),
        };

        let context = CONTEXT.get().expect("context must be initialized");

        MetricsMessage {
            id: metrics.id,
            timestamp: metrics.timestamp,
            app: context.app.to_owned(),
            host: context.host.clone(),
            severity: metrics.severity,
            error_code,
            error_message,
            stats: metrics.stats.into_iter().map(|(key, value)| (key.to_owned(), value)).collect(),
            info: metrics.info.into_iter().map(|(key, value)| (key.to_owned(), value)).collect(),
        }
    }
}
