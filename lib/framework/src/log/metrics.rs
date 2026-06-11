use chrono::DateTime;
use chrono::Utc;

use crate::log::action::Error;
use crate::log::id_generator;
use crate::log::id_generator::LogId;

pub(crate) struct Metrics {
    pub(crate) id: LogId,
    pub(crate) date: DateTime<Utc>,
    pub(crate) error: Option<Error>,
    pub(crate) stats: Vec<(&'static str, u64)>,
    pub(crate) info: Vec<(&'static str, String)>,
}

pub(crate) fn collect_metrics() -> Metrics {
    let date = Utc::now();
    let mut metrics = Metrics {
        id: id_generator::next_id(date.timestamp_millis()),
        date,
        error: None,
        stats: Vec::new(),
        info: Vec::new(),
    };

    collect_vm_info(&mut metrics);

    metrics
}

fn collect_vm_info(metrics: &mut Metrics) {}
