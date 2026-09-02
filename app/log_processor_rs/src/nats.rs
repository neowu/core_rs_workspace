use framework::log;
use framework_clickhouse::Enum8;

pub(crate) mod action_handler;
pub(crate) mod metrics_handler;

// Enum8('INFO' = 1, 'WARN' = 2, 'ERROR' = 3), mirrors framework::log::Severity discriminants,
// the message carries the name ("INFO"), clickhouse RowBinary carries the i8
#[derive(Enum8)]
pub(crate) enum Severity {
    Info = 1,
    Warn = 2,
    Error = 3,
}

impl From<log::Severity> for Severity {
    fn from(severity: log::Severity) -> Self {
        match severity {
            log::Severity::Info => Severity::Info,
            log::Severity::Warn => Severity::Warn,
            log::Severity::Error => Severity::Error,
        }
    }
}
