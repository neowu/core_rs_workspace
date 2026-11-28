use framework::exception;
use framework_macro::Enum8;

pub(crate) mod action_handler;
pub(crate) mod metrics_handler;

// Enum8('INFO' = 1, 'WARN' = 2, 'ERROR' = 3), mirrors framework::exception::Severity discriminants,
// the message carries the name ("INFO"), clickhouse RowBinary carries the i8
#[derive(Enum8)]
enum Severity {
    Info = 1,
    Warn = 2,
    Error = 3,
}

impl From<exception::Severity> for Severity {
    fn from(severity: exception::Severity) -> Severity {
        match severity {
            exception::Severity::Info => Severity::Info,
            exception::Severity::Warn => Severity::Warn,
            exception::Severity::Error => Severity::Error,
        }
    }
}
