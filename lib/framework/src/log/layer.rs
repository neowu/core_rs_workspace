use std::fmt::Debug;
use std::time::Instant;

use tracing::Event;
use tracing::Level;
use tracing::Subscriber;
use tracing::field::Field;
use tracing::field::Visit;
use tracing::span::Attributes;
use tracing::span::Id;
use tracing::span::Record;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

use super::CONTEXT;
use super::STATS;
use crate::exception::Severity;
use crate::log::Action;
use crate::log::CURRENT_ACTION;
use crate::write_str;

pub(crate) struct ActionLogLayer;

const MAX_LOG_MESSAGE_LEN: usize = 10_000;
const MAX_CONTEXT_VALUE_LEN: usize = 1_000;
const MAX_ERROR_MESSAGE_LEN: usize = 200;

impl<S> Layer<S> for ActionLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes, id: &Id, context: Context<S>) {
        let span = context.span(id).expect("span must exist");
        let span_name = span.name();

        if span_name != "action" {
            let _result = CURRENT_ACTION.try_with(|action| {
                let mut extensions = span.extensions_mut();
                extensions.insert(SpanExtension { start_time: Instant::now() });
                let mut action = action.borrow_mut();

                let (minutes, seconds, nanos) = elapsed(action.start_time);
                let mut log = format!("[span:{span_name}] {minutes:02}:{seconds:02}.{nanos:09} ");

                let level = span.metadata().level();
                if level <= &Level::INFO {
                    write_str!(log, "{level} ");
                }

                let target = span.metadata().target();
                let line = span.metadata().line().unwrap_or(0);
                write_str!(log, "{target}:{line} ");

                attrs.record(&mut LogVisitor(&mut log));
                log.push('>');
                action.logs.push(log);
            });
        }
    }

    fn on_close(&self, id: Id, context: Context<S>) {
        let span = context.span(&id).expect("span must exist");
        if let Some(span_extension) = span.extensions_mut().remove::<SpanExtension>() {
            let _result = CURRENT_ACTION.try_with(|action| {
                let mut action = action.borrow_mut();

                let span_name = span.name();
                let (minutes, seconds, nanos) = elapsed(action.start_time);
                let mut log = format!("[span:{span_name}] {minutes:02}:{seconds:02}.{nanos:09} ");
                let span_elapsed = span_extension.start_time.elapsed();
                write_str!(log, "elapsed={span_elapsed:?} <");
                action.logs.push(log);

                let total_elapsed = action.stats.entry(format!("{span_name}_elapsed")).or_default();
                *total_elapsed += span_elapsed.as_nanos();

                let count = action.stats.entry(format!("{span_name}_count")).or_default();
                *count += 1;
            });
        }
    }

    fn on_record(&self, id: &Id, values: &Record, context: Context<S>) {
        let _result = CURRENT_ACTION.try_with(|action| {
            let span = context.span(id).expect("span must exist");
            let span_name = span.name();
            let mut action = action.borrow_mut();
            let (minutes, seconds, nanos) = elapsed(action.start_time);
            let mut log = format!("[span:{span_name}] {minutes:02}:{seconds:02}.{nanos:09} ");
            values.record(&mut LogVisitor(&mut log));
            action.logs.push(log);
        });
    }

    fn on_event(&self, event: &Event, _context: Context<S>) {
        if event.metadata().level() == &Level::TRACE {
            return;
        }

        let _result = CURRENT_ACTION.try_with(|action| {
            let mut action = action.borrow_mut();

            let mut log = String::with_capacity(128);

            let (minutes, seconds, nanos) = elapsed(action.start_time);
            write_str!(log, "{minutes:02}:{seconds:02}.{nanos:09} ");

            let level = event.metadata().level();
            if level <= &Level::INFO {
                write_str!(log, "{level} ");
            }

            let target = event.metadata().target();
            let line = event.metadata().line().unwrap_or(0);
            write_str!(log, "{target}:{line} ");

            if level <= &Level::WARN {
                let mut visitor = ErrorVisitor { message: None, code: None };
                event.record(&mut visitor);
                if let Some(ref error_code) = visitor.code {
                    write_str!(log, "[{error_code}] ");
                }

                let target_severity = if level == &Level::ERROR { Severity::Error } else { Severity::Warn };
                if action.severity.is_none_or(|severity| severity > target_severity) {
                    action.severity = Some(target_severity);
                    action.error_code = visitor.code;
                    action.error_message = visitor.message;
                }
            }

            let name = event.metadata().name();
            if name == CONTEXT {
                let mut context_visitor = ContextVisitor { action: &mut action };
                event.record(&mut context_visitor);
                write_str!(log, "[context] ");
            } else if name == STATS {
                let mut stats_visitor = StatsVisitor { action: &mut action };
                event.record(&mut stats_visitor);
                write_str!(log, "[stats] ");
            }

            let mut log_visitor = LogVisitor(&mut log);
            event.record(&mut log_visitor);
            action.logs.push(truncate(log, MAX_LOG_MESSAGE_LEN, Some("...(truncated)")));
        });
    }
}

struct SpanExtension {
    start_time: Instant,
}

struct LogVisitor<'a>(&'a mut String);

impl Visit for LogVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "backtrace" {
            write_str!(self.0, "\n{value}");
        } else if field.name() == "error_code" {
            // do not log error_code here, it is handled in ErrorVisitor
        } else {
            write_str!(self.0, "{}={} ", field.name(), value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            write_str!(self.0, "{value:?} ");
        } else {
            write_str!(self.0, "{}={:?} ", field.name(), value);
        }
    }
}

struct ContextVisitor<'a> {
    action: &'a mut Action,
}

impl Visit for ContextVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        let value = value.to_owned();
        self.action.context.insert(field.name(), truncate(value, MAX_CONTEXT_VALUE_LEN, Some("...(truncated)")));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.action.context.insert(field.name(), format!("{value:?}"));
    }
}

struct StatsVisitor<'a> {
    action: &'a mut Action,
}

impl Visit for StatsVisitor<'_> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.record_u128(field, value as u128);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_u128(field, value as u128);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_u128(field, value as u128);
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.record_u128(field, value as u128);
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        let stats_value = self.action.stats.entry(field.name().to_owned()).or_default();
        *stats_value += value;
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn Debug) {}
}

struct ErrorVisitor {
    code: Option<String>,
    message: Option<String>,
}

impl Visit for ErrorVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "error_code" {
            self.code = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            let message = format!("{value:?}");
            self.message = Some(truncate(message, MAX_ERROR_MESSAGE_LEN, None));
        }
    }
}

fn truncate(mut value: String, len: usize, suffix: Option<&str>) -> String {
    if len >= value.len() {
        return value;
    }

    let mut new_len = len;
    while new_len > 0 && !value.is_char_boundary(new_len) {
        new_len -= 1;
    }

    value.truncate(new_len);
    if let Some(suffix) = suffix {
        value.push_str(suffix);
    }
    value
}

fn elapsed(start: Instant) -> (u64, u64, u32) {
    let elapsed = start.elapsed();
    let total_seconds = elapsed.as_secs();
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    let nanos = elapsed.subsec_nanos();
    (minutes, seconds, nanos)
}

#[cfg(test)]
mod tests {
    use crate::log::layer::truncate;

    #[test]
    fn truncate_with_unicode() {
        let value = "123老虎456".to_owned();
        assert_eq!(truncate(value.clone(), 3, None), "123".to_owned());
        assert_eq!(truncate(value.clone(), 4, None), "123".to_owned());
        assert_eq!(truncate(value.clone(), 5, None), "123".to_owned());
        assert_eq!(truncate(value.clone(), 6, Some("...(truncated)")), "123老...(truncated)".to_owned());
        assert_eq!(truncate(value.clone(), 7, None), "123老".to_owned());
        assert_eq!(truncate(value.clone(), 8, None), "123老".to_owned());
        assert_eq!(truncate(value.clone(), 9, None), "123老虎".to_owned());
        assert_eq!(truncate(value.clone(), 10, Some("...(truncated)")), "123老虎4...(truncated)".to_owned());
    }
}
