use std::borrow::Cow;
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

use crate::exception::Severity;
use crate::log::CURRENT_ACTION;
use crate::log::elapsed;
use crate::log::truncate;
use crate::write_str;

pub(crate) struct ActionLogLayer;

const MAX_LOG_MESSAGE_LEN: usize = 10_000;

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
                let span_elapsed = span_extension.start_time.elapsed();
                action.logs.push(format!(
                    "[span:{span_name}] {minutes:02}:{seconds:02}.{nanos:09} elapsed={span_elapsed:?} <"
                ));

                let total_elapsed = action.stats.entry(Cow::Owned(format!("{span_name}_elapsed"))).or_default();
                *total_elapsed += span_elapsed.as_nanos() as u64;

                let count = action.stats.entry(Cow::Owned(format!("{span_name}_count"))).or_default();
                *count += 1;
            });
        }
    }

    fn on_record(&self, id: &Id, values: &Record, context: Context<S>) {
        let span = context.span(id).expect("span must exist");
        let span_name = span.name();
        let _result = CURRENT_ACTION.try_with(|action| {
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
        const MAX_ERROR_MESSAGE_LEN: usize = 200;
        if field.name() == "message" {
            let message = format!("{value:?}");
            self.message = Some(truncate(message, MAX_ERROR_MESSAGE_LEN, None));
        }
    }
}
