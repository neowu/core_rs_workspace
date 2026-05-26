use std::fmt::Debug;
use std::thread;
use std::time::Instant;

use chrono::SecondsFormat;
use chrono::Utc;
use indexmap::IndexMap;
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
use tracing_subscriber::registry::Scope;
use tracing_subscriber::registry::SpanRef;

use super::ActionResult;
use super::CONTEXT;
use super::STATS;
use crate::log::APP;
use crate::log::ActionLog;
use crate::log::appender::APPENDER;
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
        let mut extensions = span.extensions_mut();

        if span.name() == "action" {
            let mut action_visitor = ActionVisitor::new();
            attrs.record(&mut action_visitor);
            if let Some(mut action_log) = action_visitor.action_log()
                && extensions.get_mut::<ActionLog>().is_none()
            {
                action_log.logs.push(format!(
                    "=== action begin ===
type={}
id={}
date={}
thread={:?}",
                    action_log.action,
                    action_log.id,
                    action_log.date.to_rfc3339_opts(SecondsFormat::Nanos, true),
                    thread::current().id()
                ));

                if let Some(ref ref_id) = action_log.ref_id {
                    action_log.logs.push(format!("ref_id={ref_id}"));
                }

                extensions.insert(action_log);
            }
        } else if let Some(action_span) = action_span(context.span_scope(id))
            && let Some(action_log) = action_span.extensions_mut().get_mut::<ActionLog>()
        {
            extensions.insert(SpanExtension { start_time: Instant::now() });

            let mut log_string =
                format!("[span:{}] {}:{} ", span.name(), span.metadata().target(), span.metadata().line().unwrap_or(0));
            attrs.record(&mut LogVisitor(&mut log_string));
            log_string.push_str(">>>");
            action_log.logs.push(log_string);
        }
    }

    fn on_close(&self, id: Id, context: Context<S>) {
        let span = context.span(&id).expect("span must exist");
        if let Some(mut action_log) = span.extensions_mut().remove::<ActionLog>() {
            let elapsed = action_log.start_time.elapsed();
            action_log.stats.insert("elapsed".to_owned(), elapsed.as_nanos());
            if action_log.result.level() > ActionResult::Ok.level() {
                action_log.logs.push(format!(
                    "elapsed={elapsed:?}
        === action end ==="
                ));
            }
            if let Some(appender) = APPENDER.get() {
                appender.append(action_log);
            }
        } else if let Some(action_span) = action_span(context.span_scope(&id))
            && let Some(action_log) = action_span.extensions_mut().get_mut::<ActionLog>()
            && let Some(span_extension) = span.extensions_mut().remove::<SpanExtension>()
        {
            let elapsed = span_extension.start_time.elapsed();
            action_log.logs.push(format!("[span:{}] elapsed={:?} <<<", span.name(), elapsed));

            let total_elapsed = action_log.stats.entry(format!("{}_elapsed", span.name())).or_default();
            *total_elapsed += elapsed.as_nanos();

            let count = action_log.stats.entry(format!("{}_count", span.name())).or_default();
            *count += 1;
        }
    }

    fn on_record(&self, id: &Id, values: &Record, context: Context<S>) {
        if let Some(action_span) = action_span(context.span_scope(id))
            && let Some(action_log) = action_span.extensions_mut().get_mut::<ActionLog>()
        {
            let span = context.span(id).expect("span must exist");
            let mut log_string = format!("[span:{}] ", span.name());
            values.record(&mut LogVisitor(&mut log_string));
            action_log.logs.push(log_string);
        }
    }

    fn on_event(&self, event: &Event, context: Context<S>) {
        if event.metadata().level() == &Level::TRACE {
            return;
        }

        if let Some(span) = action_span(context.event_scope(event))
            && let Some(action_log) = span.extensions_mut().get_mut::<ActionLog>()
        {
            let elapsed = action_log.start_time.elapsed();
            let total_seconds = elapsed.as_secs();
            let minutes = total_seconds / 60;
            let seconds = total_seconds % 60;
            let nanos = elapsed.subsec_nanos();

            let mut log = String::with_capacity(128);
            write_str!(log, "{minutes:02}:{seconds:02}.{nanos:09} ");

            let metadata = event.metadata();
            let level = metadata.level();
            if level <= &Level::INFO {
                write_str!(log, "{level} ");
            }

            write_str!(log, "{}:{} ", metadata.target(), metadata.line().unwrap_or(0));

            if level == &Level::ERROR || level == &Level::WARN {
                let mut visitor = ErrorVisitor { message: None, code: None };
                event.record(&mut visitor);
                if let Some(ref error_code) = visitor.code {
                    write_str!(log, "[{error_code}] ");
                }

                let result = if level == &Level::ERROR { ActionResult::Error } else { ActionResult::Warn };

                if action_log.result.level() < result.level() {
                    action_log.result = result;
                    action_log.error_code = visitor.code;
                    action_log.error_message = visitor.message;
                }
            }

            let name = event.metadata().name();
            if name == CONTEXT {
                let mut context_visitor = ContextVisitor { action_log };
                event.record(&mut context_visitor);
                write_str!(log, "[context] ");
            } else if name == STATS {
                let mut stats_visitor = StatsVisitor { action_log };
                event.record(&mut stats_visitor);
                write_str!(log, "[stats] ");
            }

            let mut log_visitor = LogVisitor(&mut log);
            event.record(&mut log_visitor);
            action_log.logs.push(truncate(log, MAX_LOG_MESSAGE_LEN, Some("...(truncated)")));
        }
    }
}

struct SpanExtension {
    start_time: Instant,
}

struct ActionVisitor {
    action: Option<String>,
    action_id: Option<String>,
    ref_id: Option<String>,
}

impl ActionVisitor {
    const fn new() -> Self {
        Self { action: None, action_id: None, ref_id: None }
    }

    fn action_log(self) -> Option<ActionLog> {
        if let (Some(action), Some(action_id)) = (self.action, self.action_id) {
            Some(ActionLog {
                id: action_id,
                app: APP.get().unwrap_or(&"unknown"),
                action,
                date: Utc::now(),
                start_time: Instant::now(),
                result: ActionResult::Ok,
                ref_id: self.ref_id,
                error_code: None,
                error_message: None,
                context: IndexMap::new(),
                stats: IndexMap::new(),
                logs: Vec::with_capacity(32),
            })
        } else {
            None
        }
    }
}

impl Visit for ActionVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "action" => self.action = Some(value.to_owned()),
            "action_id" => self.action_id = Some(value.to_owned()),
            "ref_id" => self.ref_id = Some(value.to_owned()),
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn Debug) {}
}

fn action_span<'a, S>(scope: Option<Scope<'a, S>>) -> Option<SpanRef<'a, S>>
where
    S: for<'lookup> LookupSpan<'lookup>,
{
    let mut scope = scope?;
    scope.find(|span| span.name() == "action" && span.extensions().get::<ActionLog>().is_some())
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
    action_log: &'a mut ActionLog,
}

impl Visit for ContextVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        let value = value.to_owned();
        self.action_log.context.insert(field.name(), truncate(value, MAX_CONTEXT_VALUE_LEN, Some("...(truncated)")));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.action_log.context.insert(field.name(), format!("{value:?}"));
    }
}

struct StatsVisitor<'a> {
    action_log: &'a mut ActionLog,
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
        let stats_value = self.action_log.stats.entry(field.name().to_owned()).or_default();
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
