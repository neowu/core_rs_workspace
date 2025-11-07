use std::fmt::Debug;
use std::fmt::Write;
use std::thread;
use std::time::Instant;

use chrono::DateTime;
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

use super::ActionLogAppender;
use super::ActionLogMessage;
use super::ActionResult;

pub(super) struct ActionLogLayer<T>
where
    T: ActionLogAppender,
{
    pub(super) appender: T,
}

struct ActionLog {
    id: String,
    action: String,
    date: DateTime<Utc>,
    start_time: Instant,
    result: ActionResult,
    ref_id: Option<String>,
    error_code: Option<String>,
    error_message: Option<String>,
    context: IndexMap<&'static str, String>,
    stats: IndexMap<String, u128>,
    logs: Vec<String>,
}

impl<T, S> Layer<S> for ActionLogLayer<T>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    T: ActionLogAppender + 'static,
{
    fn on_new_span(&self, attrs: &Attributes, id: &Id, context: Context<S>) {
        let span = context.span(id).unwrap();
        let mut extensions = span.extensions_mut();

        if span.name() == "action" {
            let mut action_visitor = ActionVisitor::new();
            attrs.record(&mut action_visitor);
            if let Some(mut action_log) = action_visitor.action_log()
                && extensions.get_mut::<ActionLog>().is_none()
            {
                action_log.logs.push(format!(
                    r#"=== action begin ===
type={}
id={}
date={}
thread={:?}"#,
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
            extensions.insert(SpanExtension {
                start_time: Instant::now(),
            });

            let mut log_string = format!("[span:{}] ", span.name());
            attrs.record(&mut LogVisitor(&mut log_string));
            log_string.push_str(">>>");
            action_log.logs.push(log_string);
        }
    }

    fn on_close(&self, id: Id, context: Context<S>) {
        let span = context.span(&id).unwrap();
        if let Some(action_log) = span.extensions_mut().remove::<ActionLog>() {
            let action_log_message = close_action(action_log);
            self.appender.append(action_log_message);
        } else if let Some(action_span) = action_span(context.span_scope(&id))
            && let Some(action_log) = action_span.extensions_mut().get_mut::<ActionLog>()
            && let Some(span_extension) = span.extensions_mut().remove::<SpanExtension>()
        {
            let elapsed = span_extension.start_time.elapsed();
            action_log
                .logs
                .push(format!("[span:{}] elapsed={:?} <<<", span.name(), elapsed));

            let value = action_log.stats.entry(format!("{}_elapsed", span.name())).or_default();
            *value += elapsed.as_nanos();

            let value = action_log.stats.entry(format!("{}_count", span.name())).or_default();
            *value += 1;
        }
    }

    fn on_record(&self, id: &Id, values: &Record, context: Context<S>) {
        if let Some(action_span) = action_span(context.span_scope(id))
            && let Some(action_log) = action_span.extensions_mut().get_mut::<ActionLog>()
        {
            let span = context.span(id).unwrap();
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

            let mut log = String::new();
            write!(log, "{minutes:02}:{seconds:02}.{nanos:09} ").unwrap();

            let metadata = event.metadata();
            let level = metadata.level();
            if level <= &Level::INFO {
                write!(log, "{level} ").unwrap();
            }

            write!(log, "{}:{} ", metadata.target(), metadata.line().unwrap_or(0)).unwrap();

            if level == &Level::ERROR || level == &Level::WARN {
                let mut visitor = ErrorVisitor {
                    message: None,
                    code: None,
                };
                event.record(&mut visitor);
                if let Some(ref error_code) = visitor.code {
                    write!(log, "[{error_code}] ").unwrap();
                }

                let result = if level == &Level::ERROR {
                    ActionResult::Error
                } else {
                    ActionResult::Warn
                };

                if action_log.result.level() < result.level() {
                    action_log.result = result;
                    action_log.error_code = visitor.code;
                    action_log.error_message = visitor.message;
                }
            }

            let mut visitor = LogVisitor(&mut log);
            event.record(&mut visitor);
            action_log.logs.push(log);

            // hanldle "context" and "stats" event
            let mut visitor = ContextVisitor {
                action_log,
                context_type: None,
            };
            event.record(&mut visitor);
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
    fn new() -> Self {
        Self {
            action: None,
            action_id: None,
            ref_id: None,
        }
    }

    fn action_log(self) -> Option<ActionLog> {
        if let (Some(action), Some(action_id)) = (self.action, self.action_id) {
            Some(ActionLog {
                id: action_id,
                action,
                date: Utc::now(),
                start_time: Instant::now(),
                result: ActionResult::Ok,
                ref_id: self.ref_id,
                error_code: None,
                error_message: None,
                context: IndexMap::new(),
                stats: IndexMap::new(),
                logs: Vec::new(),
            })
        } else {
            None
        }
    }
}

impl Visit for ActionVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "action" => self.action = Some(value.to_string()),
            "action_id" => self.action_id = Some(value.to_string()),
            "ref_id" => self.ref_id = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn Debug) {}
}

fn action_span<'a, S>(scope: Option<Scope<'a, S>>) -> Option<SpanRef<'a, S>>
where
    S: for<'lookup> LookupSpan<'lookup>,
{
    scope.and_then(|mut scope| {
        scope.find(|span| span.name() == "action" && span.extensions().get::<ActionLog>().is_some())
    })
}

fn close_action(mut action_log: ActionLog) -> ActionLogMessage {
    let elapsed = action_log.start_time.elapsed();
    action_log.stats.insert("elapsed".to_owned(), elapsed.as_nanos());
    let mut trace = None;
    if action_log.result.level() > ActionResult::Ok.level() {
        action_log.logs.push(format!(
            r#"elapsed={elapsed:?}
=== action end ===
"#
        ));
        trace = Some(action_log.logs.join("\n"));
    }

    ActionLogMessage {
        id: action_log.id,
        date: action_log.date,
        action: action_log.action,
        result: action_log.result,
        ref_id: action_log.ref_id,
        error_code: action_log.error_code,
        error_message: action_log.error_message,
        context: action_log.context,
        stats: action_log.stats,
        trace,
    }
}

struct LogVisitor<'a>(&'a mut String);

impl Visit for LogVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "backtrace" {
            write!(self.0, "\n{value}").unwrap();
        } else if field.name() == "error_code" {
            // do not log error_code here, it is handled in ErrorVisitor
        } else {
            write!(self.0, "{}={} ", field.name(), value).unwrap();
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            write!(self.0, "{value:?} ").unwrap();
        } else {
            write!(self.0, "{}={:?} ", field.name(), value).unwrap();
        }
    }
}

enum ContextType {
    Context,
    Stats,
}

struct ContextVisitor<'a> {
    action_log: &'a mut ActionLog,
    context_type: Option<ContextType>,
}

impl Visit for ContextVisitor<'_> {
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
        if let Some(ContextType::Stats) = self.context_type {
            let stats_value = self.action_log.stats.entry(field.name().to_owned()).or_default();
            *stats_value += value;
        } else if let Some(ContextType::Context) = self.context_type {
            self.action_log.context.insert(field.name(), format!("{value}"));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if let Some(ContextType::Context) = self.context_type {
            self.action_log.context.insert(field.name(), value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            let value = format!("{value:?}");
            if value == "context" {
                self.context_type = Some(ContextType::Context);
            } else if value == "stats" {
                self.context_type = Some(ContextType::Stats);
            }
        } else if let Some(ContextType::Context) = self.context_type {
            self.action_log.context.insert(field.name(), format!("{value:?}"));
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
            self.code = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            let message = format!("{value:?}");
            self.message = Some(if message.len() > 200 {
                message[..200].to_string()
            } else {
                message
            });
        }
    }
}
