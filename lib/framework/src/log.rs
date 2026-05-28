use std::cell::RefCell;
use std::env;
use std::sync::OnceLock;
use std::thread;
use std::time::Instant;

use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use indexmap::IndexMap;
use tokio::task_local;
use tracing::Instrument as _;
use tracing::info;
use tracing::info_span;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer as _;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

use crate::exception::Exception;
use crate::exception::Severity;
use crate::log::appender::APPENDER;
use crate::log::appender::ActionAppender;
use crate::log::layer::ActionLogLayer;
use crate::write_str;

pub mod appender;
pub mod id_generator;
mod layer;

pub fn init() {
    // SAFETY:
    // init only be called once on startup, no threading issue
    unsafe {
        env::set_var("RUST_BACKTRACE", "1");
    }

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .compact()
                .with_ansi(false) // generally cloud log console doesn't support color
                .with_line_number(true)
                .with_thread_ids(true)
                .with_filter(LevelFilter::INFO),
        )
        .with(ActionLogLayer)
        .init();
}

static APP: OnceLock<&'static str> = OnceLock::new();

pub fn init_action_appender(appender: &str, app: &'static str) -> Result<(), Exception> {
    let value = match appender {
        "console" => ActionAppender::Console,
        "gcloud" => ActionAppender::GoogleCloud,
        _ => return Err(exception!("unknown appender, value={appender}")),
    };
    APPENDER.set(value).map_err(|_err| exception!("appender was already initialized"))?;
    APP.set(app).map_err(|_err| exception!("appender was already initialized"))?;
    info!("init action appender, appender={appender}");
    Ok(())
}

#[inline]
pub async fn start_action<F, R>(kind: &'static str, ref_id: Option<String>, task: F) -> F::Output
where
    F: Future<Output = Result<R, Exception>>,
{
    let action_id = id_generator::random_id();
    let action_span = info_span!("action", kind, action_id, ref_id);
    let mut action = Action {
        start_time: Instant::now(),
        id: action_id,
        app: APP.get().unwrap_or(&"unknown"),
        kind,
        date: Utc::now(),
        ref_id,
        severity: None,
        error_code: None,
        error_message: None,
        context: IndexMap::new(),
        stats: IndexMap::new(),
        logs: Vec::with_capacity(32),
    };
    action.logs.push(format!(
        "# action begin, kind={}, id={}, date={}, thread={:?}, ref_id={:?}",
        &action.kind,
        &action.id,
        &action.date.to_rfc3339_opts(SecondsFormat::Nanos, true),
        thread::current().id(),
        &action.ref_id
    ));
    CURRENT_ACTION
        .scope(RefCell::new(action), async move {
            let result = task.instrument(action_span).await;
            if let Err(e) = &result {
                e.log();
            }
            CURRENT_ACTION.with(|current_action| {
                let mut current_action = current_action.borrow_mut();
                let elapsed = current_action.start_time.elapsed();
                current_action.stats.insert("elapsed".to_owned(), elapsed.as_nanos());
                if current_action.severity.is_some() {
                    current_action.logs.push(format!("# action end, elapsed={elapsed:?}"));
                }
                if let Some(appender) = APPENDER.get() {
                    appender.append(&current_action);
                }
            });
            result
        })
        .await
}

task_local! {
    static CURRENT_ACTION: RefCell<Action>;
}

#[inline]
pub fn current_action_id() -> Option<String> {
    CURRENT_ACTION.try_with(|action| Some(action.borrow().id.clone())).unwrap_or(None)
}

#[macro_export]
macro_rules! context {
    ($($key:ident = $value:expr),+ $(,)?) => {
        $(
            $crate::log::__context(
                stringify!($key),
                $value,
                concat!(module_path!(), ":", line!()),
            );
        )+
    };
}

#[doc(hidden)]
#[inline]
pub fn __context(key: &'static str, value: impl Into<String>, location: &'static str) {
    const MAX_CONTEXT_VALUE_LEN: usize = 1_000;

    let _result = CURRENT_ACTION.try_with(|action| {
        let mut action = action.borrow_mut();

        let (minutes, seconds, nanos) = elapsed(action.start_time);
        let mut log = format!("{minutes:02}:{seconds:02}.{nanos:09} ");
        let value = truncate(value.into(), MAX_CONTEXT_VALUE_LEN, Some("...(truncated)"));
        write_str!(log, "{location} [content] {key}={value}");

        action.context.insert(key, value);
        action.logs.push(log);
    });
}

#[macro_export]
macro_rules! stats {
    ($($key:ident = $value:expr),+ $(,)?) => {
        $(
            $crate::log::__stats(
                stringify!($key),
                $value as u128,
                concat!(module_path!(), ":", line!()),
            );
        )+
    };
}

#[doc(hidden)]
#[inline]
pub fn __stats(key: &'static str, value: u128, location: &'static str) {
    let _result = CURRENT_ACTION.try_with(|action| {
        let mut action = action.borrow_mut();

        let (minutes, seconds, nanos) = elapsed(action.start_time);
        let mut log = format!("{minutes:02}:{seconds:02}.{nanos:09} ");
        write_str!(log, "{location} [stats] {key}={value}");

        let stats_value = action.stats.entry(key.to_owned()).or_default();
        *stats_value += value;
        action.logs.push(log);
    });
}

struct Action {
    start_time: Instant,
    id: String,
    app: &'static str,
    kind: &'static str,
    date: DateTime<Utc>,
    ref_id: Option<String>,
    severity: Option<Severity>,
    error_code: Option<String>,
    error_message: Option<String>,
    context: IndexMap<&'static str, String>,
    stats: IndexMap<String, u128>,
    logs: Vec<String>,
}

fn elapsed(start: Instant) -> (u64, u64, u32) {
    let elapsed = start.elapsed();
    let total_seconds = elapsed.as_secs();
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    let nanos = elapsed.subsec_nanos();
    (minutes, seconds, nanos)
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
    use crate::log::truncate;

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
