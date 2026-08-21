use std::borrow::Cow;
use std::cell::Ref;
use std::cell::RefCell;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::OnceLock;
use std::time::Instant;

use serde::Deserialize;
use serde::Serialize;
use tokio::task_local;

use crate::exception::Exception;
use crate::log::action::Action;
use crate::log::appender::Appender;
use crate::network::hostname;
use crate::time::DateTime;
use crate::write_str;

mod action;
pub mod appender;
pub mod id_generator;
pub mod metrics;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Severity {
    #[serde(rename = "INFO")]
    Info = 1,
    #[serde(rename = "WARN")]
    Warn = 2,
    #[serde(rename = "ERROR")]
    Error = 3,
}

impl Severity {
    const fn as_str(self) -> &'static str {
        match self {
            Severity::Info => "INFO",
            Severity::Warn => "WARN",
            Severity::Error => "ERROR",
        }
    }
}

impl Display for Severity {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// used for logging without action context
#[macro_export]
macro_rules! console {
    ($($arg:tt)*) => {
        ::std::println!(
            concat!("{} ", module_path!(), ":", line!(), " {}"),
            $crate::time::DateTime::now().to_rfc3339(),
            format_args!($($arg)*),
        )
    };
}

pub fn init(appender: &str, app: &'static str) {
    CONTEXT.get_or_init(|| {
        console!("init log appender, appender={appender}");
        let appender = match appender {
            "console" => Appender::Console,
            "gcloud" => Appender::GoogleCloud,
            _ => panic!("unknown appender, value={appender}"),
        };
        Context { appender, app, host: hostname().leak() }
    });
}

static CONTEXT: OnceLock<Context> = OnceLock::new();

struct Context {
    appender: Appender,
    app: &'static str,
    host: &'static str,
}

task_local! {
    static CURRENT_ACTION: RefCell<Action>;
}

#[inline]
pub async fn action<F, R>(kind: &'static str, ref_id: Option<Vec<String>>, task: F) -> F::Output
where
    F: Future<Output = Result<R, Exception>>,
{
    if let Some(Context { appender, app, host }) = CONTEXT.get() {
        let now = DateTime::now();
        let id = id_generator::next_id(now.unix_timestamp_millis());
        let action = Action::new(id, kind, ref_id, now, app, host);
        CURRENT_ACTION
            .scope(RefCell::new(action), async move {
                let result = task.await;
                CURRENT_ACTION.with(|current_action| {
                    let mut current_action = current_action.borrow_mut();
                    if let Err(e) = &result {
                        current_action.log_exception(e);
                    }
                    current_action.finish();
                    appender.append_action(&current_action);
                });
                result
            })
            .await
    } else {
        let result = task.await;
        if let Err(e) = &result {
            console!("ERROR {e}");
        }
        result
    }
}

pub fn trace() {
    let _result = CURRENT_ACTION.try_with(|action| {
        let mut action = action.borrow_mut();
        action.trace = true;
    });
}

pub fn with_current_action<F, R>(f: F) -> Option<R>
where
    F: FnOnce(Ref<'_, Action>) -> R,
{
    CURRENT_ACTION
        .try_with(|action| {
            let action = action.borrow();
            Some(f(action))
        })
        .unwrap_or(None)
}

pub struct Span {
    name: &'static str,
    start_time: Instant,
    log_index: usize,
}

#[macro_export]
macro_rules! span {
    ($name:expr) => {
        $crate::log::__span($name, concat!(module_path!(), ":", line!()))
    };
}

#[doc(hidden)]
#[inline]
pub fn __span(name: &'static str, location: &'static str) -> Span {
    let mut log_index: usize = 0;
    let _result = CURRENT_ACTION.try_with(|action| {
        let mut action = action.borrow_mut();
        action.log(&format!("[span:{name}] >"), location);
        log_index = action.logs.len();
    });
    Span { name, start_time: Instant::now(), log_index }
}

impl Span {
    pub fn clear(&self) {
        let _result = CURRENT_ACTION.try_with(|action| {
            let mut action = action.borrow_mut();
            action.logs.truncate(self.log_index);
            if let Some(last) = action.logs.last_mut()
                && last.ends_with('>')
            {
                last.push_str(" ...(truncated)");
            }
        });
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        let _result = CURRENT_ACTION.try_with(|action| {
            let mut action = action.borrow_mut();

            let name = self.name;
            let span_elapsed = self.start_time.elapsed();

            let (minutes, seconds, nanos) = elapsed(action.start_time);
            let mut log = String::with_capacity(256);
            write_str!(log, "{minutes:02}:{seconds:02}.{nanos:09} [span:{name}] elapsed={span_elapsed:?} <");
            action.logs.push(log);

            let total_elapsed = action.stats.entry(Cow::Owned(format!("{name}_elapsed"))).or_default();
            *total_elapsed += span_elapsed.as_nanos() as f64;
            let count = action.stats.entry(Cow::Owned(format!("{name}_count"))).or_default();
            *count += 1_f64;
        });
    }
}

#[macro_export]
macro_rules! log {
    (exception = $exception:expr) => {
        $crate::log::__log_exception(&$exception);
    };
    ($($arg:tt)*) => {
        $crate::log::__log(
            format!($($arg)*),
            None,
            None,
            concat!(module_path!(), ":", line!()),
        );
    };
}

#[macro_export]
macro_rules! warn {
    (error_code = $error_code:expr, $($arg:tt)*) => {
        $crate::log::__log(
            format!($($arg)*),
            Some($crate::log::Severity::Warn),
            Some($error_code),
            concat!(module_path!(), ":", line!()),
        );
    };
}

#[macro_export]
macro_rules! error {
    (error_code = $error_code:expr, $($arg:tt)*) => {
        $crate::log::__log(
            format!($($arg)*),
            Some($crate::log::Severity::Error),
            Some($error_code),
            concat!(module_path!(), ":", line!()),
        );
    };
}

#[doc(hidden)]
#[inline]
pub fn __log(message: String, severity: Option<Severity>, error_code: Option<&'static str>, location: &'static str) {
    const MAX_LOG_MESSAGE_LEN: usize = 10_000;

    let _result = CURRENT_ACTION.try_with(|action| {
        let mut action = action.borrow_mut();
        action.log_with_severity(
            &truncate(message, MAX_LOG_MESSAGE_LEN, Some("...(truncated)")),
            severity,
            error_code,
            location,
        );
    });
}

#[doc(hidden)]
#[inline]
pub fn __log_exception(exception: &Exception) {
    let _result = CURRENT_ACTION.try_with(|action| {
        let mut action = action.borrow_mut();
        action.log_exception(exception);
    });
}

#[macro_export]
macro_rules! context {
    ($($key:ident = $value:expr),+ $(,)?) => {
        $({
            #[allow(unused_imports)]
            use $crate::log::{ScalarContextValue as _, VecContextValue as _};
            $crate::log::__context(
                stringify!($key),
                ($value).__into_context_value(),
                concat!(module_path!(), ":", line!()),
            );
        })+
    };
}

#[doc(hidden)]
pub trait ScalarContextValue {
    fn __into_context_value(self) -> Vec<String>;
}

impl<T: Into<String>> ScalarContextValue for T {
    #[inline]
    fn __into_context_value(self) -> Vec<String> {
        vec![self.into()]
    }
}

#[doc(hidden)]
pub trait VecContextValue {
    fn __into_context_value(self) -> Vec<String>;
}

impl<T: Into<String>> VecContextValue for Vec<T> {
    #[inline]
    fn __into_context_value(self) -> Vec<String> {
        self.into_iter().map(Into::into).collect()
    }
}

#[doc(hidden)]
#[inline]
pub fn __context(key: &'static str, values: Vec<String>, location: &'static str) {
    const MAX_CONTEXT_VALUE_LEN: usize = 1_000;

    let _result = CURRENT_ACTION.try_with(|action| {
        let mut action = action.borrow_mut();

        let values: Vec<String> =
            values.into_iter().map(|value| truncate(value, MAX_CONTEXT_VALUE_LEN, Some("...(truncated)"))).collect();

        if values.len() == 1
            && let Some(value) = values.first()
        {
            action.log(&format!("[context] {key}={value}"), location);
        } else {
            action.log(&format!("[context] {key}={values:?}"), location);
        }

        action.context.push((key, values));
    });
}

#[macro_export]
macro_rules! stats {
    ($($key:ident = $value:expr),+ $(,)?) => {
        $(
            $crate::log::__stats(
                stringify!($key),
                $value as f64,
                concat!(module_path!(), ":", line!()),
            );
        )+
    };
}

#[doc(hidden)]
#[inline]
pub fn __stats(key: &'static str, value: f64, location: &'static str) {
    let _result = CURRENT_ACTION.try_with(|action| {
        let mut action = action.borrow_mut();

        action.log(&format!("[stats] {key}={value}"), location);

        let stats_value = action.stats.entry(Cow::Borrowed(key)).or_default();
        *stats_value = stats_value.algebraic_add(value);
    });
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
    use crate::log::Severity;
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

    #[test]
    fn compare_severity() {
        assert_eq!(Severity::Info, Severity::Info);
        assert!(Severity::Info < Severity::Warn);
        assert!(Severity::Warn < Severity::Error);
    }
}
