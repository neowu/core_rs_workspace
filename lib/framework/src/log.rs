use std::cell::RefCell;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::time::Instant;

use serde::Deserialize;
use serde::Serialize;
use tokio::task_local;

use crate::appender::Message;
use crate::exception::Exception;
use crate::log::action::Action;
use crate::string::StringExt as _;
use crate::system::SENDER;
use crate::time::DateTime;

pub(crate) mod action;
pub mod id_generator;
mod span;

pub use span::__span;
pub use span::Span;

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
    pub const fn as_str(self) -> &'static str {
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

task_local! {
    // action will be taken out from option after finished
    static CURRENT_ACTION: RefCell<Option<Action>>;
}

pub fn current_action_id() -> Option<String> {
    CURRENT_ACTION.try_with(|action| action.borrow().as_ref().map(|action| action.id.clone())).unwrap_or_default()
}

/// Trigger trace for current action.
pub fn trace() {
    let _result = CURRENT_ACTION.try_with(|action| {
        if let Some(action) = action.borrow_mut().as_mut() {
            action.trace = true;
        }
    });
}

#[inline]
pub async fn action<F, R>(kind: &'static str, ref_ids: Option<Vec<String>>, task: F) -> F::Output
where
    F: Future<Output = Result<R, Exception>>,
{
    let now = DateTime::now();
    let id = id_generator::next_id(now.unix_timestamp_millis());
    let action = Action::new(id, kind, ref_ids, now);
    CURRENT_ACTION
        .scope(RefCell::new(Some(action)), async move {
            let result = task.await;

            let mut current_action = CURRENT_ACTION
                .with(|current_action| current_action.take().expect("current action must be within the scope"));

            if let Err(e) = &result {
                current_action.log_exception(e);
            }
            current_action.finish();

            if let Some(sender) = SENDER.get() {
                let _result = sender.send(Message::Action(current_action.into()));
            }

            result
        })
        .await
}

// DO NOT call log! in Display impl, and pass it as message arguments
// then CURRENT_ACTION will be borrowed twice and panic
#[macro_export]
macro_rules! log {
    (exception = $exception:expr) => {
        $crate::log::__log_exception(&$exception);
    };
    ($($arg:tt)*) => {
        $crate::log::__log(
            format_args!($($arg)*),
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
            format_args!($($arg)*),
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
            format_args!($($arg)*),
            Some($crate::log::Severity::Error),
            Some($error_code),
            concat!(module_path!(), ":", line!()),
        );
    };
}

#[doc(hidden)]
#[inline]
pub fn __log(
    message: fmt::Arguments<'_>,
    severity: Option<Severity>,
    error_code: Option<&'static str>,
    location: &'static str,
) {
    let _result = CURRENT_ACTION.try_with(|action| {
        if let Some(action) = action.borrow_mut().as_mut() {
            action.log(severity, error_code, Some(location), message);
        }
    });
}

#[doc(hidden)]
#[inline]
pub fn __log_exception(exception: &Exception) {
    let _result = CURRENT_ACTION.try_with(|action| {
        if let Some(action) = action.borrow_mut().as_mut() {
            action.log_exception(exception);
        }
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
pub fn __context(key: &'static str, mut values: Vec<String>, location: &'static str) {
    const MAX_CONTEXT_VALUE_LEN: usize = 1_000;

    let _result = CURRENT_ACTION.try_with(|action| {
        if let Some(action) = action.borrow_mut().as_mut() {
            for value in &mut values {
                truncate_with_marker(value, MAX_CONTEXT_VALUE_LEN);
            }

            if values.len() == 1
                && let Some(value) = values.first()
            {
                action.log(None, None, Some(location), format_args!("[context] {key}={value}"));
            } else {
                action.log(None, None, Some(location), format_args!("[context] {key}={values:?}"));
            }

            action.context.push((key, values));
        }
    });
}

#[macro_export]
macro_rules! stats {
    ($($key:ident = $value:expr),+ $(,)?) => {
        $(
            $crate::log::__stats(
                stringify!($key),
                $value as u64,
                concat!(module_path!(), ":", line!()),
            );
        )+
    };
}

#[doc(hidden)]
#[inline]
pub fn __stats(key: &'static str, value: u64, location: &'static str) {
    let _result = CURRENT_ACTION.try_with(|action| {
        if let Some(action) = action.borrow_mut().as_mut() {
            action.log(None, None, Some(location), format_args!("[stats] {key}={value}"));
            action.add_stat(key, value);
        }
    });
}

fn elapsed(start: Instant) -> (u64, u64, u32) {
    let elapsed = start.elapsed();
    let total_seconds = elapsed.as_secs();
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    let nanos = elapsed.subsec_nanos();
    (minutes, seconds, nanos)
}

/// Truncates in place on a char boundary, appending the marker only when something was cut.
fn truncate_with_marker(value: &mut String, len: usize) {
    if value.len() <= len {
        return;
    }

    let new_len = value.truncate_to_max(len).len();
    value.truncate(new_len);
    value.push_str("...(truncated)");
}

#[cfg(test)]
mod tests {
    use crate::log::Severity;

    #[test]
    fn compare_severity() {
        assert_eq!(Severity::Info, Severity::Info);
        assert!(Severity::Info < Severity::Warn);
        assert!(Severity::Warn < Severity::Error);
    }

    #[test]
    fn truncate_with_marker() {
        let mut cut_at_char = "123老虎456".to_owned();
        super::truncate_with_marker(&mut cut_at_char, 6);
        assert_eq!(cut_at_char, "123老...(truncated)");

        let mut cut_mid_char = "123老虎456".to_owned();
        super::truncate_with_marker(&mut cut_mid_char, 10);
        assert_eq!(cut_mid_char, "123老虎4...(truncated)");

        // nothing was cut, so no marker
        let mut untouched = "123".to_owned();
        super::truncate_with_marker(&mut untouched, 3);
        assert_eq!(untouched, "123");
    }
}
