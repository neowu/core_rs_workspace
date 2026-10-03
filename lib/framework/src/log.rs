use std::cell::RefCell;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use pin_project_lite::pin_project;
use serde::Deserialize;
use serde::Serialize;
use smallvec::SmallVec;
use tokio::task::futures::TaskLocalFuture;
use tokio::task_local;

use crate::appender::Message;
use crate::exception::Exception;
use crate::log::action::Action;
use crate::log::alloc_stats::ActionAllocs;
use crate::system::SENDER;
use crate::time::DateTime;

pub(crate) mod action;
mod alloc_stats;
pub mod id_generator;
mod mask;
mod span;

pub use span::__span;
pub use span::Span;

/// Context values serialize as an array, with the common single value stored inline.
pub type ContextValues = SmallVec<[String; 1]>;

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
    // the action is always present for the whole scope, ActionFuture takes it back out of the task
    // local slot only once the scoped future has finished
    static CURRENT_ACTION: RefCell<Action>;
}

pub fn current_action_id() -> Option<String> {
    CURRENT_ACTION.try_with(|action| action.borrow().id.clone()).ok()
}

/// Trigger trace for current action.
pub fn trace() {
    let _result = CURRENT_ACTION.try_with(|action| {
        action.borrow_mut().trace = true;
    });
}

pin_project! {
    /// Hand written so the task is stored exactly once.
    ///
    /// An `async fn` wrapping the task would hold it three times over: as its own parameter, as the
    /// upvar of the inner `async move` block, and again as the awaitee inside that block. Coroutine
    /// parameters and upvars live in the layout prefix and are never overlapped, so a 6KB task turned
    /// into an 18KB action future and tripped `clippy::large_futures`.
    pub struct ActionFuture<F> {
        #[pin]
        inner: TaskLocalFuture<RefCell<Action>, F>,
        allocs: ActionAllocs,
        // nanos spent inside inner poll, i.e. holding the worker thread; waiting is not counted
        poll_elapsed: u64,
        poll_count: u64,
    }
}

// the `Result<_, Exception>` bound lives on the `Future` impl instead of here, so callers passing an
// inline async block do not need to annotate its output type
#[inline]
pub fn action<F: Future>(kind: &'static str, ref_ids: Option<Vec<String>>, task: F) -> ActionFuture<F> {
    let now = DateTime::now();
    let id = id_generator::next_id(now.unix_timestamp_millis());
    let action = Action::new(id, kind, ref_ids, now);
    ActionFuture {
        inner: CURRENT_ACTION.scope(RefCell::new(action), task),
        allocs: ActionAllocs::new(),
        poll_elapsed: 0,
        poll_count: 0,
    }
}

impl<F, R> Future for ActionFuture<F>
where
    F: Future<Output = Result<R, Exception>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        // not `ready!`: the allocation and poll stats have to be taken on the Pending path too, or every
        // poll but the last is lost
        let start = Instant::now();
        let polled = this.allocs.poll(|| this.inner.as_mut().poll(cx));
        *this.poll_elapsed += start.elapsed().as_nanos() as u64;
        *this.poll_count += 1;
        let Poll::Ready(result) = polled else {
            return Poll::Pending;
        };

        // the scope has ended, so the action comes out of the slot rather than the task local
        let mut current_action =
            this.inner.take_value().map(RefCell::into_inner).expect("current action must be within the scope");

        if let Err(e) = &result {
            current_action.log_exception(e);
        }
        this.allocs.write_to(&mut current_action);
        current_action.add_stat("poll_elapsed", *this.poll_elapsed);
        current_action.add_stat("poll_count", *this.poll_count);
        current_action.finish();

        if let Some(sender) = SENDER.get() {
            let _result = sender.send(Message::Action(current_action.into()));
        }

        Poll::Ready(result)
    }
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
        action.borrow_mut().log(severity, error_code, Some(location), message);
    });
}

#[doc(hidden)]
#[inline]
pub fn __log_exception(exception: &Exception) {
    let _result = CURRENT_ACTION.try_with(|action| {
        action.borrow_mut().log_exception(exception);
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
    fn __into_context_value(self) -> ContextValues;
}

impl<T: Into<String>> ScalarContextValue for T {
    #[inline]
    fn __into_context_value(self) -> ContextValues {
        SmallVec::from_buf([self.into()])
    }
}

#[doc(hidden)]
pub trait VecContextValue {
    fn __into_context_value(self) -> ContextValues;
}

impl<T: Into<String>> VecContextValue for Vec<T> {
    #[inline]
    fn __into_context_value(self) -> ContextValues {
        self.into_iter().map(T::into).collect()
    }
}

#[doc(hidden)]
#[inline]
pub fn __context(key: &'static str, values: ContextValues, location: &'static str) {
    let _result = CURRENT_ACTION.try_with(|action| {
        let mut action = action.borrow_mut();

        if values.len() == 1
            && let Some(value) = values.first()
        {
            action.log(None, None, Some(location), format_args!("[context] {key}={value}"));
        } else {
            action.log(None, None, Some(location), format_args!("[context] {key}={values:?}"));
        }

        action.add_context(key, values);
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
        let mut action = action.borrow_mut();
        action.log(None, None, Some(location), format_args!("[stats] {key}={value}"));
        action.add_stat(key, value);
    });
}

#[cfg(test)]
mod tests {

    use super::ContextValues;
    use super::ScalarContextValue as _;
    use super::VecContextValue as _;
    use crate::log::Severity;

    #[test]
    fn context_values_preserve_arrays() {
        let scalar = "GET".__into_context_value();
        assert!(!scalar.spilled());
        let cases = [
            (scalar, r#"["GET"]"#),
            (Vec::<String>::new().__into_context_value(), "[]"),
            (vec!["one"].__into_context_value(), r#"["one"]"#),
            (vec!["one", "two"].__into_context_value(), r#"["one","two"]"#),
        ];
        for (values, json) in cases {
            assert_eq!(serde_json::to_string(&values).unwrap(), json);
            assert_eq!(serde_json::from_str::<ContextValues>(json).unwrap(), values);
        }
    }

    #[test]
    fn compare_severity() {
        assert_eq!(Severity::Info, Severity::Info);
        assert!(Severity::Info < Severity::Warn);
        assert!(Severity::Warn < Severity::Error);
    }
}
