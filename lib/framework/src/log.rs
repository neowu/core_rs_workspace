use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use pin_project_lite::pin_project;
use serde::Deserialize;
use serde::Serialize;
use smallvec::SmallVec;

use crate::appender::Message;
use crate::exception::Exception;
use crate::log::action::Action;
use crate::log::inflight::ActionCell;
use crate::log::inflight::with_current;
use crate::system::SENDER;
use crate::time::DateTime;

pub(crate) mod action;
pub mod id_generator;
pub(crate) mod inflight;
mod mask;
mod span;
mod stats;

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

pub fn current_action_id() -> Option<String> {
    with_current(|action| action.borrow().id.as_str().to_owned())
}

/// Trigger trace for current action.
pub fn trace() {
    let _result = with_current(|action| {
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
        // dropped inside the action, on completion and on cancellation, then None
        #[pin]
        task: Option<F>,
        // the action lives here, not in a task local, so it has an address the in-flight registry can read
        #[pin]
        cell: ActionCell,
    }

    impl<F> PinnedDrop for ActionFuture<F> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            // cancelled, what the task holds (a span, a guard) still logs into its own action, not an outer one
            if this.task.is_some() {
                this.cell.as_ref().enter(|| this.task.set(None));
            }
        }
    }
}

// the `Result<_, Exception>` bound lives on the `Future` impl instead of here, so callers passing an
// inline async block do not need to annotate its output type
#[inline]
pub fn action<F: Future>(kind: &'static str, ref_ids: Option<Vec<String>>, task: F) -> ActionFuture<F> {
    let now = DateTime::now();
    let id = id_generator::next_id(now.unix_timestamp_millis());
    let action = Action::new(id, kind, ref_ids, now);
    ActionFuture { task: Some(task), cell: ActionCell::new(action) }
}

impl<F, R> Future for ActionFuture<F>
where
    F: Future<Output = Result<R, Exception>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut ActionFuture<F>>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let cell = this.cell.as_ref();
        // the allocation and poll stats are taken on the Pending path too, or every poll but the last is lost
        let polled = cell.poll(|| {
            let task = this.task.as_mut().as_pin_mut().expect("action must not be polled after completion");
            let polled = task.poll(cx);
            if polled.is_ready() {
                this.task.set(None);
            }
            polled
        });
        let Poll::Ready(result) = polled else {
            return Poll::Pending;
        };

        let mut current_action = cell.finish();

        if let Err(e) = &result {
            current_action.log_exception(e);
        }
        current_action.finish();

        if let Some(sender) = SENDER.get() {
            let _result = sender.send(Message::Action(current_action.into()));
        }

        Poll::Ready(result)
    }
}

// DO NOT call log! in Display impl, and pass it as message arguments
// then the current action will be borrowed twice and panic
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
    let _result = with_current(|action| {
        action.borrow_mut().log(severity, error_code, Some(location), message);
    });
}

#[doc(hidden)]
#[inline]
pub fn __log_exception(exception: &Exception) {
    let _result = with_current(|action| {
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
    let _result = with_current(|action| {
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
    let _result = with_current(|action| {
        let mut action = action.borrow_mut();
        action.log(None, None, Some(location), format_args!("[stats] {key}={value}"));
        action.add_stat(key, value);
    });
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Context;
    use std::task::Poll;

    use futures::executor::block_on;
    use futures::poll;

    use super::ContextValues;
    use super::ScalarContextValue as _;
    use super::VecContextValue as _;
    use crate::exception::Exception;
    use crate::log::Severity;
    use crate::log::action;
    use crate::log::current_action_id;

    // records the current action when dropped
    struct DropGuard(Arc<Mutex<Option<String>>>);

    impl Drop for DropGuard {
        fn drop(&mut self) {
            *self.0.lock().unwrap() = current_action_id();
        }
    }

    // completes at once and keeps its guard until it is destroyed
    struct Retains {
        _guard: DropGuard,
    }

    impl Future for Retains {
        type Output = Result<Option<String>, Exception>;

        fn poll(self: Pin<&mut Retains>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(current_action_id()))
        }
    }

    #[test]
    fn cancelled_task_is_dropped_inside_its_action() {
        let dropped_in = Arc::new(Mutex::new(None));
        let guard = DropGuard(Arc::clone(&dropped_in));
        let child_id = Arc::new(Mutex::new(None));
        let started = Arc::clone(&child_id);

        let outer_id = block_on(action("test", None, async move {
            let mut child = Box::pin(action("test", None, async move {
                let _guard = guard;
                *started.lock().unwrap() = current_action_id();
                future::pending::<Result<(), Exception>>().await
            }));
            assert!(poll!(child.as_mut()).is_pending());
            drop(child); // cancelled while the outer action is current
            Ok::<_, Exception>(current_action_id())
        }))
        .expect("outer action must complete");

        let child_id = child_id.lock().unwrap().clone();
        assert!(child_id.is_some());
        assert_ne!(child_id, outer_id);
        assert_eq!(*dropped_in.lock().unwrap(), child_id);
    }

    #[test]
    fn completed_task_is_dropped_inside_its_action() {
        let dropped_in = Arc::new(Mutex::new(None));
        let id = block_on(action("test", None, Retains { _guard: DropGuard(Arc::clone(&dropped_in)) }))
            .expect("action must complete");

        assert!(id.is_some());
        assert_eq!(*dropped_in.lock().unwrap(), id);
    }

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
