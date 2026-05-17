use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;
use tracing::error;
use tracing::warn;

use crate::write_str;

pub mod error_code;

pub struct Exception {
    pub severity: Severity,
    pub code: Option<String>,
    pub message: String,
    pub location: Option<&'static str>,
    pub source: Option<Box<Exception>>,
}

// used by HttpErrorBody to serialize/deserialize
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Severity {
    #[serde(rename = "WARN")]
    Warn,
    #[serde(rename = "ERROR")]
    Error,
}

impl Exception {
    #[inline]
    #[doc(hidden)]
    pub fn __new(message: impl Into<String>, location: &'static str) -> Self {
        Self { severity: Severity::Error, code: None, message: message.into(), location: Some(location), source: None }
    }

    #[inline]
    #[must_use]
    #[doc(hidden)]
    pub const fn __with_severity(mut self, severity: Severity) -> Self {
        self.severity = severity;
        self
    }

    #[inline]
    #[must_use]
    #[doc(hidden)]
    pub fn __with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    #[inline]
    #[must_use]
    #[doc(hidden)]
    pub fn __with_source(mut self, source: impl Into<Exception>) -> Self {
        self.source = Some(Box::new(source.into()));
        self
    }

    #[inline]
    pub fn backtrace(&self) -> String {
        let mut trace = String::with_capacity(256);
        let mut index = 0;
        let mut current_source = Some(self);
        while let Some(source) = current_source {
            if index > 0 {
                trace.push('\n');
            }
            write_str!(
                trace,
                "{index}: {} ",
                match source.severity {
                    Severity::Warn => "WARN",
                    Severity::Error => "ERROR",
                }
            );
            if let Some(ref code) = source.code {
                write_str!(trace, "[{code}] ");
            }
            write_str!(trace, "{}", source.message);
            if let Some(location) = source.location {
                write_str!(trace, " at {location}");
            }
            index += 1;
            current_source = source.source.as_ref().map(Box::as_ref);
        }
        trace
    }

    #[inline]
    pub fn log(&self) {
        let backtrace = self.backtrace();
        let message = &self.message;
        match (self.severity, self.code.as_deref()) {
            (Severity::Warn, Some(error_code)) => warn!(error_code, backtrace, "{message}"),
            (Severity::Warn, None) => warn!(backtrace, "{message}"),
            (Severity::Error, Some(error_code)) => error!(error_code, backtrace, "{message}"),
            (Severity::Error, None) => error!(backtrace, "{message}"),
        }
    }
}

impl Debug for Exception {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for Exception {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

#[macro_export]
macro_rules! exception {
    ($message:expr $(, severity = $severity:expr)? $(, code = $code:expr)? $(, source = $source:expr)?) => {{
        let result = $crate::exception::Exception::__new(
            $message,
            concat!(file!(), ":", line!(), ":", column!()),
        );
        $( let result = result.__with_severity($severity); )?
        $( let result = result.__with_code($code); )?
        $( let result = result.__with_source($source); )?
        result
    }};
}

#[macro_export]
macro_rules! validation_error {
    ($message:expr $(, severity = $severity:expr)?) => {{
        let result = $crate::exception::Exception::__new(
            $message,
            concat!(file!(), ":", line!(), ":", column!()),
        );
        let result = result.__with_code($crate::exception::error_code::VALIDATION_ERROR);
        $( let result = result.__with_severity($severity); )?
        result
    }};
}

fn source(error: Option<&(dyn Error + 'static)>) -> Option<Box<Exception>> {
    error.map(|e| {
        Box::new(Exception {
            severity: Severity::Error,
            code: None,
            message: e.to_string(),
            location: None,
            source: source(e.source()),
        })
    })
}

impl<T> From<T> for Exception
where
    T: Error + 'static,
{
    #[inline]
    fn from(error: T) -> Self {
        Exception {
            severity: Severity::Error,
            code: None,
            message: error.to_string(),
            location: None,
            source: source(error.source()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backtrace_with_single() {
        let exception = Exception {
            severity: Severity::Error,
            code: Some("E001".to_owned()),
            message: "bad input".to_owned(),
            location: Some("src/foo.rs:10:5"),
            source: None,
        };
        assert_eq!(exception.backtrace(), "0: ERROR [E001] bad input at src/foo.rs:10:5");
    }

    #[test]
    fn backtrace_with_nested_error() {
        let root = Exception {
            severity: Severity::Error,
            code: None,
            message: "root cause".to_owned(),
            location: Some("src/root.rs:1:1"),
            source: None,
        };
        let middle = Exception {
            severity: Severity::Error,
            code: Some("MID".to_owned()),
            message: "middle".to_owned(),
            location: None,
            source: Some(Box::new(root)),
        };
        let top = Exception {
            severity: Severity::Warn,
            code: Some("TOP".to_owned()),
            message: "top".to_owned(),
            location: Some("src/top.rs:5:5"),
            source: Some(Box::new(middle)),
        };
        assert_eq!(
            top.backtrace(),
            "0: WARN [TOP] top at src/top.rs:5:5
1: ERROR [MID] middle
2: ERROR root cause at src/root.rs:1:1"
        );
    }
}
