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
    pub location: Option<String>,
    pub source: Option<Box<Exception>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Severity {
    #[serde(rename = "WARN")]
    Warn,
    #[serde(rename = "ERROR")]
    Error,
}

impl Exception {
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
            if let Some(ref location) = source.location {
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
        match (&self.severity, self.code.as_ref()) {
            (&Severity::Warn, Some(error_code)) => warn!(error_code, backtrace, "{message}"),
            (&Severity::Warn, None) => warn!(backtrace, "{message}"),
            (&Severity::Error, Some(error_code)) => error!(error_code, backtrace, "{message}"),
            (&Severity::Error, None) => error!(backtrace, "{message}"),
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
        write!(f, "{}", self.message)?;
        Ok(())
    }
}

#[macro_export]
macro_rules! exception {
    ($(severity = $severity:expr,)? $(code = $code:expr,)? message = $message:expr $(,source = $source:expr)?) => {{
        #[allow(unused_variables)]
        let severity = $crate::exception::Severity::Error;
        $(
            let severity = $severity;
        )?
        #[allow(unused_variables)]
        let code: Option<String> = None;
        $(
            let code = Some($code.to_string());
        )?
        #[allow(unused_variables)]
        let source: Option<Box<$crate::exception::Exception>> = None;
        $(
            let source = Some(Box::new($source.into()));
        )?
        $crate::exception::Exception {
            severity,
            code,
            message: $message.to_string(),
            location: Some(format!("{}:{}:{}", file!(), line!(), column!())),
            source,
        }
    }};
}

#[macro_export]
macro_rules! validation_error {
    ($(severity = $severity:expr,)? message = $message:expr) => {{
        #[allow(unused_variables)]
        let severity = $crate::exception::Severity::Warn;
        $(
            let severity = $severity;
        )?
        $crate::exception!(severity = severity, code = $crate::exception::error_code::VALIDATION_ERROR, message = $message)
    }};
}

fn source(source: Option<&(dyn Error + 'static)>) -> Option<Box<Exception>> {
    let mut sources = Vec::new();
    let mut current_source = source;
    while let Some(target) = current_source {
        sources.push(target);
        current_source = target.source();
    }

    let mut result = None;
    for error in sources.into_iter().rev() {
        result = Some(Box::new(Exception {
            severity: Severity::Error,
            code: None,
            message: error.to_string(),
            location: None,
            source: result,
        }));
    }
    result
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
            location: Some("src/foo.rs:10:5".to_owned()),
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
            location: Some("src/root.rs:1:1".to_owned()),
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
            location: Some("src/top.rs:5:5".to_owned()),
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
