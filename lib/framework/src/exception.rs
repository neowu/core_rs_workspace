use std::error::Error;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

pub mod error_code;

pub type CoreRsResult<T> = Result<T, Exception>;

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

impl Display for Severity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Warn => write!(f, "WARN"),
            Severity::Error => write!(f, "ERROR"),
        }
    }
}

impl Debug for Exception {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for Exception {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut index = 0;
        let mut current_source = Some(self);
        while let Some(source) = current_source {
            if index > 0 {
                writeln!(f)?;
            }
            write!(f, "{index}: {} ", source.severity)?;
            if let Some(ref code) = source.code {
                write!(f, "[{code}] ")?;
            }
            write!(f, "{}", source.message)?;
            if let Some(ref location) = source.location {
                write!(f, " at {location}")?;
            }
            index += 1;
            current_source = source.source.as_ref().map(|s| s.as_ref());
        }
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
