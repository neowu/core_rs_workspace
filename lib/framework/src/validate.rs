use crate::exception::Exception;

pub trait Validator {
    fn validate(&self) -> Result<(), Exception>;
}

#[macro_export]
macro_rules! validation_error {
    ($message:expr $(, severity = $severity:expr)?) => {{
        let result = $crate::exception::Exception::__new(
            $message,
            concat!(file!(), ":", line!(), ":", column!()),
        );
        let result = result.__with_code($crate::exception::error_code::VALIDATION_ERROR).__with_severity($crate::exception::Severity::Warn);
        $( let result = result.__with_severity($severity); )?
        result
    }};
}
