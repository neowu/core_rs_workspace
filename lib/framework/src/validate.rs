use crate::exception::Exception;

pub trait Validator {
    fn validate(&self) -> Result<(), Exception>;
}

impl<T: Validator> Validator for Option<T> {
    fn validate(&self) -> Result<(), Exception> {
        if let Some(value) = self {
            value.validate()?;
        }
        Ok(())
    }
}

impl<T: Validator> Validator for Vec<T> {
    fn validate(&self) -> Result<(), Exception> {
        for value in self {
            value.validate()?;
        }
        Ok(())
    }
}

#[macro_export]
macro_rules! validation_error {
    ($message:expr $(, severity = $severity:expr)?) => {{
        let result = $crate::exception::Exception::__new(
            $message,
            concat!(file!(), ":", line!(), ":", column!()),
        );
        let result = result.__with_code($crate::exception::error_code::VALIDATION_ERROR).__with_severity($crate::log::Severity::Warn);
        $( let result = result.__with_severity($severity); )?
        result
    }};
}
