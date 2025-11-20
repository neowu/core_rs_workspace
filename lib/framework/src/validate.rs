use crate::exception::Exception;

pub trait Validator {
    fn validate(&self) -> Result<(), Exception>;
}
