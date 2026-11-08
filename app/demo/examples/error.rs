#![allow(unused_crate_dependencies)]
use framework::console;
use framework::exception::Exception;
use framework::log::Severity;
use framework::validation_error;

pub fn main() {
    let error = test().expect_err("test");
    console!("{error}");
    let error1 = test2().expect_err("test");
    console!("{error1}");
}

fn test() -> Result<(), Exception> {
    Err(validation_error!("some field is wrong"))
}

fn test2() -> Result<(), Exception> {
    Err(validation_error!("some field is wrong", severity = Severity::Error))
}
