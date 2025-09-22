use framework::exception::CoreRsResult;
use framework::exception::Severity;
use framework::validation_error;

pub fn main() {
    let error = test().err().unwrap();
    println!("{error:?}");
    let error = test2().err().unwrap();
    println!("{error:?}");
}

fn test() -> CoreRsResult<()> {
    Err(validation_error!(message = "some field is wrong"))
}

fn test2() -> CoreRsResult<()> {
    Err(validation_error!(
        severity = Severity::Error,
        message = "some field is wrong"
    ))
}
