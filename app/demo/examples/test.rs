use framework::exception::Exception;
use framework::validate::Validator;
use framework_validator::Validate;

#[derive(Validate, Debug)]
struct Example1 {
    #[validate(length(max = 30))]
    name: String,
    #[validate(range(max = 4, min = 1))]
    age: u32,
    #[validate(range(max = 10))]
    age2: Option<i32>,
    #[validate(length(max = 10))]
    context: Vec<u32>,
    #[validate(length(max = 3))]
    last_name: Option<String>,
}

fn main() -> Result<(), Exception> {
    // let x = Example.into();
    let x = Example1 {
        name: "Example".to_owned(),
        age: 4,
        context: vec![1, 2, 3, 4],
        age2: Some(4),
        last_name: Some("hello".to_owned()),
    };
    x.validate()?;

    dbg!(x);
    Ok(())
}
