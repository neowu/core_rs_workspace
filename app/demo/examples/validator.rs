use framework::exception::Exception;
use framework::validate::Validator;
use framework_macro::Validate;

#[derive(Validate, Debug)]
struct Example1 {
    #[length(max = 30)]
    name: String,
    #[range(max = 4, min = 1)]
    age: u32,
    #[range(max = 10)]
    age2: Option<i32>,
    #[length(max = 10)]
    context: Vec<u32>,
    #[not_blank]
    #[length(max = 5)]
    last_name: String,

    #[validate]
    child: Child1,
    #[validate]
    optional_child: Option<Child1>,
}

#[derive(Validate, Debug)]
struct Child1 {
    #[length(max = 10)]
    name: String,
}

fn main() -> Result<(), Exception> {
    // let x = Example.into();
    let x = Example1 {
        name: "Example".to_owned(),
        age: 4,
        context: vec![1, 2, 3, 4],
        age2: Some(4),
        last_name: "   ".to_owned(),
        child: Child1 { name: "hello".to_owned() },
        optional_child: Some(Child1 { name: "12345678901".to_owned() }),
    };
    x.validate()?;

    dbg!(x);
    Ok(())
}
