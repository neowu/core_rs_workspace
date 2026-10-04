use framework::exception::Exception;
use framework::validate::Validator;
use framework_macro::Validate;

#[derive(Validate)]
struct Bean {
    #[range(min = -5, max = 10)]
    rating: Option<i32>,
    #[not_blank]
    #[length(min = 1, max = 2)]
    name: std::string::String,
    #[length(max = 2)]
    tags: Vec<u8>,
    #[validate]
    child: Child,
    #[validate]
    children: Vec<Option<Child>>,
}

#[derive(Validate)]
struct Child {
    #[length(max = 1)]
    name: String,
}

// an inherent method of the same name must not shadow the derived validator of a nested field
#[allow(dead_code)]
impl Child {
    const fn validate(&self) -> Result<(), Exception> {
        Ok(())
    }
}

#[derive(Validate)]
struct Page<'a, T: Validator> {
    #[length(max = 1)]
    name: &'a str,
    #[validate]
    items: Vec<T>,
}

// generated code must compile with a caller `Result<T>` alias in scope
#[allow(dead_code)]
mod result_alias {
    use framework_macro::Validate;

    type Result<T> = std::result::Result<T, ()>;

    #[derive(Validate)]
    struct Bean {
        #[range(max = 1)]
        value: i32,
    }
}

fn bean() -> Bean {
    Bean {
        rating: Some(10),
        name: "界界".to_owned(),
        tags: vec![1, 2],
        child: child("a"),
        children: vec![None, Some(child("界"))],
    }
}

fn child(name: &str) -> Child {
    Child { name: name.to_owned() }
}

fn error(validator: &impl Validator) -> Option<String> {
    Validator::validate(validator).err().map(|error| error.message)
}

#[test]
fn validate() {
    assert_eq!(error(&bean()), None);
    assert_eq!(error(&Bean { rating: None, ..bean() }), None);
    assert_eq!(error(&Bean { rating: Some(-6), ..bean() }).unwrap(), "rating must not be less than -5, value=-6");
    assert_eq!(error(&Bean { rating: Some(11), ..bean() }).unwrap(), "rating must not be greater than 10, value=11");
    assert_eq!(error(&Bean { name: " \u{3000}".to_owned(), ..bean() }).unwrap(), "name must not be blank");
    assert_eq!(
        error(&Bean { name: "界界界".to_owned(), ..bean() }).unwrap(),
        "name length must not be greater than 2, value=3"
    );
    assert_eq!(
        error(&Bean { tags: vec![1, 2, 3], ..bean() }).unwrap(),
        "tags length must not be greater than 2, value=3"
    );
    assert_eq!(
        error(&Bean { child: child("ab"), ..bean() }).unwrap(),
        "name length must not be greater than 1, value=2"
    );
    assert_eq!(
        error(&Bean { children: vec![Some(child("ab"))], ..bean() }).unwrap(),
        "name length must not be greater than 1, value=2"
    );
}

#[test]
fn validate_first_error() {
    assert_eq!(
        error(&Bean { rating: Some(11), name: String::new(), ..bean() }).unwrap(),
        "rating must not be greater than 10, value=11"
    );
    assert_eq!(error(&Bean { name: String::new(), ..bean() }).unwrap(), "name length must not be less than 1, value=0");
}

#[test]
fn validate_generic() {
    assert_eq!(error(&Page { name: "界", items: vec![child("a")] }), None);
    assert_eq!(
        error(&Page { name: "ab", items: vec![child("a")] }).unwrap(),
        "name length must not be greater than 1, value=2"
    );
    assert_eq!(
        error(&Page { name: "a", items: vec![child("ab")] }).unwrap(),
        "name length must not be greater than 1, value=2"
    );
}
