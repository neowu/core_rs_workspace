mod date;
mod datetime;
mod decimal;

pub use date::Date16;
pub use datetime::DateTime64;
pub use decimal::Decimal64;

// the Enum8 derive lives in framework_macro, a proc macro crate can't use its own derive,
// so its serde behaviour is covered here
#[cfg(test)]
mod tests {
    use framework::json;
    use framework_macro::Enum8;

    // Enum8('OK' = 1, 'ERROR' = -2)
    #[derive(Enum8, Debug, PartialEq)]
    enum TestResult {
        Ok = 1,
        Error = -2,
    }

    #[test]
    fn enum8_serde_i8() {
        assert_eq!(json::to_json(&TestResult::Ok).unwrap(), "1");
        assert_eq!(json::to_json(&TestResult::Error).unwrap(), "-2");
        assert_eq!(json::from_json::<TestResult>("1").unwrap(), TestResult::Ok);
        assert_eq!(json::from_json::<TestResult>("-2").unwrap(), TestResult::Error);
        let error = json::from_json::<TestResult>("3").unwrap_err();
        assert!(error.to_string().starts_with("failed to deserialize, json=3"));
    }
}
