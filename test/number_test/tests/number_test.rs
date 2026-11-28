use framework::number::parse_i32;
use framework::number::parse_i64;
use framework::number::parse_u64;

// text input such as path params, headers and env vars, failure is an Exception carrying the value
#[test]
fn parse() {
    assert_eq!(parse_i32("-5").unwrap(), -5);
    assert_eq!(parse_i64("1700000000123").unwrap(), 1_700_000_000_123);
    assert_eq!(parse_u64("18446744073709551615").unwrap(), u64::MAX);
}

#[test]
fn parse_invalid() {
    let _err = parse_i32("2147483648").unwrap_err(); // out of range
    let _err = parse_i64("1.5").unwrap_err();
    let _err = parse_i64(" 1").unwrap_err(); // not trimmed
    let _err = parse_u64("-1").unwrap_err();
    let _err = parse_u64("").unwrap_err();
}
