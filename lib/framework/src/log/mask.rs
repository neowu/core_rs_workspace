use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

// sensitive fields must be named to contain one of these, matched as ascii case-insensitive substring
const MASKED_KEYS: &[&str] = &["authorization", "cookie", "password", "secret", "token", "session", "api-key"];

const MASKED: &str = "**masked**";

/// Formats only the value, or `**masked**` when the key is sensitive.
pub struct LogValue<'a, V> {
    key: &'a str,
    value: V,
}

impl<'a, V> LogValue<'a, V> {
    pub const fn new(key: &'a str, value: V) -> Self {
        LogValue { key, value }
    }
}

impl<V: Display> Display for LogValue<'_, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if is_masked(self.key) { f.write_str(MASKED) } else { self.value.fmt(f) }
    }
}

impl<V: Debug> Debug for LogValue<'_, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if is_masked(self.key) { f.write_str(MASKED) } else { self.value.fmt(f) }
    }
}

fn is_masked(key: &str) -> bool {
    let key = key.as_bytes();
    MASKED_KEYS
        .iter()
        .any(|masked| key.windows(masked.len()).any(|window| window.eq_ignore_ascii_case(masked.as_bytes())))
}

#[cfg(test)]
mod tests {
    use super::LogValue;
    use super::is_masked;

    #[test]
    fn masked_keys() {
        for key in ["authorization", "proxy-authorization", "set-cookie", "x-api-key", "access_token", "JSESSIONID"] {
            assert!(is_masked(key), "{key}");
        }
        for key in ["content-type", "user-agent", "idempotency-key", ""] {
            assert!(!is_masked(key), "{key}");
        }
    }

    #[test]
    fn format_value() {
        assert_eq!(LogValue::new("authorization", "Bearer x").to_string(), "**masked**");
        assert_eq!(format!("{:?}", LogValue::new("authorization", "Bearer x")), "**masked**");
        assert_eq!(LogValue::new("content-type", "text/plain").to_string(), "text/plain");
        assert_eq!(format!("{:?}", LogValue::new("content-type", "text/plain")), r#""text/plain""#);
    }
}
