use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

// matched by equality, header names are always lowercase, add names here when needed
const MASKED_KEYS: &[&str] = &["authorization"];

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

impl<V: Debug> Debug for LogValue<'_, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if is_masked(self.key) { f.write_str("**masked**") } else { self.value.fmt(f) }
    }
}

fn is_masked(key: &str) -> bool {
    MASKED_KEYS.contains(&key)
}

#[cfg(test)]
mod tests {
    use super::LogValue;
    use super::is_masked;

    #[test]
    fn masked_keys() {
        assert!(is_masked("authorization"));
        for key in ["content-type", "proxy-authorization", "cookie", ""] {
            assert!(!is_masked(key), "{key}");
        }
    }

    #[test]
    fn format_value() {
        assert_eq!(format!("{:?}", LogValue::new("authorization", "Bearer x")), "**masked**");
        assert_eq!(format!("{:?}", LogValue::new("content-type", "text/plain")), r#""text/plain""#);
    }
}
