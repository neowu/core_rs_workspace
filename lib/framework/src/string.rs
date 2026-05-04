pub trait StringExt {
    fn truncate_to_max(&self, len: usize) -> &str;
}

impl StringExt for str {
    #[inline]
    fn truncate_to_max(&self, len: usize) -> &str {
        if len >= self.len() {
            return self;
        }

        let mut new_len = len;
        while new_len > 0 && !self.is_char_boundary(new_len) {
            new_len -= 1;
        }

        &self[..new_len]
    }
}

#[macro_export]
macro_rules! write_str {
    ($dst:expr, $($arg:tt)*) => {{
        use std::fmt::Write as _;
        write!($dst, $($arg)*).expect("writing to a String cannot fail")
    }};
}

#[cfg(test)]
mod tests {
    use crate::string::StringExt as _;

    #[test]
    fn truncate_to_max() {
        let value = "123老虎456".to_owned();
        assert_eq!(value.truncate_to_max(3), "123");
        assert_eq!(value.truncate_to_max(4), "123");
        assert_eq!(value.truncate_to_max(5), "123");
        assert_eq!(value.truncate_to_max(6), "123老");
        assert_eq!(value.truncate_to_max(7), "123老");
        assert_eq!(value.truncate_to_max(8), "123老");
        assert_eq!(value.truncate_to_max(9), "123老虎");
        assert_eq!(value.truncate_to_max(10), "123老虎4");
    }

    #[test]
    fn write_str() {
        let mut value = String::new();
        write_str!(value, "value={}", 1);
        assert_eq!(value, "value=1");
    }
}
