pub trait StringExt {
    fn truncate_to_max(&self, len: usize) -> &str;
}

impl StringExt for str {
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

#[cfg(test)]
mod tests {
    use crate::string::StringExt;

    #[test]
    fn test_truncate_to_max() {
        let value = "123老虎456".to_owned();
        assert_eq!(value.truncate_to_max(3), "123");
        assert_eq!(value.truncate_to_max(4), "123".to_owned());
        assert_eq!(value.truncate_to_max(5), "123".to_owned());
        assert_eq!(value.truncate_to_max(6), "123老".to_owned());
        assert_eq!(value.truncate_to_max(7), "123老".to_owned());
        assert_eq!(value.truncate_to_max(8), "123老".to_owned());
        assert_eq!(value.truncate_to_max(9), "123老虎".to_owned());
        assert_eq!(value.truncate_to_max(10), "123老虎4".to_owned());
    }
}
