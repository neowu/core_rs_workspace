use crate::exception::Exception;

pub fn parse_u32(value: &str) -> Result<u32, Exception> {
    value.parse::<u32>().map_err(|err| exception!(message = format!("failed to parse, value={}", value), source = err))
}

#[cfg(test)]
mod tests {
    use crate::number::parse_u32;

    #[test]
    fn test_parse_u32() {
        assert!(parse_u32("invalid").is_err())
    }
}
