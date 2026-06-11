use crate::exception::Exception;

#[inline]
pub fn parse_u64(value: &str) -> Result<u64, Exception> {
    value.parse::<u64>().map_err(|err| exception!(format!("failed to parse, value={value}"), source = err))
}

#[cfg(test)]
mod tests {
    use crate::number::parse_u64;

    #[test]
    fn parse_u64_invalid() {
        parse_u64("invalid").unwrap_err();
    }
}
