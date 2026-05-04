use crate::exception::Exception;

#[inline]
pub fn parse_u32(value: &str) -> Result<u32, Exception> {
    value.parse::<u32>().map_err(|err| exception!(message = format!("failed to parse, value={value}"), source = err))
}

#[cfg(test)]
mod tests {
    use crate::number::parse_u32;

    #[test]
    fn parse_u32_invalid() {
        parse_u32("invalid").unwrap_err();
    }
}
