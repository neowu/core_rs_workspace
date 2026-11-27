pub(super) fn item() {}

#[cfg(test)]
mod tests {
    #[test]
    fn item() {
        super::super::a::item();
    }
}
