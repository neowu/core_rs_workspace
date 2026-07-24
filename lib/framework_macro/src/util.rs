pub(crate) fn to_snake_case(value: &str) -> String {
    let mut result = String::new();
    for (index, ch) in value.char_indices() {
        if ch.is_uppercase() && index > 0 {
            result.push('_');
        }
        result.extend(ch.to_lowercase());
    }
    result
}
