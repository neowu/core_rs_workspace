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

pub(crate) fn to_pascal_case(value: &str) -> String {
    value
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(ch) => ch.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}
