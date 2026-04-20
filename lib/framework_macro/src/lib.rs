mod model;
mod validate;

/**
`#[derive(Validate)]` supports following field validations:
```
#[range(min = 1, max = 10)]    // for Numeric
#[length(max = 10, min = 1)]   // for String, Collections
#[validate]                    // for nested struct
#[not_blank]                   // for String
```
*/
#[proc_macro_derive(Validate, attributes(range, length, validate, not_blank))]
pub fn validate(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    validate::validate_impl(item.into()).unwrap_or_else(|e| e.into_compile_error()).into()
}
