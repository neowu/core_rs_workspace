#[cfg(feature = "db")]
mod entity;
mod model;
mod validate;

/// `#[derive(Validate)]` supports following field validations:
/// ```
/// #[range(min = 1, max = 10)]    // for Numeric
/// #[length(max = 10, min = 1)]   // for String, Collections
/// #[validate]                    // for nested struct
/// #[not_blank]                   // for String
/// ```
#[proc_macro_derive(Validate, attributes(range, length, validate, not_blank))]
pub fn validate(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    validate::validate_impl(item.into()).unwrap_or_else(|e| e.into_compile_error()).into()
}

/// Derive `framework::db::Entity<T>` for a struct.
/// struct attributes
/// ```
/// #[table(name = "table_name")]
/// ```
/// field attributes
/// ```
/// #[primary_key(auto_increment)]  // auto increment pk, excluded from INSERT, must be `Option<i64>`, only one allowed
/// #[primary_key]                  // assigned pk, included in INSERT
/// #[column(name = "column_name")]
/// ```
#[cfg(feature = "db")]
#[proc_macro_derive(Entity, attributes(table, column, primary_key))]
pub fn entity(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    entity::entity_impl(item.into()).unwrap_or_else(|e| e.into_compile_error()).into()
}
