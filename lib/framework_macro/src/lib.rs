use syn::Error;

mod api;
mod entity;
mod model;
mod row;
mod util;
mod validate;

/// `#[derive(Validate)]` supports following field validations:
/// ```
/// #[range(min = 1, max = 10)]    // for Numeric
/// #[length(max = 10, min = 1)]   // for String, Collections
/// #[validate]                    // for nested struct
/// #[not_blank]                   // for String
/// ```
#[proc_macro_derive(Validate, attributes(range, length, validate, not_blank))]
pub fn validate(stream: proc_macro::TokenStream) -> proc_macro::TokenStream {
    validate::build(stream.into()).unwrap_or_else(Error::into_compile_error).into()
}

/// Derive `framework_db::Entity<T>` for a struct.
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
#[proc_macro_derive(Entity, attributes(table, column, primary_key))]
pub fn entity(stream: proc_macro::TokenStream) -> proc_macro::TokenStream {
    entity::build(stream.into()).unwrap_or_else(Error::into_compile_error).into()
}

/// Derive `clickhouse::Row` and `framework_clickhouse::Table` for a ClickHouse row struct.
/// struct attributes
/// ```
/// #[table(name = "table_name")]
/// ```
/// field attributes
/// ```
/// #[column(name = "column_name")]
/// ```
/// `#[serde(rename)]`/`#[serde(skip)]` are not honored, and only owned structs with named fields are supported.
#[proc_macro_derive(Row, attributes(table, column))]
pub fn row(stream: proc_macro::TokenStream) -> proc_macro::TokenStream {
    row::build(stream.into()).unwrap_or_else(Error::into_compile_error).into()
}

/// `#[api]` derives an axum route builder and an HTTP client from a trait.
/// Each method must be `async fn`, annotated with one of `#[get]`, `#[post]`, `#[put]` plus `#[path("/...")]`,
/// take `&self` and a single request parameter, and return `Result<..., Exception>`.
/// Generates a sibling module (snake_case of the trait name) exposing `route(service)` and `client(http_client, api_url)`.
#[proc_macro_attribute]
pub fn api(_attr: proc_macro::TokenStream, item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    api::build(item.into()).unwrap_or_else(Error::into_compile_error).into()
}
