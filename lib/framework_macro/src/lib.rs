use syn::Error;

mod api;
mod entity;
mod enum8;
mod model;
mod nats_api;
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

/// Derive `framework_db::Entity` for a struct, plus a `FIELD_<NAME>` const per column.
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

/// Derive serde `Serialize`/`Deserialize` for a fieldless enum mapped to a clickhouse Enum8 column;
/// serialized as the i8 discriminant, e.g. `Ok = 1` <-> `1`.
#[proc_macro_derive(Enum8)]
pub fn enum8(stream: proc_macro::TokenStream) -> proc_macro::TokenStream {
    enum8::build(stream.into()).unwrap_or_else(Error::into_compile_error).into()
}

/// `#[api]` derives an axum route builder and an HTTP client from a trait.
/// Each method must be `async fn`, annotated with one of `#[get]`, `#[post]`, `#[put]` plus `#[path("/...")]`,
/// take `&self` and a single request parameter, and return `Result<..., Exception>`.
/// Adds a `route(service)` associated fn to the trait, and generates a sibling `<Trait>Client` struct
/// implementing the trait, both with the trait's own visibility.
/// ```text
/// let router = UserService::route(Arc::new(service));
/// let client = UserServiceClient::new(http_client, api_url, client);
/// ```
#[proc_macro_attribute]
pub fn api(_attr: proc_macro::TokenStream, item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    api::build(item.into()).unwrap_or_else(Error::into_compile_error).into()
}

/// `#[nats_api]` derives a NATS request/reply service builder and client from a trait.
/// Each method must be `async fn`, annotated with `#[subject = "..."]`,
/// take `&self` and at most one request parameter, and return `Result<..., Exception>`.
/// Adds a `service(nats_client, service)` associated fn to the trait, and generates a sibling `<Trait>Client`
/// struct implementing the trait, both with the trait's own visibility.
/// ```text
/// let service = GreetingService::service(nats_client.clone(), Arc::new(service));
/// let client = GreetingServiceClient::new(nats_client, client);
/// ```
#[proc_macro_attribute]
pub fn nats_api(_attr: proc_macro::TokenStream, item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    nats_api::build(item.into()).unwrap_or_else(Error::into_compile_error).into()
}
