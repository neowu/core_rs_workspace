use axum::Router;
use http::HeaderName;

pub mod api;
pub mod body;
pub mod client_info;
pub mod error;
pub mod server;

const REF_ID: HeaderName = HeaderName::from_static("ref-id");
const CLIENT: HeaderName = HeaderName::from_static("client");

pub trait SystemRoute<S> {
    fn routes(&self, state: S) -> Router;
}
