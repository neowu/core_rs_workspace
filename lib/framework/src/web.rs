use std::any::Any;

use http::HeaderName;

pub mod api;
mod file;
pub mod request;
pub mod response;
pub mod router;
pub mod server;
pub mod sse;

const REF_ID: HeaderName = HeaderName::from_static("ref-id");
const CLIENT: HeaderName = HeaderName::from_static("client");

fn panic_message(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown")
}
