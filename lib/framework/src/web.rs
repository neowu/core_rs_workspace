use std::panic::AssertUnwindSafe;

use futures::FutureExt as _;
use http::HeaderName;

use crate::exception::Exception;

pub mod api;
pub mod diagnostic;
mod file;
pub mod request;
pub mod response;
pub mod router;
pub mod server;
pub mod sse;

const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");

const REF_ID: HeaderName = HeaderName::from_static("ref-id");
const CLIENT: HeaderName = HeaderName::from_static("client");

// only catches panics while polling, handlers are async fn / async closures, invoking one only creates the future
async fn catch_panic<T>(future: impl Future<Output = Result<T, Exception>>) -> Result<T, Exception> {
    AssertUnwindSafe(future).catch_unwind().await.unwrap_or_else(|panic| {
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("unknown");
        Err(exception!(format!("handler panicked, error={message}")))
    })
}
