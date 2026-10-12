use std::fmt::Debug;
use std::panic::AssertUnwindSafe;

use futures::FutureExt as _;
use http::HeaderName;

use crate::exception::Exception;
use crate::json;

pub mod api;
mod file;
pub mod request;
pub mod response;
pub mod router;
pub mod server;
pub mod sse;

const REF_ID: HeaderName = HeaderName::from_static("ref-id");
const CLIENT: HeaderName = HeaderName::from_static("client");

/// One server-sent event, written by `SseChannel` and read by `EventSource`.
#[derive(Debug)]
pub struct Event {
    pub id: Option<String>,
    pub event_type: Option<String>,
    pub data: String,
}

impl Event {
    pub const fn new(data: String) -> Self {
        Self { id: None, event_type: None, data }
    }

    pub fn json<T>(data: &T) -> Result<Self, Exception>
    where
        T: serde::Serialize + Debug,
    {
        Ok(Self::new(json::to_json(data)?))
    }
}

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
