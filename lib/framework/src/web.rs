use std::any::Any;
use std::fmt::Debug;

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

fn panic_message(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown")
}
