use std::sync::Arc;

use bytes::Bytes;
use http::Method;

use crate::exception::Exception;
use crate::log::inflight;
use crate::thread;
use crate::web::request::Request;
use crate::web::response::Response;
use crate::web::router::Router;

async fn action(_state: Arc<()>, _request: Request) -> Result<Response, Exception> {
    Ok(Response::chunks(inflight::dump().map(Bytes::from)))
}

// not spawn_blocking, the dump has to work when the blocking pool is stuck
async fn thread(_state: Arc<()>, _request: Request) -> Result<Response, Exception> {
    Ok(Response::text(thread::dump()))
}

/// `GET /_sys/action` lists the actions in flight with the trace each has written so far,
/// `GET /_sys/thread` dumps the stack of every thread. No access control, the http server serving it must bind loopback.
pub fn routes() -> Router {
    Router::state(Arc::new(()))
        .route(Method::GET, "/_sys/action", action)
        .route(Method::GET, "/_sys/thread", thread)
        .into()
}
