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

/// `GET /sys/action` lists the actions in flight with the trace each has written so far,
/// `GET /sys/thread` dumps the stack of every thread. No access control, serve from a separate http server bound to loopback.
pub fn routes() -> Router {
    Router::state(Arc::new(()))
        .route(Method::GET, "/sys/action", action)
        .route(Method::GET, "/sys/thread", thread)
        .into()
}
