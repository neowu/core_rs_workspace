use std::sync::Arc;
use std::time::Duration;

use framework::asset_path;
use framework::exception::Exception;
use framework::http::Method;
use framework::http::StatusCode;
use framework::log::trace;
use framework::web::request::Request;
use framework::web::response::Response;
use framework::web::router::Router;
use tokio::time::sleep;

pub(crate) fn routes() -> Router {
    Router::new()
        .file("/", asset_path!("assets/web/index.html"))
        .dir("/static/", asset_path!("assets/web/"))
        .state(Arc::new(()))
        .route(Method::GET, "/503", http_503)
        .route(Method::GET, "/long", long)
        .into()
}

async fn http_503(_state: Arc<()>, _request: Request) -> Result<Response, Exception> {
    trace();
    Ok(Response::empty().status(StatusCode::SERVICE_UNAVAILABLE))
}

async fn long(_state: Arc<()>, _request: Request) -> Result<Response, Exception> {
    sleep(Duration::from_secs(20)).await;
    Ok(Response::empty().status(StatusCode::OK))
}
