use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use framework::asset_path;
use framework::exception::Exception;
use framework::web::server::ServeDir;
use framework::web::server::ServeFile;
use tokio::time::sleep;

pub(crate) fn routes() -> Result<Router, Exception> {
    let router = Router::new();
    let router = router.route("/503", get(http_503));
    let router = router.route("/long", get(long));
    let router = router
        .route_service("/", ServeFile::new(asset_path!("assets/web/index.html")))
        .route_service("/static/{*path}", ServeDir::new(asset_path!("assets/web/")));
    //     .fallback_service(ServeFile::new(asset_path!("assets/web/index.html")?))
    Ok(router)
}

async fn http_503() -> StatusCode {
    StatusCode::SERVICE_UNAVAILABLE
}

async fn long() -> StatusCode {
    sleep(Duration::from_secs(20)).await;
    StatusCode::OK
}
