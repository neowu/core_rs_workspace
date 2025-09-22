use axum::Router;
use framework::asset::asset_path;
use framework::exception::CoreRsResult;
use framework::log;
use framework::log::ConsoleAppender;
use framework::shutdown::Shutdown;
use framework::web::server::HttpServerConfig;
use framework::web::server::ServeDir;
use framework::web::server::ServeFile;
use framework::web::server::start_http_server;

mod web;

// #[derive(Debug, Deserialize, Clone)]
// struct AppConfig {}

pub struct AppState {}

#[tokio::main]
async fn main() -> CoreRsResult<()> {
    log::init_with_action(ConsoleAppender);

    let shutdown = Shutdown::new();
    let signal = shutdown.subscribe();
    shutdown.listen();

    let state = Box::leak(Box::new(AppState {}));

    let app = Router::new();
    let app = app.merge(web::routes());
    let app = app
        .route_service("/", ServeFile::new(asset_path("assets/web/index.html")?))
        .route_service("/static/{*path}", ServeDir::new(asset_path("assets/web/")?));
    let app = app.with_state(state);
    start_http_server(app, signal, HttpServerConfig::default()).await
}
