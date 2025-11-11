use std::time::Duration;

use axum::Router;
use chrono::FixedOffset;
use framework::asset::asset_path;
use framework::exception::Exception;
use framework::log;
use framework::log::ConsoleAppender;
use framework::schedule::Scheduler;
use framework::shutdown::Shutdown;
use framework::task;
use framework::web::server::HttpServerConfig;
use framework::web::server::ServeDir;
use framework::web::server::ServeFile;
use framework::web::server::start_http_server;

use crate::job::demo_job;

mod job;
mod web;

// #[derive(Debug, Deserialize, Clone)]
// struct AppConfig {}

pub struct AppState {}

#[tokio::main]
async fn main() -> Result<(), Exception> {
    log::init_with_action(ConsoleAppender);

    let shutdown = Shutdown::new();
    let signal = shutdown.subscribe();
    let scheduler_signal = shutdown.subscribe();
    shutdown.listen();

    let state: &'static AppState = Box::leak(Box::new(AppState {}));

    task::spawn_task(async move {
        let mut scheduler = Scheduler::new(FixedOffset::east_opt(8 * 60 * 60).unwrap());
        scheduler.schedule_fixed_rate("demo_job", demo_job, Duration::from_hours(5));
        scheduler.start(state, scheduler_signal).await
    });

    let app = Router::new();
    let app = app.merge(web::routes());
    let app = app.merge(job::routes());
    let app = app
        .route_service("/", ServeFile::new(asset_path("assets/web/index.html")?))
        .route_service("/static/{*path}", ServeDir::new(asset_path("assets/web/")?))
        .fallback_service(ServeFile::new(asset_path("assets/web/index.html")?));
    let app = app.with_state(state);
    start_http_server(app, signal, HttpServerConfig::default()).await
}
