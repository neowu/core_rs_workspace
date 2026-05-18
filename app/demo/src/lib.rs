use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use chrono::FixedOffset;
use framework::asset_path;
use framework::config::EnvString;
use framework::exception::Exception;
use framework::json;
use framework::log;
use framework::log::appender::ConsoleAppender;
use framework::schedule::Scheduler;
use framework::shutdown::Shutdown;
use framework::task;
use framework::web::server::HttpServerConfig;
use framework::web::server::ServeDir;
use framework::web::server::ServeFile;
use framework::web::server::start_http_server;
use serde::Deserialize;

use crate::job::demo_job;

mod job;
pub mod web;

pub struct AppState {}

#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub db_url: String,
    pub db_user: String,
    pub db_password: EnvString,
}

#[inline]
pub async fn run() -> Result<(), Exception> {
    log::init_with_action(ConsoleAppender);

    let _config: AppConfig = json::load_file(&asset_path!("assets/conf.json")?)?;

    let shutdown = Shutdown::new();
    let signal = shutdown.subscribe();
    let scheduler_signal = shutdown.subscribe();
    shutdown.listen();

    let state: &'static AppState = Box::leak(Box::new(AppState {}));

    task::spawn_task(async move {
        let mut scheduler = Scheduler::new(FixedOffset::east_opt(8 * 60 * 60).unwrap());
        scheduler.schedule_fixed_rate("demo_job", demo_job, Duration::from_hours(1));
        scheduler.start(state, scheduler_signal).await
    });

    let app = Router::new();
    let app = app.merge(job::routes(state));
    let app = app.merge(web::routes(state));
    let app = app.merge(Router::new().route("/503", get(http_503)));
    let app = app
        .route_service("/", ServeFile::new(asset_path!("assets/web/index.html")?))
        .route_service("/static/{*path}", ServeDir::new(asset_path!("assets/web/")?));
    //     .fallback_service(ServeFile::new(asset_path!("assets/web/index.html")?));
    start_http_server(app, signal, HttpServerConfig::default()).await?;

    task::shutdown().await;

    Ok(())
}

async fn http_503() -> StatusCode {
    StatusCode::SERVICE_UNAVAILABLE
}
