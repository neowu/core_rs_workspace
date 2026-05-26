use std::time::Duration;

use axum::Router;
use chrono::FixedOffset;
use framework::asset_path;
use framework::config::EnvString;
use framework::exception::Exception;
use framework::json;
use framework::load_env;
use framework::log;
use framework::schedule::Scheduler;
use framework::schedule::controller::SystemRoute as _;
use framework::shutdown::listen_shutdown_signal;
use framework::task;
use framework::web::server::HttpServerConfig;
use framework::web::server::start_http_server;
use framework_db::Database;
use framework_db::DbConfig;
use serde::Deserialize;

use crate::job::demo_job;

mod job;
pub mod user;
mod web;

pub struct AppState {
    db: Database,
}

#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub db_url: String,
    pub db_user: String,
    pub db_password: EnvString,
}

#[inline]
pub async fn run() -> Result<(), Exception> {
    log::init();
    log::init_action_log_appender("gcloud", env!("CARGO_PKG_NAME"))?;
    load_env!(".env")?;

    let config: AppConfig = json::load_file(&asset_path!("assets/conf.json")?)?;

    let shutdown_signal = listen_shutdown_signal();

    let db = Database::new(DbConfig {
        uri: config.db_url,
        user: config.db_user,
        password: config.db_password.into(),
        client: env!("CARGO_PKG_NAME"),
    })?;

    let state: &'static AppState = Box::leak(Box::new(AppState { db }));

    let scheduler_signal = shutdown_signal.clone();
    let mut scheduler = Scheduler::new(FixedOffset::east_opt(8 * 60 * 60).expect("cannot fail"));
    scheduler.schedule_fixed_rate("demo", demo_job, Duration::from_hours(1));
    let scheduler_routes = scheduler.routes(state);
    tokio::spawn(async move { scheduler.start(state, scheduler_signal).await });

    let app = Router::new();
    let app = app.merge(scheduler_routes);
    let app = app.merge(user::web::routes(state));
    let app = app.merge(web::routes()?);
    start_http_server(app, shutdown_signal, HttpServerConfig::default()).await?;

    task::shutdown().await;

    Ok(())
}
