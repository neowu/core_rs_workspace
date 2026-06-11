use std::time::Duration;

use axum::Router;
use chrono::FixedOffset;
use framework::config::EnvString;
use framework::exception::Exception;
use framework::load_config;
use framework::log;
use framework::log::metrics::MetricsCollector;
use framework::schedule::Scheduler;
use framework::system::System;
use framework::task;
use framework::task::task_collector;
use framework::web::SystemRoute as _;
use framework::web::server::HttpServerConfig;
use framework::web::server::http_server_collector;
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
    pub log_appender: String,
    pub db_url: String,
    pub db_user: String,
    pub db_password: EnvString,
}

#[inline]
pub async fn run() -> Result<(), Exception> {
    let config: AppConfig = load_config!("assets/conf.json");
    log::init(&config.log_appender, env!("CARGO_PKG_NAME"));

    let mut system = System::new();

    let db = Database::new(DbConfig {
        uri: config.db_url,
        user: config.db_user,
        password: config.db_password.into(),
        client: env!("CARGO_PKG_NAME"),
    })?;

    let state: &'static AppState = Box::leak(Box::new(AppState { db }));

    let mut scheduler = Scheduler::new(FixedOffset::east_opt(8 * 60 * 60).expect("cannot fail"));
    scheduler.schedule_fixed_rate("demo", demo_job, Duration::from_hours(1));
    let scheduler_routes = scheduler.routes(state);
    system.spawn(scheduler.start(state, system.shutdown_signal()));

    let app = Router::new();
    let app = app.merge(scheduler_routes);
    let app = app.merge(user::web::routes(state));
    let app = app.merge(web::routes()?);
    system.spawn(start_http_server(
        app,
        system.shutdown_signal(),
        HttpServerConfig { shutdown_grace_period: Duration::ZERO, ..Default::default() },
    ));

    let mut collector = MetricsCollector::new();
    collector.add(task_collector());
    collector.add(http_server_collector());
    collector.start_collect_task();

    system.wait().await;
    task::shutdown(Duration::from_secs(15)).await;

    Ok(())
}
