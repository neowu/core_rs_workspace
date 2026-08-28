use std::time::Duration;

use axum::Router;
use framework::config::EnvString;
use framework::exception::Exception;
use framework::load_config;
use framework::log::ConsoleAppender;
use framework::metrics::MetricsCollector;
use framework::schedule::Scheduler;
use framework::system::System;
use framework::time::Offset;
use framework::time::SignedDuration;
use framework::web::server::HttpServer;
use framework::web::server::HttpServerConfig;
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

    let mut system = System::init(env!("CARGO_PKG_NAME"));
    system.start_action_logger(ConsoleAppender);

    let mut collector = MetricsCollector::new();

    let db = Database::new(DbConfig {
        uri: config.db_url,
        user: config.db_user,
        password: config.db_password.into(),
        client: env!("CARGO_PKG_NAME"),
    })?;
    collector.add(db.metrics());

    collector.add(system.executor_metrics());

    let state: &'static AppState = Box::leak(Box::new(AppState { db }));

    let mut scheduler = Scheduler::new(Offset::new(8, 0));
    scheduler.schedule_fixed_rate("demo", demo_job, SignedDuration::from_hours(1));
    let scheduler_routes = scheduler.routes(state);
    system.start_service(|token| scheduler.start(state, token));

    let app = Router::new();
    let app = app.merge(scheduler_routes);
    let app = app.merge(user::web::routes(state));
    let app = app.merge(web::routes()?);
    let http_server = HttpServer::new(HttpServerConfig { shutdown_grace_period: Duration::ZERO, ..Default::default() });
    collector.add(http_server.metrics());
    system.start_service(|token| http_server.start(app, token));

    system.start_metrics_collector(collector, ConsoleAppender);

    system.wait().await;
    system.shutdown(Duration::from_secs(15)).await
}
