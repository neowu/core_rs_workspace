use std::time::Duration;

use axum::Router;
use framework::config::EnvString;
use framework::load_config;
use framework::schedule::Scheduler;
use framework::system::DefaultEnv;
use framework::system::System;
use framework::task::start_executor;
use framework::time::Offset;
use framework::time::SignedDuration;
use framework::web::server::HttpServer;
use framework::web::server::HttpServerConfig;
use framework_db::Database;
use framework_db::DbConfig;
use framework_nats::appender::NatsAppender;
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
    pub nats_appender_url: String,
    pub db_url: String,
    pub db_user: String,
    pub db_password: EnvString,
}

#[inline]
pub async fn run() {
    let config: AppConfig = load_config!("assets/conf.json");

    let mut system = System::init(env!("CARGO_PKG_NAME"), DefaultEnv).await;
    let executor = start_executor();

    system.add_metrics(executor.metrics());

    let db = Database::new(DbConfig {
        uri: config.db_url,
        user: config.db_user,
        password: config.db_password.into(),
        client: env!("CARGO_PKG_NAME"),
    });
    system.add_metrics(db.metrics());

    let state: &'static AppState = Box::leak(Box::new(AppState { db }));

    let mut scheduler = Scheduler::new(Offset::new(8, 0));
    scheduler.schedule_fixed_rate("demo", demo_job, SignedDuration::from_hours(1));
    let scheduler_routes = scheduler.routes(state);

    let app = Router::new();
    let app = app.merge(scheduler_routes);
    let app = app.merge(user::web::routes(state));
    let app = app.merge(web::routes());
    let http_server = HttpServer::new(HttpServerConfig { shutdown_grace_period: Duration::ZERO, ..Default::default() });
    system.add_metrics(http_server.metrics());

    let system = system.start_logger(NatsAppender::new(&config.nats_appender_url).await);
    system.start_service(|token| scheduler.start(state, token));
    system.start_service(|token| http_server.start(app, token));

    system.wait().await;
    executor.shutdown(Duration::from_secs(15)).await;
    system.shutdown_logger().await;
}
