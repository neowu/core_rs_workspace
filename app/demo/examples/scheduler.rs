use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use chrono::FixedOffset;
use chrono::NaiveTime;
use framework::exception::Exception;
use framework::log;
use framework::schedule::JobContext;
use framework::schedule::Scheduler;
use framework::system::System;
use framework::task;
use framework::web::SystemRoute as _;
use framework::web::server::HttpServerConfig;
use framework::web::server::start_http_server;
use tokio::time::sleep;
use tracing::warn;

struct State {}

#[tokio::main]
pub async fn main() -> Result<(), Exception> {
    log::init();
    log::init_appender("console", env!("CARGO_BIN_NAME"))?;

    let mut system = System::new();

    let state = Arc::new(State {});

    let mut scheduler = Scheduler::new(FixedOffset::east_opt(8 * 60 * 60).unwrap());
    scheduler.schedule_fixed_rate("test", job, Duration::from_secs(1));
    scheduler.schedule_daily("test_daily", daily_job, NaiveTime::from_hms_opt(17, 28, 50).unwrap());
    let scheduler_routes = scheduler.routes(state.clone());
    system.spawn(scheduler.start(state, system.shutdown_signal()));

    let app = Router::new();
    let app = app.merge(scheduler_routes);
    system.spawn(start_http_server(app, system.shutdown_signal(), HttpServerConfig::default()));

    system.wait().await;
    task::shutdown(Duration::from_secs(5)).await;
    Ok(())
}

async fn job(_state: Arc<State>, context: JobContext) -> Result<(), Exception> {
    warn!("test");
    println!("Job executed: {}", context.name);
    sleep(Duration::from_mins(1)).await;
    Ok(())
}

async fn daily_job(_state: Arc<State>, context: JobContext) -> Result<(), Exception> {
    println!("daily executed: {}", context.name);
    Ok(())
}
