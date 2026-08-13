use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use demo::AppConfig;
use framework::date::DateTime;
use framework::date::Offset;
use framework::date::SignedDuration;
use framework::exception::Exception;
use framework::load_config;
use framework::log;
use framework::log::trace;
use framework::schedule::JobContext;
use framework::schedule::Scheduler;
use framework::system::System;
use framework::task;
use framework::web::SystemRoute as _;
use framework::web::server::HttpServerConfig;
use framework::web::server::start_http_server;

struct State {}

#[tokio::main]
pub async fn main() -> Result<(), Exception> {
    let config: AppConfig = load_config!("assets/conf.json");
    log::init(&config.log_appender, env!("CARGO_PKG_NAME"));

    let mut system = System::new();

    let state = Arc::new(State {});

    // let hk = Offset::new(8, 0);
    let mut scheduler = Scheduler::new(Offset::UTC);
    scheduler.schedule_fixed_rate("test", job, SignedDuration::from_secs(1));
    scheduler.schedule_daily(
        "test_daily",
        daily_job,
        DateTime::now().add_duration(SignedDuration::from_secs(5))?.time(),
    );
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
    trace();
    println!("Job executed: {}", context.name);
    // sleep(Duration::from_mins(1)).await;
    Ok(())
}

async fn daily_job(_state: Arc<State>, context: JobContext) -> Result<(), Exception> {
    println!("daily executed: {}", context.name);
    Ok(())
}
