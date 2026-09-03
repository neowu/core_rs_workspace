use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use framework::appender::ConsoleAppender;
use framework::exception::Exception;
use framework::log;
use framework::schedule::JobContext;
use framework::schedule::Scheduler;
use framework::system::DefaultEnv;
use framework::system::System;
use framework::time::DateTime;
use framework::time::Offset;
use framework::time::SignedDuration;
use framework::web::server::HttpServer;
use framework::web::server::HttpServerConfig;
use tokio::time::sleep;

struct State {}

#[tokio::main]
pub async fn main() {
    let system = System::init(env!("CARGO_BIN_NAME"), DefaultEnv).await.start_logger(ConsoleAppender);

    let state = Arc::new(State {});

    // let hk = Offset::new(8, 0);
    let mut scheduler = Scheduler::new(Offset::UTC);
    scheduler.schedule_fixed_rate("test", job, SignedDuration::from_secs(1));
    scheduler.schedule_daily(
        "test_daily",
        daily_job,
        DateTime::now().add_duration(SignedDuration::from_secs(5)).expect("value must be valid").time(),
    );
    let scheduler_routes = scheduler.routes(state.clone());
    system.start_service(|token| scheduler.start(state, token));

    let app = Router::new();
    let app = app.merge(scheduler_routes);
    let http_server = HttpServer::new(HttpServerConfig::default());
    system.start_service(|token| http_server.start(app, token));

    system.wait().await;
    system.shutdown_logger().await;
}

async fn job(_state: Arc<State>, context: JobContext) -> Result<(), Exception> {
    log::trace();
    println!("Job executed: {}", context.name);
    sleep(Duration::from_secs(20)).await;
    Ok(())
}

async fn daily_job(_state: Arc<State>, context: JobContext) -> Result<(), Exception> {
    println!("daily executed: {}", context.name);
    Ok(())
}
