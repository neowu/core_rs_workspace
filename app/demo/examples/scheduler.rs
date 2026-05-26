use std::sync::Arc;
use std::time::Duration;

use chrono::FixedOffset;
use chrono::NaiveTime;
use framework::exception::Exception;
use framework::log;
use framework::schedule::JobContext;
use framework::schedule::Scheduler;
use framework::shutdown::listen_shutdown_signal;
use tracing::warn;

struct State {}

#[tokio::main]
pub async fn main() -> Result<(), Exception> {
    log::init();
    log::init_action_log_appender("console", env!("CARGO_BIN_NAME"))?;

    let shutdown_signal = listen_shutdown_signal();

    let mut scheduler = Scheduler::new(FixedOffset::east_opt(8 * 60 * 60).unwrap());
    scheduler.schedule_fixed_rate("test", job, Duration::from_secs(1));
    scheduler.schedule_daily("test_daily", daily_job, NaiveTime::from_hms_opt(17, 28, 50).unwrap());
    scheduler.start(Arc::new(State {}), shutdown_signal).await
}

async fn job(_state: Arc<State>, context: JobContext) -> Result<(), Exception> {
    warn!("test");
    println!("Job executed: {}", context.name);
    Ok(())
}

async fn daily_job(_state: Arc<State>, context: JobContext) -> Result<(), Exception> {
    println!("daily executed: {}", context.name);
    Ok(())
}
