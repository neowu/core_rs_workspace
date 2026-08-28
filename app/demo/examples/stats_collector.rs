use std::time::Duration;

use framework::console;
use framework::exception::Exception;
use framework::log::ConsoleAppender;
use framework::metrics::MetricsCollector;
use framework::spawn_action;
use framework::system::System;
use tokio::time;

#[tokio::main]
async fn main() -> Result<(), Exception> {
    let mut system = System::init(env!("CARGO_PKG_NAME"));
    system.start_action_logger(ConsoleAppender);

    let mut collector = MetricsCollector::new();
    collector.add(system.executor_metrics());
    system.start_metrics_collector(collector, ConsoleAppender);

    for i in 0..10 {
        spawn_action!("sleep", async move {
            time::sleep(Duration::from_secs(20)).await;
            console!("{i}");
            Ok(())
        });
    }

    system.wait().await;
    // the collector ticks every 5s while shutdown drains the spawned actions, so active_tasks shows 10 then drops
    system.shutdown(Duration::from_secs(30)).await
}
