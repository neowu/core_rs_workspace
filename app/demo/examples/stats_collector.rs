use std::time::Duration;

use demo::AppConfig;
use framework::console;
use framework::load_config;
use framework::log;
use framework::log::metrics::MetricsCollector;
use framework::spawn_action;
use framework::task;
use tokio::time;

#[tokio::main]
async fn main() {
    let config: AppConfig = load_config!("assets/conf.json");
    log::init(&config.log_appender, env!("CARGO_PKG_NAME"));

    let mut collector = MetricsCollector::new();
    collector.add(task::task_collector());
    collector.start_collect_task();

    for i in 0..10 {
        spawn_action!("sleep", async move {
            time::sleep(Duration::from_secs(20)).await;
            console!("{i}");
            Ok(())
        });
    }

    task::shutdown(Duration::from_secs(30)).await;
}
