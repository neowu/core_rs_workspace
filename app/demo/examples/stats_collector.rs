use std::time::Duration;

use framework::appender::ConsoleAppender;
use framework::console;
use framework::spawn_action;
use framework::system::System;
use framework::task::start_executor;
use tokio::time;

#[tokio::main]
async fn main() {
    let mut system = System::init(env!("CARGO_PKG_NAME"));

    let executor = start_executor();
    system.add_metrics(executor.metrics());

    let system = system.start_logger(ConsoleAppender);

    for i in 0..10 {
        spawn_action!("sleep", async move {
            time::sleep(Duration::from_secs(20)).await;
            console!("{i}");
            Ok(())
        });
    }

    system.wait().await;
    executor.shutdown(Duration::from_secs(15)).await;
    system.shutdown_logger().await;
}
