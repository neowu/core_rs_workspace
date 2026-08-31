use std::time::Duration;

use framework::appender::ConsoleAppender;
use framework::spawn_action;
use framework::system::System;
use framework::task::start_executor;
use framework_kafka::Topic;
use framework_kafka::producer::Producer;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage {
    name: String,
}

#[tokio::main]
pub async fn main() {
    let producer = Producer::new("dev.internal:9092".to_owned());

    let system = System::init(env!("CARGO_BIN_NAME")).start_logger(ConsoleAppender);
    let executor = start_executor();

    spawn_action!("produce", async move {
        let topic = Topic::new("test");

        for i in 1..10 {
            producer.send(&topic, Some(i.to_string()), &TestMessage { name: format!("{i}") }).await?;
        }

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        for i in 10..20 {
            producer.send(&topic, Some(i.to_string()), &TestMessage { name: format!("{i}") }).await?;
        }
        Ok(())
    });

    system.wait().await;
    executor.shutdown(Duration::from_secs(15)).await;
    system.shutdown_logger().await;
}
