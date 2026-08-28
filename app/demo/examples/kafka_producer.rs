use std::time::Duration;

use framework::exception::Exception;
use framework::log::ConsoleAppender;
use framework::spawn_action;
use framework::system::System;
use framework_kafka::Topic;
use framework_kafka::producer::Producer;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage {
    name: String,
}

#[tokio::main]
pub async fn main() -> Result<(), Exception> {
    let producer = Producer::new("dev.internal:9092".to_owned());

    let mut system = System::init(env!("CARGO_BIN_NAME"));
    system.start_action_logger(ConsoleAppender);

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
    // the spawned action sleeps 3s between batches, give it room to finish
    system.shutdown(Duration::from_mins(1)).await
}
