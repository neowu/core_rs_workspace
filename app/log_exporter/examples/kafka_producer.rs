use anyhow::Result;
use core_ng::kafka::producer::Producer;
use core_ng::kafka::producer::ProducerConfig;
use core_ng::kafka::topic::Topic;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage {
    name: String,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let producer = Producer::new(ProducerConfig {
        bootstrap_servers: "dev.internal:9092",
    });

    let topic = Topic::new("test");

    for i in 1..10 {
        producer
            .send(&topic, Some(i.to_string()), &TestMessage { name: format!("{i}") })
            .await?;
    }

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    for i in 10..20 {
        producer
            .send(&topic, Some(i.to_string()), &TestMessage { name: format!("{i}") })
            .await?;
    }

    Ok(())
}
