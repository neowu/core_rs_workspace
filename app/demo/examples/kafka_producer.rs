use framework::exception::Exception;
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
    let producer = Producer::new("dev.internal:9092".to_owned(), env!("CARGO_BIN_NAME"));

    let topic = Topic::new("test_single");

    for i in 1..10 {
        producer.send(&topic, Some(i.to_string()), &TestMessage { name: format!("{i}") }).await?;
    }

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    for i in 10..20 {
        producer.send(&topic, Some(i.to_string()), &TestMessage { name: format!("{i}") }).await?;
    }

    Ok(())
}
