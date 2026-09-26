use std::sync::Arc;

use framework::time::DateTime;
use framework_kafka::Topic;
use framework_kafka::consumer::ConsumerConfig;
use framework_kafka::consumer::MessageConsumer;
use framework_kafka::producer::Producer;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct AppState {
    pub semaphore: Arc<Semaphore>,
}

const BROKER: &str = "kafka.test:9092";

// topics and groups are unique per run and auto created, so each test starts clean without leftover messages
// or stale group members from previous runs
fn unique(name: &str) -> &'static str {
    format!("{name}_{}", DateTime::now().unix_timestamp_millis()).leak()
}

pub fn topic<T>(name: &str) -> Topic<T> {
    Topic::new(unique(name))
}

pub fn producer() -> Producer {
    Producer::new(BROKER.to_owned())
}

// earliest, messages are sent before the new group subscribes
pub fn consumer(group_id: &str) -> MessageConsumer<AppState> {
    let config = ConsumerConfig { auto_offset_reset: "earliest", ..Default::default() };
    MessageConsumer::new(BROKER.to_owned(), unique(group_id), &config)
}
