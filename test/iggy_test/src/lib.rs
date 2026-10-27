use std::sync::Arc;

use framework::time::DateTime;
use framework_iggy::Topic;
use framework_iggy::consumer::ConsumerConfig;
use framework_iggy::consumer::MessageConsumer;
use framework_iggy::producer::Producer;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct AppState {
    pub semaphore: Arc<Semaphore>,
}

const URL: &str = "iggy://iggy:iggy@iggy.test:8090";

// streams and groups are unique per run and auto created with their topics, so each test starts clean without
// leftover messages or stale group members from previous runs
pub fn unique(name: &str) -> &'static str {
    format!("{name}_{}", DateTime::now().unix_timestamp_millis()).leak()
}

pub fn stream() -> &'static str {
    unique("iggy_test")
}

pub async fn producer(stream: &'static str) -> Producer {
    Producer::new(URL, stream).await
}

pub fn consumer(stream: &'static str, group: &str) -> MessageConsumer<AppState> {
    consumer_with_config(stream, unique(group), ConsumerConfig::default())
}

// a new group has no stored offset, so PollingStrategy::next() reads from the first message
pub fn consumer_with_config(
    stream: &'static str,
    group: &'static str,
    config: ConsumerConfig,
) -> MessageConsumer<AppState> {
    MessageConsumer::new(URL.to_owned(), stream, group, &config)
}

pub const fn topic<T>(name: &'static str) -> Topic<T> {
    Topic::new(name)
}
