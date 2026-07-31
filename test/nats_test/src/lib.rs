use std::sync::Arc;

use async_nats::jetstream;
use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::stream::Config;
use framework::console;
use framework_nats::producer::Producer;
use tokio::sync::Semaphore;

// producer must live while sending message in background
pub async fn producer() -> Arc<Producer> {
    Arc::new(Producer::new(URL.to_owned(), env!("CARGO_PKG_NAME")).await)
}

#[derive(Clone)]
pub struct AppState {
    pub semaphore: Arc<Semaphore>,
}

pub const URL: &str = "dev.internal:4222";
pub const STREAM: &str = "nats_test_stream";

pub async fn setup_jetstream() {
    console!("create jetstream, name={STREAM}");
    jetstream::new(async_nats::connect(URL).await.unwrap())
        .create_or_update_stream(Config {
            name: STREAM.to_owned(),
            subjects: vec!["nats_test.>".to_owned()],
            ..Default::default()
        })
        .await
        .unwrap();
}

// create consumer before start, to make sure receive all new message, since deliver_policy = New,
pub async fn setup_consumer(durable: &str, ack_policy: AckPolicy) {
    jetstream::new(async_nats::connect(URL).await.unwrap())
        .create_consumer_on_stream(
            pull::Config {
                durable_name: Some(durable.to_owned()),
                deliver_policy: DeliverPolicy::New,
                ack_policy,
                ..Default::default()
            },
            STREAM,
        )
        .await
        .unwrap();
}
