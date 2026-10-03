use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::AckPolicy;
use framework::exception::Exception;
use framework::system::CancellationToken;
use framework_macro::integration_test;
use framework_nats::Subject;
use framework_nats::consumer::Consumer;
use framework_nats::consumer::ConsumerConfig;
use framework_nats::consumer::Message;
use framework_nats::producer::Producer;
use nats_test::STREAM;
use nats_test::client;
use nats_test::setup_consumer;
use nats_test::setup_jetstream;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tokio::time::timeout;

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage {
    value: i32,
}

#[derive(Clone)]
struct State {
    release: CancellationToken,
    handled: Arc<Semaphore>,
}

// handlers hold every permit past expires + 5s, when async-nats ends a batch: no message may be
// pulled ahead of a permit, or it is stranded unacked until ack_wait (30 min)
#[integration_test]
async fn consumer_saturated() -> Result<(), Exception> {
    let client = client().await;
    let durable = concat!(env!("CARGO_PKG_NAME"), "_saturated");
    setup_jetstream(client.clone()).await;
    setup_consumer(client.clone(), durable, AckPolicy::Explicit).await;

    let subject: Subject<TestMessage> = Subject::new("nats_test.saturated");
    let state = State { release: CancellationToken::new(), handled: Arc::new(Semaphore::new(0)) };

    let shutdown_signal = CancellationToken::new();
    let config =
        ConsumerConfig { max_concurrency: 2, batch_max_messages: 1000, batch_max_wait: Duration::from_millis(100) };
    let mut consumer = Consumer::new(client.clone(), STREAM, durable, config);
    consumer.add_handler(&subject, handler);
    let consumer = tokio::spawn(consumer.start(state.clone(), shutdown_signal.clone()));

    let producer = Producer::new(client);
    for i in 0..10 {
        producer.send(&subject, &TestMessage { value: i }).await?;
    }

    sleep(Duration::from_secs(6)).await;
    state.release.cancel();
    let handled = timeout(Duration::from_secs(5), state.handled.acquire_many(10)).await;
    assert!(handled.is_ok(), "every message must be handled once handlers free up");

    shutdown_signal.cancel();
    consumer.await.unwrap();

    Ok(())
}

async fn handler(state: State, _message: Message<TestMessage>) -> Result<(), Exception> {
    state.release.cancelled().await;
    state.handled.add_permits(1);
    Ok(())
}
