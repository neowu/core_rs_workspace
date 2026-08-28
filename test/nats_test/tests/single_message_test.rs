use std::sync::Arc;

use async_nats::jetstream::consumer::AckPolicy;
use framework::exception::Exception;
use framework::log;
use framework::system::CancellationToken;
use framework_macro::integration_test;
use framework_nats::Subject;
use framework_nats::consumer::Consumer;
use framework_nats::consumer::ConsumerConfig;
use framework_nats::consumer::Message;
use framework_nats::producer::Producer;
use nats_test::AppState;
use nats_test::STREAM;
use nats_test::client;
use nats_test::setup_consumer;
use nats_test::setup_jetstream;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Semaphore;

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage {
    value: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage2 {
    value: i32,
}

#[integration_test]
async fn single_message() -> Result<(), Exception> {
    let client = client().await;

    let durable = env!("CARGO_PKG_NAME");
    setup_jetstream(client.clone()).await;
    setup_consumer(client.clone(), durable, AckPolicy::Explicit).await;

    let subject_1: Subject<TestMessage> = Subject::new("nats_test.1");
    let subject_2: Subject<TestMessage2> = Subject::new("nats_test.2");
    let semaphore = Arc::new(Semaphore::new(0));

    // the test drives shutdown itself, System only cancels on a signal
    let shutdown_signal = CancellationToken::new();
    let mut consumer = Consumer::new(client.clone(), STREAM, durable, ConsumerConfig::default());
    consumer.add_handler(&subject_1, test_message_handler);
    consumer.add_handler(&subject_2, test_message_handler_2);
    let consumer =
        tokio::spawn(consumer.start(AppState { semaphore: Arc::clone(&semaphore) }, shutdown_signal.clone()));

    let producer = Producer::new(client);
    producer.send(&subject_1, &TestMessage { value: "v1".to_owned() }).await.unwrap();
    producer.send(&subject_2, &TestMessage2 { value: 3 }).await.unwrap();

    let _permits = semaphore.acquire_many(2).await.unwrap();
    shutdown_signal.cancel();

    consumer.await.unwrap();

    Ok(())
}

async fn test_message_handler(state: AppState, message: Message<TestMessage>) -> Result<(), Exception> {
    log::trace();
    assert_eq!(message.subject, "nats_test.1");
    assert_eq!(message.payload.value, "v1");
    state.semaphore.add_permits(1);
    Ok(())
}

async fn test_message_handler_2(state: AppState, message: Message<TestMessage2>) -> Result<(), Exception> {
    log::trace();
    assert_eq!(message.subject, "nats_test.2");
    assert_eq!(message.payload.value, 3);
    state.semaphore.add_permits(1);
    Ok(())
}
