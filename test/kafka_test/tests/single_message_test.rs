use std::sync::Arc;

use framework::exception::Exception;
use framework::system::CancellationToken;
use framework_kafka::consumer::Message;
use framework_macro::integration_test;
use kafka_test::AppState;
use kafka_test::consumer;
use kafka_test::producer;
use kafka_test::topic;
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
    let topic_1 = topic::<TestMessage>("kafka_test_1");
    let topic_2 = topic::<TestMessage2>("kafka_test_2");

    // topics are auto created on first send, consumer reads them from earliest
    let producer = producer();
    producer.send(&topic_1, Some("k1".to_owned()), &TestMessage { value: "v1".to_owned() }).await.unwrap();
    producer.send(&topic_2, None, &TestMessage2 { value: 3 }).await.unwrap();

    let semaphore = Arc::new(Semaphore::new(0));

    // the test drives shutdown itself, System only cancels on a signal
    let shutdown_signal = CancellationToken::new();
    let mut consumer = consumer(env!("CARGO_PKG_NAME"));
    consumer.add_handler(&topic_1, test_message_handler);
    consumer.add_handler(&topic_2, test_message_handler_2);
    let consumer =
        tokio::spawn(consumer.start(AppState { semaphore: Arc::clone(&semaphore) }, shutdown_signal.clone()));

    let _permits = semaphore.acquire_many(2).await.unwrap();
    shutdown_signal.cancel();

    consumer.await.unwrap();

    Ok(())
}

async fn test_message_handler(state: AppState, message: Message<TestMessage>) -> Result<(), Exception> {
    assert_eq!(message.key.as_deref(), Some("k1"));
    assert_eq!(message.payload.value, "v1");
    state.semaphore.add_permits(1);
    Ok(())
}

async fn test_message_handler_2(state: AppState, message: Message<TestMessage2>) -> Result<(), Exception> {
    assert_eq!(message.key, None);
    assert_eq!(message.payload.value, 3);
    state.semaphore.add_permits(1);
    Ok(())
}
