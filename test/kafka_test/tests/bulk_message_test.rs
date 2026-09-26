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
    value: i32,
}

#[integration_test]
async fn bulk_message() -> Result<(), Exception> {
    let topic_3 = topic::<TestMessage>("kafka_test_3");

    // topics are auto created on first send, consumer reads them from earliest
    let producer = producer();
    for i in 0..10 {
        producer.send(&topic_3, Some(i.to_string()), &TestMessage { value: i }).await.unwrap();
    }

    let semaphore = Arc::new(Semaphore::new(0));

    // the test drives shutdown itself, System only cancels on a signal
    let shutdown_signal = CancellationToken::new();
    let mut consumer = consumer(concat!(env!("CARGO_PKG_NAME"), "_bulk"));
    consumer.add_bulk_handler(&topic_3, test_bulk_message_handler);
    let consumer =
        tokio::spawn(consumer.start(AppState { semaphore: Arc::clone(&semaphore) }, shutdown_signal.clone()));

    let _permits = semaphore.acquire_many(10).await.unwrap();
    shutdown_signal.cancel();

    consumer.await.unwrap();

    Ok(())
}

async fn test_bulk_message_handler(state: AppState, messages: Vec<Message<TestMessage>>) -> Result<(), Exception> {
    for message in messages {
        assert_eq!(message.key, Some(message.payload.value.to_string()));
        state.semaphore.add_permits(1);
    }
    Ok(())
}
