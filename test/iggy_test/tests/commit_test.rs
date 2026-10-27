use std::sync::Arc;
use std::sync::Mutex;

use framework::exception::Exception;
use framework::system::CancellationToken;
use framework_iggy::consumer::ConsumerConfig;
use framework_iggy::consumer::Message;
use framework_macro::integration_test;
use iggy_test::AppState;
use iggy_test::consumer_with_config;
use iggy_test::producer;
use iggy_test::stream;
use iggy_test::topic;
use iggy_test::unique;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Semaphore;

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage {
    value: i32,
}

// values the second run handled; a handler panic is only logged, so assertions run after the consumer stops
static SECOND_RUN: Mutex<Vec<i32>> = Mutex::new(Vec::new());

#[integration_test]
async fn commit() -> Result<(), Exception> {
    let stream = stream();
    let topic_6 = topic::<TestMessage>("topic_6");
    // both runs share the group, so the second resumes from the offsets the first committed
    let group = unique("iggy_test_commit");

    // distinct keys spread the messages over the topic's partitions
    let producer = producer(stream).await;
    for i in 0..10 {
        producer.send(&topic_6, Some(i.to_string()), &TestMessage { value: i }).await.unwrap();
    }

    let semaphore = Arc::new(Semaphore::new(0));
    let shutdown_signal = CancellationToken::new();
    let mut consumer = consumer_with_config(stream, group, ConsumerConfig::default());
    consumer.add_handler(&topic_6, test_message_handler);
    let consumer =
        tokio::spawn(consumer.start(AppState { semaphore: Arc::clone(&semaphore) }, shutdown_signal.clone()));
    let _permits = semaphore.acquire_many(10).await.unwrap();
    shutdown_signal.cancel();
    consumer.await.unwrap();

    for i in 10..13 {
        producer.send(&topic_6, Some(i.to_string()), &TestMessage { value: i }).await.unwrap();
    }

    let shutdown_signal = CancellationToken::new();
    let mut consumer = consumer_with_config(stream, group, ConsumerConfig::default());
    consumer.add_handler(&topic_6, test_new_message_handler);
    let consumer =
        tokio::spawn(consumer.start(AppState { semaphore: Arc::clone(&semaphore) }, shutdown_signal.clone()));
    let _permits = semaphore.acquire_many(3).await.unwrap();
    shutdown_signal.cancel();
    consumer.await.unwrap();

    let mut values = SECOND_RUN.lock().unwrap().clone();
    values.sort_unstable();
    assert_eq!(values, vec![10, 11, 12], "committed messages must not be redelivered");

    Ok(())
}

async fn test_message_handler(state: AppState, _message: Message<TestMessage>) -> Result<(), Exception> {
    state.semaphore.add_permits(1);
    Ok(())
}

async fn test_new_message_handler(state: AppState, message: Message<TestMessage>) -> Result<(), Exception> {
    SECOND_RUN.lock().unwrap().push(message.payload.value);
    state.semaphore.add_permits(1);
    Ok(())
}
