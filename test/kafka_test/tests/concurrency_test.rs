use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use framework::exception::Exception;
use framework::system::CancellationToken;
use framework_kafka::consumer::ConsumerConfig;
use framework_kafka::consumer::Message;
use framework_macro::integration_test;
use kafka_test::AppState;
use kafka_test::consumer_with_config;
use kafka_test::producer;
use kafka_test::topic;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Semaphore;
use tokio::time;

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage {
    value: i32,
}

static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
static MAX_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

#[integration_test]
async fn concurrency() -> Result<(), Exception> {
    let topic_4 = topic::<TestMessage>("kafka_test_4");
    let topic_5 = topic::<TestMessage>("kafka_test_5");

    // distinct keys, so each message is its own key group and would run in parallel without the bound;
    // the bulk handler takes a permit as well, so it never overlaps with a key group
    let producer = producer();
    for i in 0..5 {
        producer.send(&topic_4, Some(i.to_string()), &TestMessage { value: i }).await.unwrap();
        producer.send(&topic_5, Some(i.to_string()), &TestMessage { value: i }).await.unwrap();
    }

    let semaphore = Arc::new(Semaphore::new(0));

    let shutdown_signal = CancellationToken::new();
    let mut consumer = consumer_with_config(
        concat!(env!("CARGO_PKG_NAME"), "_concurrency"),
        ConsumerConfig { max_concurrency: 1, ..Default::default() },
    );
    consumer.add_handler(&topic_4, test_message_handler);
    consumer.add_bulk_handler(&topic_5, test_bulk_message_handler);
    let consumer =
        tokio::spawn(consumer.start(AppState { semaphore: Arc::clone(&semaphore) }, shutdown_signal.clone()));

    let _permits = semaphore.acquire_many(10).await.unwrap();
    shutdown_signal.cancel();

    consumer.await.unwrap();

    assert_eq!(MAX_IN_FLIGHT.load(Ordering::Relaxed), 1);

    Ok(())
}

async fn test_message_handler(state: AppState, message: Message<TestMessage>) -> Result<(), Exception> {
    assert_eq!(message.key, Some(message.payload.value.to_string()));
    track_in_flight().await;
    state.semaphore.add_permits(1);
    Ok(())
}

async fn test_bulk_message_handler(state: AppState, messages: Vec<Message<TestMessage>>) -> Result<(), Exception> {
    track_in_flight().await;
    state.semaphore.add_permits(messages.len());
    Ok(())
}

async fn track_in_flight() {
    let in_flight = IN_FLIGHT.fetch_add(1, Ordering::Relaxed) + 1;
    MAX_IN_FLIGHT.fetch_max(in_flight, Ordering::Relaxed);
    time::sleep(Duration::from_millis(100)).await;
    IN_FLIGHT.fetch_sub(1, Ordering::Relaxed);
}
