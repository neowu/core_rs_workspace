use std::sync::Arc;

use async_nats::jetstream::consumer::AckPolicy;
use framework::exception::Exception;
use framework::log;
use framework::system::System;
use framework_macro::integration_test;
use framework_nats::Subject;
use framework_nats::consumer::BatchConsumer;
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
    value: i32,
}

#[integration_test]
async fn batch_message() -> Result<(), Exception> {
    let client = client().await;
    let durable = concat!(env!("CARGO_PKG_NAME"), "_batch");
    setup_jetstream(client.clone()).await;
    setup_consumer(client.clone(), durable, AckPolicy::All).await;

    let subject_3: Subject<TestMessage> = Subject::new("nats_test.3");
    let semaphore = Arc::new(Semaphore::new(0));

    let mut system = System::new();
    let batch_consumer = BatchConsumer::new(
        client.clone(),
        STREAM,
        durable,
        &subject_3,
        test_batch_message_handler,
        ConsumerConfig::default(),
    );
    system.spawn(batch_consumer.start(AppState { semaphore: Arc::clone(&semaphore) }, system.shutdown_signal()));

    let producer = Producer::new(client);
    for i in 0..10 {
        producer.send(&subject_3, &TestMessage { value: i }).await.unwrap();
    }

    let _permits = semaphore.acquire_many(10).await.unwrap();
    system.shutdown_signal().cancel();

    system.wait().await;

    Ok(())
}

async fn test_batch_message_handler(state: AppState, messages: Vec<Message<TestMessage>>) -> Result<(), Exception> {
    log::trace();
    for message in messages {
        assert_eq!(message.subject, "nats_test.3");
        state.semaphore.add_permits(1);
    }
    Ok(())
}
