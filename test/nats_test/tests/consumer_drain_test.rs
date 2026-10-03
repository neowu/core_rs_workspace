use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::consumer::PullConsumer;
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
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tokio::time::timeout;

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage {
    value: i32,
}

#[derive(Clone)]
struct State {
    started: Arc<Notify>,
    release: CancellationToken,
    handled: Arc<Semaphore>,
}

// shutdown with messages pulled and queued behind a busy handler: the in-flight one finishes, the queued
// ones are nak'd and the next release handles them right away, none left for redelivery after ack_wait
#[integration_test]
async fn consumer_drain() -> Result<(), Exception> {
    let client = client().await;
    let durable = concat!(env!("CARGO_PKG_NAME"), "_drain");
    setup_jetstream(client.clone()).await;
    setup_consumer(client.clone(), durable, AckPolicy::Explicit).await;

    let subject: Subject<TestMessage> = Subject::new("nats_test.drain");
    let state = State {
        started: Arc::new(Notify::new()),
        release: CancellationToken::new(),
        handled: Arc::new(Semaphore::new(0)),
    };
    let config = ConsumerConfig { max_concurrency: 1, ..ConsumerConfig::default() };
    let start = |shutdown_signal: CancellationToken| {
        let mut consumer = Consumer::new(client.clone(), STREAM, durable, config);
        consumer.add_handler(&subject, handler);
        tokio::spawn(consumer.start(state.clone(), shutdown_signal))
    };

    let shutdown_signal = CancellationToken::new();
    let consumer = start(shutdown_signal.clone());
    let producer = Producer::new(client.clone());
    for i in 0..10 {
        producer.send(&subject, &TestMessage { value: i }).await?;
    }
    state.started.notified().await;
    sleep(Duration::from_millis(200)).await; // the rest are pulled and queued behind the busy handler

    shutdown_signal.cancel();
    sleep(Duration::from_millis(200)).await;
    state.release.cancel();
    timeout(Duration::from_secs(5), consumer).await.expect("consumer must stop once drained").unwrap();

    let shutdown_signal = CancellationToken::new();
    let next_release = start(shutdown_signal.clone());
    let handled = timeout(Duration::from_secs(5), state.handled.acquire_many(10)).await;
    shutdown_signal.cancel();
    next_release.await.unwrap();
    assert!(handled.is_ok(), "queued messages must be redelivered to the next release right away");

    client.flush().await.unwrap();
    let mut info_consumer: PullConsumer =
        jetstream::new(client).get_stream(STREAM).await.unwrap().get_consumer(durable).await.unwrap();
    let info = info_consumer.info().await.unwrap();
    assert_eq!(info.num_ack_pending, 0, "no message may be left unacked");

    Ok(())
}

async fn handler(state: State, _message: Message<TestMessage>) -> Result<(), Exception> {
    state.started.notify_one();
    state.release.cancelled().await;
    state.handled.add_permits(1);
    Ok(())
}
