use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::consumer::AckPolicy;
use framework::exception::Exception;
use framework::json::to_json;
use framework::system::CancellationToken;
use framework_macro::integration_test;
use framework_nats::Subject;
use framework_nats::consumer::BatchConsumer;
use framework_nats::consumer::BatchConsumerConfig;
use framework_nats::consumer::Message;
use nats_test::STREAM;
use nats_test::client;
use nats_test::setup_consumer;
use nats_test::setup_jetstream;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage {
    value: i32,
}

// with a backlog, a batch fills up to batch_max_messages; the server default max_ack_pending (1000)
// would cut it at 1000 and leave it waiting for batch_max_wait
#[integration_test]
async fn batch_consumer_backlog() -> Result<(), Exception> {
    let client = client().await;
    let durable = concat!(env!("CARGO_PKG_NAME"), "_backlog");
    setup_jetstream(client.clone()).await;
    setup_consumer(client.clone(), durable, AckPolicy::All).await;

    // backlog stored before the consumer pulls
    let subject: Subject<TestMessage> = Subject::new("nats_test.backlog");
    let context = jetstream::new(client.clone());
    for value in 0..2500 {
        let payload = to_json(&TestMessage { value })?;
        context.publish(subject.name, payload.into()).await.unwrap().await.unwrap();
    }

    let (sender, mut batch_sizes) = mpsc::unbounded_channel();
    let shutdown_signal = CancellationToken::new();
    let config = BatchConsumerConfig { batch_max_messages: 2000, batch_max_wait: Duration::from_secs(1) };
    let consumer = BatchConsumer::new(client, STREAM, durable, &subject, handler, config);
    let consumer = tokio::spawn(consumer.start(sender, shutdown_signal.clone()));

    assert_eq!(batch_sizes.recv().await, Some(2000));
    assert_eq!(batch_sizes.recv().await, Some(500));

    shutdown_signal.cancel();
    consumer.await.unwrap();

    Ok(())
}

async fn handler(sender: UnboundedSender<usize>, messages: Vec<Message<TestMessage>>) -> Result<(), Exception> {
    sender.send(messages.len()).unwrap();
    Ok(())
}
