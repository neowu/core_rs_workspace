use std::sync::Arc;

use framework::exception::CoreRsResult;
use framework::kafka::consumer::ConsumerConfig;
use framework::kafka::consumer::Message;
use framework::kafka::consumer::MessageConsumer;
use framework::kafka::producer::Producer;
use framework::kafka::topic::Topic;
use framework::log;
use framework::log::ConsoleAppender;
use framework::shutdown::Shutdown;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tracing::warn;

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage {
    name: String,
}

struct State {
    topics: Topics,
    producer: Producer,
    tx: mpsc::Sender<TestMessage>,
}

struct Topics {
    test_single: Topic<TestMessage>,
    test_bulk: Topic<TestMessage>,
}

#[tokio::main]
pub async fn main() -> CoreRsResult<()> {
    log::init_with_action(ConsoleAppender);

    let (tx, rx) = mpsc::channel::<TestMessage>(1000);
    let state = Arc::new(State {
        topics: Topics {
            test_single: Topic::new("test_single"),
            test_bulk: Topic::new("test"),
        },
        producer: Producer::new("dev.internal:9092", env!("CARGO_BIN_NAME")),
        tx,
    });

    let shutdown = Shutdown::new();
    let signal = shutdown.subscribe();
    shutdown.listen();

    let handle = tokio::spawn(process_message(rx));

    let mut consumer = MessageConsumer::new("dev.internal:9092", env!("CARGO_BIN_NAME"), ConsumerConfig::default());

    consumer.add_handler(&state.topics.test_single, handler_single);
    consumer.add_bulk_handler(&state.topics.test_bulk, handler_bulk);
    consumer.start(state, signal).await?;

    handle.await?;

    Ok(())
}

async fn handler_single(state: Arc<State>, message: Message<TestMessage>) -> CoreRsResult<()> {
    if let Some(ref key) = message.key {
        if key == "1" {
            let value = message.payload()?;
            state
                .producer
                .send(&state.topics.test_single, Some("xxx".to_string()), &value)
                .await?;
        } else {
            state.tx.send(message.payload()?).await?;
        }
    }
    Ok(())
}

async fn process_message(mut rx: Receiver<TestMessage>) {
    let mut buffer = Vec::with_capacity(1000);

    while rx.recv_many(&mut buffer, 1000).await != 0 {
        for message in buffer.drain(..) {
            println!("Received message: {}", message.name);
        }
    }

    println!("finished");
}

async fn handler_bulk(state: Arc<State>, messages: Vec<Message<TestMessage>>) -> CoreRsResult<()> {
    for message in messages {
        if let Some(ref key) = message.key {
            if key == "1" {
                let value = message.payload()?;
                state
                    .producer
                    .send(&state.topics.test_single, Some("xxx".to_string()), &value)
                    .await?;
                warn!("test");
            } else {
                println!("Received message: {}", message.payload()?.name);
            }
        }
    }
    Ok(())
}
