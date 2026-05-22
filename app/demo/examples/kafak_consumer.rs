use std::sync::Arc;

use framework::exception::Exception;
use framework::log;
use framework::log::appender::ConsoleAppender;
use framework::shutdown::listen_shutdown_signal;
use framework_kafka::Topic;
use framework_kafka::consumer::ConsumerConfig;
use framework_kafka::consumer::Message;
use framework_kafka::consumer::MessageConsumer;
use framework_kafka::producer::Producer;
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
pub async fn main() -> Result<(), Exception> {
    log::init_with_action(ConsoleAppender);

    let (tx, rx) = mpsc::channel::<TestMessage>(1000);
    let state = Arc::new(State {
        topics: Topics { test_single: Topic::new("test_single"), test_bulk: Topic::new("test") },
        producer: Producer::new("dev.internal:9092".to_owned(), env!("CARGO_BIN_NAME")),
        tx,
    });

    let shutdown_signal = listen_shutdown_signal();

    let handle = tokio::spawn(process_message(rx));

    let mut consumer = MessageConsumer::new(
        "dev.internal:9092".to_owned(),
        env!("CARGO_BIN_NAME").to_owned(),
        &ConsumerConfig::default(),
    );

    consumer.add_handler(&state.topics.test_single, handler_single);
    consumer.add_bulk_handler(&state.topics.test_bulk, handler_bulk);
    consumer.start(state, shutdown_signal).await?;

    handle.await?;

    Ok(())
}

async fn handler_single(state: Arc<State>, message: Message<TestMessage>) -> Result<(), Exception> {
    if let Some(ref key) = message.key {
        if key == "1" {
            let value = message.payload()?;
            state.producer.send(&state.topics.test_single, Some("xxx".to_string()), &value).await?;
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

async fn handler_bulk(state: Arc<State>, messages: Vec<Message<TestMessage>>) -> Result<(), Exception> {
    for message in messages {
        if let Some(ref key) = message.key {
            if key == "1" {
                let value = message.payload()?;
                state.producer.send(&state.topics.test_single, Some("xxx".to_owned()), &value).await?;
                warn!("test");
            } else {
                println!("Received message: {}", message.payload()?.name);
            }
        }
    }
    Ok(())
}
