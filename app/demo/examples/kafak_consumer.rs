use std::sync::Arc;

use demo::AppConfig;
use framework::exception::Exception;
use framework::load_config;
use framework::log;
use framework::log::metrics::MetricsCollector;
use framework::system::System;
use framework::warn;
use framework_kafka::Topic;
use framework_kafka::consumer::ConsumerConfig;
use framework_kafka::consumer::Message;
use framework_kafka::consumer::MessageConsumer;
use framework_kafka::producer::Producer;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

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
    let config: AppConfig = load_config!("assets/conf.json");
    log::init(&config.log_appender, env!("CARGO_PKG_NAME"));

    let (tx, rx) = mpsc::channel::<TestMessage>(1000);
    let state = Arc::new(State {
        topics: Topics { test_single: Topic::new("test_single"), test_bulk: Topic::new("test") },
        producer: Producer::new("dev.internal:9092".to_owned(), env!("CARGO_BIN_NAME")),
        tx,
    });

    let mut system = System::new();
    let mut collector = MetricsCollector::new();

    let handle = tokio::spawn(process_message(rx));

    let mut consumer =
        MessageConsumer::new("dev.internal:9092".to_owned(), env!("CARGO_BIN_NAME"), &ConsumerConfig::default());

    consumer.add_handler(&state.topics.test_single, handler_single);
    consumer.add_bulk_handler(&state.topics.test_bulk, handler_bulk);
    collector.add(consumer.consumer_metrics());

    system.spawn(consumer.start(state, system.shutdown_signal()));
    system.spawn(collector.start(system.shutdown_signal()));

    handle.await?;

    system.wait().await;
    Ok(())
}

async fn handler_single(state: Arc<State>, message: Message<TestMessage>) -> Result<(), Exception> {
    if let Some(ref key) = message.key {
        if key == "1" {
            state.producer.send(&state.topics.test_single, Some("xxx".to_string()), &message.payload).await?;
        } else {
            state.tx.send(message.payload).await?;
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
                state.producer.send(&state.topics.test_single, Some("xxx".to_owned()), &message.payload).await?;
                warn!(error_code = "TRIGGER", "test");
            } else {
                println!("Received message: {}", message.payload.name);
            }
        }
    }
    Ok(())
}
