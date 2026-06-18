use std::sync::Arc;

use demo::AppConfig;
use framework::exception::Exception;
use framework::load_config;
use framework::log;
use framework::log::metrics::MetricsCollector;
use framework::system::System;
use framework_nats::Subject;
use framework_nats::consumer::ConsumerConfig;
use framework_nats::consumer::Message;
use framework_nats::consumer::MessageConsumer;
use framework_nats::producer::Producer;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage {
    name: String,
}

struct State {
    subjects: Subjects,
    producer: Producer,
    tx: mpsc::Sender<TestMessage>,
}

struct Subjects {
    test_single: Subject<TestMessage>,
    test_bulk: Subject<TestMessage>,
}

#[tokio::main]
pub async fn main() -> Result<(), Exception> {
    let config: AppConfig = load_config!("assets/conf.json");
    log::init(&config.log_appender, env!("CARGO_PKG_NAME"));

    let (tx, rx) = mpsc::channel::<TestMessage>(1000);
    let state = Arc::new(State {
        subjects: Subjects { test_single: Subject::new("test.single"), test_bulk: Subject::new("test.bulk") },
        producer: Producer::new("dev.internal:4222".to_owned(), env!("CARGO_BIN_NAME")).await,
        tx,
    });

    let mut system = System::new();
    let mut collector = MetricsCollector::new();

    let handle = tokio::spawn(process_message(rx));

    let mut consumer =
        MessageConsumer::new("dev.internal:4222".to_owned(), "JET", env!("CARGO_BIN_NAME"), &ConsumerConfig::default());

    consumer.add_handler(&state.subjects.test_single, handler_single);
    consumer.add_bulk_handler(&state.subjects.test_bulk, handler_bulk);
    collector.add(consumer.consumer_metrics());

    system.spawn(consumer.start(state, system.shutdown_signal()));
    system.spawn(collector.start(system.shutdown_signal()));

    handle.await?;

    system.wait().await;
    Ok(())
}

async fn handler_single(state: Arc<State>, message: Message<TestMessage>) -> Result<(), Exception> {
    if message.payload()?.name == "1" {
        let value = message.payload()?;
        state.producer.send(&state.subjects.test_single, &value).await?;
    } else {
        state.tx.send(message.payload()?).await?;
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

async fn handler_bulk(_state: Arc<State>, messages: Vec<Message<TestMessage>>) -> Result<(), Exception> {
    for message in messages {
        println!("Received bulk message: {}", message.payload()?.name);
    }
    Ok(())
}
