use std::sync::Arc;

use demo::AppConfig;
use framework::exception::Exception;
use framework::load_config;
use framework::log;
use framework::log::metrics::MetricsCollector;
use framework::system::System;
use framework::warn;
use framework_nats::Subject;
use framework_nats::consumer::BatchConsumer;
use framework_nats::consumer::ConsumerConfig;
use framework_nats::consumer::Message;
use framework_nats::consumer::MessageConsumer;
use framework_nats::consumer::consumer_metrics;
use framework_nats::producer::Producer;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage {
    name: String,
}

struct State {
    subjects: Subjects,
    producer: Producer,
}

struct Subjects {
    test_single: Subject<TestMessage>,
    test_bulk: Subject<TestMessage>,
}

#[tokio::main]
pub async fn main() -> Result<(), Exception> {
    let config: AppConfig = load_config!("assets/conf.json");
    log::init(&config.log_appender, env!("CARGO_PKG_NAME"));

    let state = Arc::new(State {
        subjects: Subjects {
            test_single: Subject::new("queue.test.single"),
            test_bulk: Subject::new("queue.test.bulk"),
        },
        producer: Producer::new("dev.internal:4222".to_owned(), env!("CARGO_BIN_NAME")).await,
    });

    let mut system = System::new();
    let mut collector = MetricsCollector::new();

    let mut consumer = MessageConsumer::new(
        "dev.internal:4222".to_owned(),
        "queue",
        env!("CARGO_BIN_NAME"),
        ConsumerConfig::default(),
    );
    consumer.add_handler(&state.subjects.test_single, handler_single);
    collector.add(consumer_metrics());

    let batch_consumer = BatchConsumer::new(
        "dev.internal:4222".to_owned(),
        "queue",
        concat!(env!("CARGO_BIN_NAME"), "-bulk"),
        &state.subjects.test_bulk,
        handler_bulk,
        ConsumerConfig::default(),
    );

    system.spawn(consumer.start(Arc::clone(&state), system.shutdown_signal()));
    system.spawn(batch_consumer.start(state, system.shutdown_signal()));
    system.spawn(collector.start(system.shutdown_signal()));

    system.wait().await;
    Ok(())
}

async fn handler_single(state: Arc<State>, message: Message<TestMessage>) -> Result<(), Exception> {
    if message.payload.name == "1" {
        state.producer.send(&state.subjects.test_single, &TestMessage { name: "resend".to_owned() }).await?;
        warn!(error_code = "TRIGGER", "test");
    } else {
        println!("Received message: {}", message.payload.name);
    }
    Ok(())
}

async fn handler_bulk(_state: Arc<State>, messages: Vec<Message<TestMessage>>) -> Result<(), Exception> {
    for message in messages {
        println!("Received bulk message: {}", message.payload.name);
    }
    Ok(())
}
