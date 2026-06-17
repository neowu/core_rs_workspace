use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use chrono::FixedOffset;
use chrono::NaiveTime;
use framework::console;
use framework::exception::Exception;
use framework::load_config;
use framework::log;
use framework::log::metrics::MetricsCollector;
use framework::log::metrics::mem_max;
use framework::network::hostname;
use framework::schedule::Scheduler;
use framework::system::System;
use framework::task;
use framework::web::server::HttpServerConfig;
use framework::web::server::http_server_metrics;
use framework::web::server::start_http_server;
use framework_kafka::Topic;
use framework_kafka::consumer::ConsumerConfig;
use framework_kafka::consumer::MessageConsumer;
use job::process_log_job;
use kafka::action_log_handler::ActionLogMessage;
use kafka::action_log_handler::action_log_message_handler;
use kafka::event_handler::EventMessage;
use kafka::event_handler::event_message_handler;
use serde::Deserialize;
use sha2::Digest as _;
use sha2::Sha256;

mod job;
mod kafka;
mod service;
mod web;

#[derive(Debug, Deserialize)]
struct AppConfig {
    log_appender: String,
    kafka_uri: String,
    log_dir: String,
    bucket: String,
}

pub struct AppState {
    topics: Topics,

    log_dir: String,
    hash: String,
    bucket: String,

    duckdb_memory_limit: u64, // in bytes
}

fn hash(hostname: &str) -> String {
    let hash = Sha256::digest(hostname);
    hex::encode(hash)[0..6].to_owned()
}

fn duckdb_memory_limit() -> u64 {
    if let Some(mem_max) = mem_max() {
        console!("detected max memory, value={mem_max}, set duckdb_memory_limit to 50%");
        mem_max / 2
    } else {
        console!("not in cgroup v2 env, set duckdb_memory_limit to 200MB");
        200_000_000
    }
}

struct Topics {
    action: Topic<ActionLogMessage>,
    event: Topic<EventMessage>,
}

#[tokio::main]
async fn main() -> Result<(), Exception> {
    let config: AppConfig = load_config!("assets/conf.json");
    log::init(&config.log_appender, env!("CARGO_PKG_NAME"));

    let mut system = System::new();
    let mut collector = MetricsCollector::new();

    let state = Arc::new({
        let hash = hash(hostname());

        AppState {
            topics: Topics { action: Topic::new("action-log-v2"), event: Topic::new("event") },
            log_dir: config.log_dir,
            hash,
            bucket: config.bucket,
            duckdb_memory_limit: duckdb_memory_limit(),
        }
    });

    let scheduler_state = Arc::clone(&state);
    let mut scheduler = Scheduler::new(FixedOffset::east_opt(8 * 60 * 60).expect("value must be valid"));
    scheduler.schedule_daily(
        "process_log_job",
        process_log_job,
        NaiveTime::from_hms_opt(1, 0, 0).expect("value must be valid"),
    );
    system.spawn(scheduler.start(scheduler_state, system.shutdown_signal()));

    let consumer_state = Arc::clone(&state);
    let mut consumer = MessageConsumer::new(config.kafka_uri, env!("CARGO_BIN_NAME"), &ConsumerConfig::default());
    consumer.add_bulk_handler(&consumer_state.topics.action, action_log_message_handler);
    consumer.add_bulk_handler(&consumer_state.topics.event, event_message_handler);
    collector.add(consumer.consumer_metrics());
    system.spawn(consumer.start(consumer_state, system.shutdown_signal()));

    let app = Router::new();
    let app = app.merge(web::routes(Arc::clone(&state)));
    collector.add(http_server_metrics());
    system.spawn(start_http_server(app, system.shutdown_signal(), HttpServerConfig::default()));

    system.spawn(collector.start(system.shutdown_signal()));
    system.wait().await;
    task::shutdown(Duration::from_secs(15)).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::hash;

    #[test]
    fn hash_with_host() {
        let hash = hash("host");
        assert_eq!(hash, "4740ae");
    }
}
