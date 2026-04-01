use std::fs;
use std::fs::read_to_string;
use std::sync::Arc;

use axum::Router;
use chrono::FixedOffset;
use chrono::NaiveTime;
use framework::asset::asset_path;
use framework::exception::Exception;
use framework::json;
use framework::kafka::consumer::ConsumerConfig;
use framework::kafka::consumer::MessageConsumer;
use framework::kafka::topic::Topic;
use framework::log;
use framework::log::ConsoleAppender;
use framework::number::parse_u32;
use framework::schedule::Scheduler;
use framework::shutdown::Shutdown;
use framework::task;
use framework::web::server::HttpServerConfig;
use framework::web::server::start_http_server;
use job::process_log_job;
use kafka::action_log_handler::ActionLogMessage;
use kafka::action_log_handler::action_log_message_handler;
use kafka::event_handler::EventMessage;
use kafka::event_handler::event_message_handler;
use serde::Deserialize;
use sha2::Digest;
use sha2::Sha256;
use tracing::info;

mod job;
mod kafka;
mod service;
mod web;

#[derive(Debug, Deserialize, Clone)]
struct AppConfig {
    kafka_uri: String,
    log_dir: String,
    bucket: String,
}

pub struct AppState {
    topics: Topics,

    log_dir: String,
    hash: String,
    bucket: String,

    duckdb_memory_limit: u32, // in bytes
}

impl AppState {
    fn new(config: &AppConfig) -> Result<Self, Exception> {
        let hostname = hostname::get()?.to_string_lossy().to_string();
        let hash = hash(&hostname);

        Ok(AppState {
            topics: Topics {
                action: Topic::new("action-log-v2"),
                event: Topic::new("event"),
            },
            log_dir: config.log_dir.clone(),
            hash,
            bucket: config.bucket.clone(),
            duckdb_memory_limit: duckdb_memory_limit()?,
        })
    }
}

fn hash(hostname: &str) -> String {
    let hash = Sha256::digest(hostname);
    hex::encode(hash)[0..6].to_owned()
}

fn duckdb_memory_limit() -> Result<u32, Exception> {
    if fs::exists("/sys/fs/cgroup/memory.max")? {
        let max_memory = parse_u32(read_to_string("/sys/fs/cgroup/memory.max")?.trim())?;
        info!("detected cgroup v2, max_memory={max_memory}, set duckdb_memory_limit to 50%");
        Ok(max_memory / 2)
    } else {
        info!("not in cgroup v2 env, set duckdb_memory_limit to 200MB");
        Ok(200_000_000)
    }
}

struct Topics {
    action: Topic<ActionLogMessage>,
    event: Topic<EventMessage>,
}

#[tokio::main]
async fn main() -> Result<(), Exception> {
    log::init_with_action(ConsoleAppender);

    let config: AppConfig = json::load_file(&asset_path("assets/conf.json")?)?;

    let shutdown = Shutdown::new();
    let http_signal = shutdown.subscribe();
    let scheduler_signal = shutdown.subscribe();
    let consumer_signal = shutdown.subscribe();
    shutdown.listen();

    let state = Arc::new(AppState::new(&config)?);
    let scheduler_state = state.clone();
    let consumer_state = state.clone();

    task::spawn_task(async move {
        let mut scheduler = Scheduler::new(FixedOffset::east_opt(8 * 60 * 60).unwrap());
        scheduler.schedule_daily(
            "process_log_job",
            process_log_job,
            NaiveTime::from_hms_opt(1, 0, 0).unwrap(),
        );
        scheduler.start(scheduler_state, scheduler_signal).await
    });

    task::spawn_task(async move {
        let mut consumer = MessageConsumer::new(&config.kafka_uri, env!("CARGO_BIN_NAME"), ConsumerConfig::default());
        consumer.add_bulk_handler(&consumer_state.topics.action, action_log_message_handler);
        consumer.add_bulk_handler(&consumer_state.topics.event, event_message_handler);
        consumer.start(consumer_state, consumer_signal).await
    });

    let app = Router::new();
    let app = app.merge(web::routes());
    let app = app.with_state(state);
    start_http_server(app, http_signal, HttpServerConfig::default()).await?;

    task::shutdown().await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::hash;

    #[test]
    fn test_hash() {
        let hash = hash("host");
        assert_eq!(hash, "4740ae");
    }
}
