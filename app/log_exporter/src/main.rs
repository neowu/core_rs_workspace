use std::sync::Arc;

use axum::Router;
use chrono::FixedOffset;
use chrono::NaiveTime;
use framework::asset::asset_path;
use framework::exception::CoreRsResult;
use framework::json;
use framework::kafka::consumer::ConsumerConfig;
use framework::kafka::consumer::MessageConsumer;
use framework::kafka::topic::Topic;
use framework::log;
use framework::log::ConsoleAppender;
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
}

impl AppState {
    fn new(config: &AppConfig) -> CoreRsResult<Self> {
        let hostname = hostname::get()?.to_string_lossy().to_string();
        let hash = &format!("{:x}", sha2::Sha256::digest(hostname))[0..6];

        Ok(AppState {
            topics: Topics {
                action: Topic::new("action-log-v2"),
                event: Topic::new("event"),
            },
            log_dir: config.log_dir.clone(),
            hash: hash.to_owned(),
            bucket: config.bucket.clone(),
        })
    }
}

struct Topics {
    action: Topic<ActionLogMessage>,
    event: Topic<EventMessage>,
}

#[tokio::main]
async fn main() -> CoreRsResult<()> {
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
            "process-log-job",
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
