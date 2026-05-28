use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use framework::asset_path;
use framework::exception::Exception;
use framework::json;
use framework::log;
use framework::system::System;
use framework::task;
use framework::web::server::HttpServerConfig;
use framework::web::server::start_http_server;
use framework_kafka::Topic;
use framework_kafka::producer::Producer;
use kafka::EventMessage;
use serde::Deserialize;

mod kafka;
mod web;

#[derive(Debug, Deserialize)]
struct AppConfig {
    action_appender: String,
    kafka_uri: String,
}

pub struct AppState {
    topics: Topics,
    producer: Producer,
}

struct Topics {
    event: Topic<EventMessage>,
}

#[tokio::main]
async fn main() -> Result<(), Exception> {
    log::init();
    let config: AppConfig = json::load_file(&asset_path!("assets/conf.json")?)?;
    log::init_action_appender(&config.action_appender, env!("CARGO_BIN_NAME"))?;

    let mut system = System::new();

    let state = Arc::new(AppState {
        topics: Topics { event: Topic::new("event") },
        producer: Producer::new(config.kafka_uri, env!("CARGO_BIN_NAME")),
    });

    let app = Router::new();
    let app = app.merge(web::routes(state));
    system.spawn(start_http_server(app, system.shutdown_signal(), HttpServerConfig::default()));

    system.wait().await;
    task::shutdown(Duration::from_secs(15)).await;
    Ok(())
}
