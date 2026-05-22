use std::sync::Arc;

use axum::Router;
use framework::asset_path;
use framework::exception::Exception;
use framework::json;
use framework::log;
use framework::log::appender::ConsoleAppender;
use framework::shutdown::listen_shutdown_signal;
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
    log::init_with_action(ConsoleAppender);

    let config: AppConfig = json::load_file(&asset_path!("assets/conf.json")?)?;

    let shutdown_signal = listen_shutdown_signal();

    let state = Arc::new(AppState {
        topics: Topics { event: Topic::new("event") },
        producer: Producer::new(config.kafka_uri, env!("CARGO_BIN_NAME")),
    });

    let app = Router::new();
    let app = app.merge(web::routes(state));
    start_http_server(app, shutdown_signal, HttpServerConfig::default()).await
}
