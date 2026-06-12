use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use framework::exception::Exception;
use framework::load_config;
use framework::log;
use framework::log::metrics::MetricsCollector;
use framework::system::System;
use framework::task;
use framework::web::server::HttpServerConfig;
use framework::web::server::http_server_metrics;
use framework::web::server::start_http_server;
use framework_kafka::Topic;
use framework_kafka::producer::Producer;
use kafka::EventMessage;
use serde::Deserialize;

mod kafka;
mod web;

#[derive(Debug, Deserialize)]
struct AppConfig {
    log_appender: String,
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
    let config: AppConfig = load_config!("assets/conf.json");
    log::init(&config.log_appender, env!("CARGO_PKG_NAME"));

    let mut system = System::new();
    let mut collector = MetricsCollector::new();

    let state = Arc::new(AppState {
        topics: Topics { event: Topic::new("event") },
        producer: Producer::new(config.kafka_uri, env!("CARGO_BIN_NAME")),
    });

    let app = Router::new();
    let app = app.merge(web::routes(state));
    system.spawn(start_http_server(app, system.shutdown_signal(), HttpServerConfig::default()));

    collector.add(http_server_metrics());
    system.spawn(collector.start(system.shutdown_signal()));

    system.wait().await;
    task::shutdown(Duration::from_secs(15)).await;
    Ok(())
}
