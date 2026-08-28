use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use framework::exception::Exception;
use framework::load_config;
use framework::log::ConsoleAppender;
use framework::log::GCloudAppender;
use framework::metrics::MetricsCollector;
use framework::system::System;
use framework::web::server::HttpServer;
use framework::web::server::HttpServerConfig;
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

    let mut system = System::init(env!("CARGO_PKG_NAME"));
    match config.log_appender.as_str() {
        "console" => system.start_action_logger(ConsoleAppender),
        "gcloud" => system.start_action_logger(GCloudAppender),
        value => panic!("unknown appender, value={value}"),
    }

    let mut collector = MetricsCollector::new();

    let state = Arc::new(AppState {
        topics: Topics { event: Topic::new("event-v2") },
        producer: Producer::new(config.kafka_uri),
    });

    let app = Router::new();
    let app = app.merge(web::routes(state));
    let http_server = HttpServer::new(HttpServerConfig::default());
    collector.add(http_server.metrics());
    system.start_service(|token| http_server.start(app, token));

    match config.log_appender.as_str() {
        "console" => system.start_metrics_collector(collector, ConsoleAppender),
        "gcloud" => system.start_metrics_collector(collector, GCloudAppender),
        value => panic!("unknown appender, value={value}"),
    }

    system.wait().await;
    system.shutdown(Duration::from_secs(15)).await
}
