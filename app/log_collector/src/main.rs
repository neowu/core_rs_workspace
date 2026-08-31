use std::sync::Arc;

use axum::Router;
use framework::appender::ConsoleAppender;
use framework::appender::GCloudAppender;
use framework::exception::Exception;
use framework::load_config;
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
    let state = Arc::new(AppState {
        topics: Topics { event: Topic::new("event-v2") },
        producer: Producer::new(config.kafka_uri),
    });

    let app = Router::new();
    let app = app.merge(web::routes(state));
    let http_server = HttpServer::new(HttpServerConfig::default());
    system.add_metrics(http_server.metrics());

    let system = match config.log_appender.as_str() {
        "console" => system.start_logger(ConsoleAppender),
        "gcloud" => system.start_logger(GCloudAppender),
        value => panic!("unknown appender, value={value}"),
    };

    system.start_service(|token| http_server.start(app, token));

    system.wait().await;
    system.shutdown_logger().await
}
