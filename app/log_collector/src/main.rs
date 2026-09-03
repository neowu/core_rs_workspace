use std::sync::Arc;

use axum::Router;
use framework::load_config;
use framework::system::DefaultEnv;
use framework::system::System;
use framework::web::server::HttpServer;
use framework::web::server::HttpServerConfig;
use framework_kafka::Topic;
use framework_kafka::producer::Producer;
use framework_nats::appender::NatsAppender;
use kafka::EventMessage;
use serde::Deserialize;

mod kafka;
mod web;

#[derive(Debug, Deserialize)]
struct AppConfig {
    nats_appender_url: String,
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
async fn main() {
    let config: AppConfig = load_config!("assets/conf.json");

    let mut system = System::init(env!("CARGO_PKG_NAME"), DefaultEnv).await;
    let state = Arc::new(AppState {
        topics: Topics { event: Topic::new("event-v2") },
        producer: Producer::new(config.kafka_uri),
    });

    let app = Router::new();
    let app = app.merge(web::routes(state));
    let http_server = HttpServer::new(HttpServerConfig::default());
    system.add_metrics(http_server.metrics());

    let system = system.start_logger(NatsAppender::new(&config.nats_appender_url).await);

    system.start_service(|token| http_server.start(app, token));

    system.wait().await;
    system.shutdown_logger().await;
}
