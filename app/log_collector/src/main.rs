#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use std::sync::Arc;

use axum::Router;
use framework::asset::asset_path;
use framework::exception::Exception;
use framework::json;
use framework::kafka::producer::Producer;
use framework::kafka::topic::Topic;
use framework::log;
use framework::log::ConsoleAppender;
use framework::shutdown::Shutdown;
use framework::web::server::HttpServerConfig;
use framework::web::server::start_http_server;
use kafka::EventMessage;
use serde::Deserialize;

mod kafka;
mod web;

#[derive(Debug, Deserialize, Clone)]
struct AppConfig {
    kafka_uri: String,
}

pub struct AppState {
    topics: Topics,
    producer: Producer,
}

impl AppState {
    fn new(config: &AppConfig) -> Result<Self, Exception> {
        Ok(AppState {
            topics: Topics {
                event: Topic::new("event"),
            },
            producer: Producer::new(&config.kafka_uri, env!("CARGO_BIN_NAME")),
        })
    }
}

struct Topics {
    event: Topic<EventMessage>,
}

#[tokio::main]
async fn main() -> Result<(), Exception> {
    log::init_with_action(ConsoleAppender);

    let config: AppConfig = json::load_file(&asset_path("assets/conf.json")?)?;

    let shutdown = Shutdown::new();
    let signal = shutdown.subscribe();
    shutdown.listen();

    let state = Arc::new(AppState::new(&config)?);

    let app = Router::new();
    let app = app.merge(web::routes());
    let app = app.with_state(state);
    start_http_server(app, signal, HttpServerConfig::default()).await
}
