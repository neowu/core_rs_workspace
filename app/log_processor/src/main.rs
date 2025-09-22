use std::fs;
use std::sync::Arc;

use framework::asset::asset_path;
use framework::exception::CoreRsResult;
use framework::json;
use framework::kafka::consumer::ConsumerConfig;
use framework::kafka::consumer::MessageConsumer;
use framework::kafka::topic::Topic;
use framework::log;
use framework::log::ConsoleAppender;
use framework::shutdown::Shutdown;
use framework::task;
use serde::Deserialize;

use crate::kafka::action_log_handler::action_log_message_handler;
use crate::kafka::event_handler::event_message_handler;
use crate::opensearch::Opensearch;

mod kafka;
mod kibana;
mod opensearch;

#[derive(Debug, Deserialize, Clone)]
struct AppConfig {
    kafka_uri: String,
    opensearch_uri: String,
    kibana_uri: String,
}

pub struct AppState {
    opensearch: Opensearch,
}

impl AppState {
    fn new(config: &AppConfig) -> CoreRsResult<Self> {
        Ok(AppState {
            opensearch: Opensearch::new(&config.opensearch_uri),
        })
    }
}

#[tokio::main]
async fn main() -> CoreRsResult<()> {
    log::init_with_action(ConsoleAppender);

    let config: AppConfig = json::load_file(&asset_path("assets/conf.json")?)?;

    let shutdown = Shutdown::new();
    let consumer_signal = shutdown.subscribe();
    shutdown.listen();

    let kibana_uri = config.kibana_uri.clone();
    task::spawn_action("import_kibana_objects", async move {
        let objects = fs::read_to_string(&asset_path("assets/kibana_objects.json")?)?;
        kibana::import(&kibana_uri, objects).await?;
        Ok(())
    });

    let state = Arc::new(AppState::new(&config)?);

    put_index_templates(&state.opensearch).await?;

    let mut consumer = MessageConsumer::new(&config.kafka_uri, env!("CARGO_BIN_NAME"), ConsumerConfig::default());
    consumer.add_bulk_handler(&Topic::new("action-log-v2"), action_log_message_handler);
    consumer.add_bulk_handler(&Topic::new("event"), event_message_handler);
    consumer.start(state, consumer_signal).await?;

    task::shutdown().await;

    Ok(())
}

async fn put_index_templates(opensearch: &Opensearch) -> CoreRsResult<()> {
    opensearch
        .put_index_template(
            "action",
            fs::read_to_string(&asset_path("assets/index/action-index-template.json")?)?,
        )
        .await?;
    opensearch
        .put_index_template(
            "event",
            fs::read_to_string(&asset_path("assets/index/event-index-template.json")?)?,
        )
        .await?;
    opensearch
        .put_index_template(
            "stat",
            fs::read_to_string(&asset_path("assets/index/stat-index-template.json")?)?,
        )
        .await?;
    opensearch
        .put_index_template(
            "trace",
            fs::read_to_string(&asset_path("assets/index/trace-index-template.json")?)?,
        )
        .await?;
    Ok(())
}
