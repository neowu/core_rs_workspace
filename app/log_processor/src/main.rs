use std::fs;
use std::sync::Arc;

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
use framework::schedule::Scheduler;
use framework::shutdown::Shutdown;
use framework::task;
use serde::Deserialize;

use crate::elasticsearch::Elasticsearch;
use crate::job::cleanup_old_index_job;
use crate::kafka::action_log_handler::action_log_message_handler;
use crate::kafka::event_handler::event_message_handler;
use crate::kafka::stat_handler::stat_message_handler;

mod elasticsearch;
mod job;
mod kafka;
mod kibana;

#[derive(Debug, Deserialize, Clone)]
struct AppConfig {
    kafka_uri: String,
    elasticsearch_uri: String,
    kibana_uri: String,
    banner: String,
}

pub struct AppState {
    elasticsearch: Elasticsearch,
}

impl AppState {
    fn new(config: &AppConfig) -> Result<Self, Exception> {
        Ok(AppState {
            elasticsearch: Elasticsearch::new(&config.elasticsearch_uri),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Exception> {
    log::init_with_action(ConsoleAppender);

    let config: AppConfig = json::load_file(&asset_path("assets/conf.json")?)?;

    let shutdown = Shutdown::new();
    let consumer_signal = shutdown.subscribe();
    let scheduler_signal = shutdown.subscribe();
    shutdown.listen();

    let kibana_uri = config.kibana_uri.clone();
    let banner = config.banner.clone();
    task::spawn_action("import_kibana_objects", async move {
        let objects = fs::read_to_string(&asset_path("assets/kibana_objects.json")?)?;
        let objects = objects.replace("${NOTIFICATION_BANNER}", &banner);
        kibana::import(&kibana_uri, objects).await?;
        Ok(())
    });

    let state = Arc::new(AppState::new(&config)?);

    let scheduler_state = state.clone();
    task::spawn_task(async move {
        let mut scheduler = Scheduler::new(FixedOffset::east_opt(8 * 60 * 60).unwrap());
        scheduler.schedule_daily(
            "cleanup_old_index_job",
            cleanup_old_index_job,
            NaiveTime::from_hms_opt(1, 0, 0).unwrap(),
        );
        scheduler.start(scheduler_state, scheduler_signal).await
    });

    put_index_templates(&state.elasticsearch).await?;

    let mut consumer = MessageConsumer::new(&config.kafka_uri, env!("CARGO_BIN_NAME"), ConsumerConfig::default());
    consumer.add_bulk_handler(&Topic::new("action-log-v2"), action_log_message_handler);
    consumer.add_bulk_handler(&Topic::new("stat"), stat_message_handler);
    consumer.add_bulk_handler(&Topic::new("event"), event_message_handler);
    consumer.start(state, consumer_signal).await?;

    task::shutdown().await;

    Ok(())
}

async fn put_index_templates(elasticsearch: &Elasticsearch) -> Result<(), Exception> {
    elasticsearch
        .put_index_template(
            "action",
            fs::read_to_string(&asset_path("assets/index/action-index-template.json")?)?,
        )
        .await?;
    elasticsearch
        .put_index_template(
            "event",
            fs::read_to_string(&asset_path("assets/index/event-index-template.json")?)?,
        )
        .await?;
    elasticsearch
        .put_index_template(
            "stat",
            fs::read_to_string(&asset_path("assets/index/stat-index-template.json")?)?,
        )
        .await?;
    elasticsearch
        .put_index_template(
            "trace",
            fs::read_to_string(&asset_path("assets/index/trace-index-template.json")?)?,
        )
        .await?;
    Ok(())
}
