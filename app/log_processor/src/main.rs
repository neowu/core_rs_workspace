use std::fs;
use std::sync::Arc;
use std::time::Duration;

use chrono::FixedOffset;
use chrono::NaiveTime;
use framework::asset_path;
use framework::exception::Exception;
use framework::json;
use framework::log;
use framework::schedule::Scheduler;
use framework::spawn_action;
use framework::system::System;
use framework::task;
use framework_kafka::Topic;
use framework_kafka::consumer::ConsumerConfig;
use framework_kafka::consumer::MessageConsumer;
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

#[derive(Debug, Deserialize)]
struct AppConfig {
    log_appender: String,
    kafka_uri: String,
    elasticsearch_uri: String,
    kibana_uri: String,
    banner: String,
}

pub struct AppState {
    elasticsearch: Elasticsearch,
}

#[tokio::main]
async fn main() -> Result<(), Exception> {
    log::init();
    let config: AppConfig = json::load_file(&asset_path!("assets/conf.json")?)?;
    log::init_appender(&config.log_appender, env!("CARGO_BIN_NAME"))?;

    let mut system = System::new();

    let kibana_uri = config.kibana_uri;
    let banner = config.banner;
    spawn_action!("import_kibana_objects", async move {
        let objects = fs::read_to_string(&asset_path!("assets/kibana_objects.json")?)?;
        let objects = objects.replace("${NOTIFICATION_BANNER}", &banner);
        kibana::import(&kibana_uri, objects).await
    });

    let state = Arc::new(AppState { elasticsearch: Elasticsearch::new(config.elasticsearch_uri) });

    let scheduler_state = Arc::clone(&state);
    let mut scheduler = Scheduler::new(FixedOffset::east_opt(8 * 60 * 60).expect("value must be valid"));
    scheduler.schedule_daily(
        "cleanup_old_index_job",
        cleanup_old_index_job,
        NaiveTime::from_hms_opt(1, 0, 0).expect("value must be valid"),
    );
    system.spawn(scheduler.start(scheduler_state, system.shutdown_signal()));

    put_index_templates(&state.elasticsearch).await?;

    let mut consumer = MessageConsumer::new(config.kafka_uri, env!("CARGO_BIN_NAME"), &ConsumerConfig::default());
    consumer.add_bulk_handler(&Topic::new("action-log-v2"), action_log_message_handler);
    consumer.add_bulk_handler(&Topic::new("stat"), stat_message_handler);
    consumer.add_bulk_handler(&Topic::new("event"), event_message_handler);
    system.spawn(consumer.start(state, system.shutdown_signal()));

    system.wait().await;
    task::shutdown(Duration::from_secs(15)).await;

    Ok(())
}

async fn put_index_templates(elasticsearch: &Elasticsearch) -> Result<(), Exception> {
    elasticsearch
        .put_index_template("action", fs::read_to_string(&asset_path!("assets/index/action-index-template.json")?)?)
        .await?;
    elasticsearch
        .put_index_template("event", fs::read_to_string(&asset_path!("assets/index/event-index-template.json")?)?)
        .await?;
    elasticsearch
        .put_index_template("stat", fs::read_to_string(&asset_path!("assets/index/stat-index-template.json")?)?)
        .await?;
    elasticsearch
        .put_index_template("trace", fs::read_to_string(&asset_path!("assets/index/trace-index-template.json")?)?)
        .await?;
    Ok(())
}
