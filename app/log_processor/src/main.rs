use std::fs;
use std::sync::Arc;
use std::time::Duration;

use chrono::FixedOffset;
use chrono::NaiveTime;
use framework::asset_path;
use framework::config::EnvString;
use framework::console;
use framework::context;
use framework::exception::Exception;
use framework::load_config;
use framework::log;
use framework::log::metrics::MetricsCollector;
use framework::schedule::Scheduler;
use framework::spawn_action;
use framework::system::System;
use framework::task;
use framework_clickhouse::ClickHouse;
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
    clickhouse_uri: String,
    clickhouse_user: String,
    clickhouse_password: EnvString,
    kibana_uri: String,
    banner: String,
}

pub struct AppState {
    elasticsearch: Elasticsearch,
    clickhouse: ClickHouse,
}

#[tokio::main]
async fn main() -> Result<(), Exception> {
    let config: AppConfig = load_config!("assets/conf.json");
    log::init(&config.log_appender, env!("CARGO_BIN_NAME"));

    let mut system = System::new();
    let mut collector = MetricsCollector::new();

    let kibana_uri = config.kibana_uri;
    let banner = config.banner;

    // kibana init is fallible
    spawn_action!("import_kibana_objects", async move {
        let objects = fs::read_to_string(asset_path!("assets/kibana_objects.json"))?;
        let objects = objects.replace("${NOTIFICATION_BANNER}", &banner);
        kibana::import(&kibana_uri, objects).await?;
        console!("kibana objects are imported");
        Ok(())
    });

    let state = Arc::new(AppState {
        elasticsearch: Elasticsearch::new(config.elasticsearch_uri),
        clickhouse: ClickHouse::new(
            config.clickhouse_uri.clone(),
            config.clickhouse_user.clone(),
            &config.clickhouse_password,
            Some("log"),
        ),
    });

    let scheduler_state = Arc::clone(&state);
    let mut scheduler = Scheduler::new(FixedOffset::east_opt(8 * 60 * 60).expect("value must be valid"));
    scheduler.schedule_daily(
        "cleanup_old_index_job",
        cleanup_old_index_job,
        NaiveTime::from_hms_opt(1, 0, 0).expect("value must be valid"),
    );
    system.spawn(scheduler.start(scheduler_state, system.shutdown_signal()));

    let clickhouse = ClickHouse::new(config.clickhouse_uri, config.clickhouse_user, &config.clickhouse_password, None);
    init_clickhouse(clickhouse).await?;
    init_elasticsearch(&state.elasticsearch).await?;

    let mut consumer = MessageConsumer::new(config.kafka_uri, env!("CARGO_BIN_NAME"), &ConsumerConfig::default());
    consumer.add_bulk_handler(&Topic::new("action-log-v2"), action_log_message_handler);
    consumer.add_bulk_handler(&Topic::new("stat"), stat_message_handler);
    consumer.add_bulk_handler(&Topic::new("event"), event_message_handler);
    collector.add(consumer.consumer_metrics());
    system.spawn(consumer.start(state, system.shutdown_signal()));

    system.spawn(collector.start(system.shutdown_signal()));
    system.wait().await;
    task::shutdown(Duration::from_secs(15)).await;

    Ok(())
}

async fn init_elasticsearch(elasticsearch: &Elasticsearch) -> Result<(), Exception> {
    console!("init elasticsearch");

    log::action("task", None, async {
        context!(task = "init_elasticsearch");
        elasticsearch
            .put_index_template("action", fs::read_to_string(asset_path!("assets/index/action-index-template.json"))?)
            .await?;
        elasticsearch
            .put_index_template("event", fs::read_to_string(asset_path!("assets/index/event-index-template.json"))?)
            .await?;
        elasticsearch
            .put_index_template("stat", fs::read_to_string(asset_path!("assets/index/stat-index-template.json"))?)
            .await?;
        elasticsearch
            .put_index_template("trace", fs::read_to_string(asset_path!("assets/index/trace-index-template.json"))?)
            .await?;
        Ok(())
    })
    .await
}

async fn init_clickhouse(clickhouse: ClickHouse) -> Result<(), Exception> {
    console!("init clickhouse");

    log::action("task", None, async {
        context!(task = "init_clickhouse");

        // clickhouse.execute("DROP DATABASE IF EXISTS log").await?;

        clickhouse.execute("CREATE DATABASE IF NOT EXISTS log").await?;
        clickhouse.execute(&fs::read_to_string(asset_path!("assets/clickhouse/action.sql"))?).await?;
        clickhouse.execute(&fs::read_to_string(asset_path!("assets/clickhouse/trace.sql"))?).await?;

        // read-only account: SELECT on the log database is its only grant.
        clickhouse.execute("CREATE USER IF NOT EXISTS viewer IDENTIFIED BY 'viewer'").await?;
        clickhouse.execute("GRANT SELECT ON log.* TO viewer").await?;
        Ok(())
    })
    .await
}
