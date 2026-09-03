use std::fs;
use std::sync::Arc;
use std::time::Duration;

use framework::appender::ConsoleAppender;
use framework::asset_path;
use framework::cloud::CloudRunEnv;
use framework::config::EnvString;
use framework::console;
use framework::context;
use framework::exception::Exception;
use framework::load_config;
use framework::log;
use framework::system::System;
use framework::task::start_executor;
use framework_clickhouse::ClickHouse;
use framework_nats::Subject;
use framework_nats::appender::ACTION_SUBJECT;
use framework_nats::appender::METRICS_SUBJECT;
use framework_nats::async_nats::Client;
use framework_nats::async_nats::jetstream;
use framework_nats::async_nats::jetstream::stream::Config;
use framework_nats::consumer::BatchConsumer;
use framework_nats::consumer::ConsumerConfig;
use framework_nats::consumer::consumer_metrics;
use serde::Deserialize;

use crate::alert::AlertService;
use crate::alert::slack::SlackClient;
use crate::nats::action_handler::action_message_handler;
use crate::nats::metrics_handler::metrics_message_handler;

mod alert;
mod nats;

#[derive(Debug, Deserialize)]
struct AppConfig {
    nats_uri: String,
    clickhouse: ClickhouseConfig,
    slack: Option<SlackConfig>,
}

#[derive(Debug, Deserialize)]
struct ClickhouseConfig {
    uri: String,
    user: String,
    password: EnvString,
}

#[derive(Debug, Deserialize)]
struct SlackConfig {
    token: EnvString,
    error_channel: String,
    warn_channel: String,
}

pub struct AppState {
    clickhouse: ClickHouse,
    alert_service: Option<AlertService>,
}

#[tokio::main]
async fn main() -> Result<(), Exception> {
    let config: AppConfig = load_config!("assets/conf.json", env = "CONFIG");

    let mut system = System::init(env!("CARGO_PKG_NAME"), CloudRunEnv).await;
    system.add_metrics(consumer_metrics());

    // slack notification runs on the executor, out of the consumer path
    let executor = start_executor();
    system.add_metrics(executor.metrics());

    // the app must not log to nats itself, it consumes the same stream and would amplify its own actions
    let system = system.start_logger(ConsoleAppender);

    let client = framework_nats::connect(&config.nats_uri).await;
    init_jetstream(client.clone()).await?;

    let clickhouse = &config.clickhouse;
    init_clickhouse(ClickHouse::new(&clickhouse.uri, &clickhouse.user, &clickhouse.password, None)).await?;

    let state = Arc::new(AppState {
        clickhouse: ClickHouse::new(&clickhouse.uri, &clickhouse.user, &clickhouse.password, Some("log")),
        alert_service: alert_service(config.slack),
    });

    // one batch per consumer becomes one clickhouse insert, so favor larger batches over latency
    let consumer_config =
        ConsumerConfig { batch_max_messages: 5_000, batch_max_wait: Duration::from_secs(3), ..Default::default() };

    let action_consumer = BatchConsumer::new(
        client.clone(),
        "log",
        concat!(env!("CARGO_BIN_NAME"), "_action"),
        &Subject::new(ACTION_SUBJECT),
        action_message_handler,
        consumer_config,
    );

    let metrics_consumer = BatchConsumer::new(
        client,
        "log",
        concat!(env!("CARGO_BIN_NAME"), "_metrics"),
        &Subject::new(METRICS_SUBJECT),
        metrics_message_handler,
        consumer_config,
    );

    let action_state = Arc::clone(&state);
    system.start_service(|token| action_consumer.start(action_state, token));
    system.start_service(|token| metrics_consumer.start(state, token));

    system.wait().await;
    executor.shutdown(Duration::from_secs(15)).await;
    system.shutdown_logger().await;

    Ok(())
}

// alerting stays off until the token and both channels are configured
fn alert_service(config: Option<SlackConfig>) -> Option<AlertService> {
    if let Some(config) = config {
        let token = String::from(config.token);
        assert!(!token.is_empty(), "slack token must not be empty");
        assert!(!config.error_channel.is_empty(), "slack error channel must not be empty");
        assert!(!config.warn_channel.is_empty(), "slack warn channel must not be empty");
        Some(AlertService::new(SlackClient::new(token, config.error_channel, config.warn_channel)))
    } else {
        None
    }
}

// the appender only publishes, the stream itself is owned here
async fn init_jetstream(client: Client) -> Result<(), Exception> {
    console!("init log jetstream");
    log::action("task", None, async {
        context!(task = "init_jetstream");

        let jetstream = jetstream::new(client);
        let config = Config {
            name: "log".to_owned(),
            subjects: vec!["log.>".to_owned()],
            max_age: Duration::from_hours(24 * 7),
            no_ack: true,
            ..Default::default()
        };
        jetstream.create_or_update_stream(config).await?;
        Ok(())
    })
    .await
}

async fn init_clickhouse(clickhouse: ClickHouse) -> Result<(), Exception> {
    console!("init clickhouse");

    log::action("task", None, async {
        context!(task = "init_clickhouse");

        clickhouse.execute("CREATE DATABASE IF NOT EXISTS log", &[]).await?;
        clickhouse.execute(&fs::read_to_string(asset_path!("assets/clickhouse/action.sql"))?, &[]).await?;
        clickhouse.execute(&fs::read_to_string(asset_path!("assets/clickhouse/trace.sql"))?, &[]).await?;
        clickhouse.execute(&fs::read_to_string(asset_path!("assets/clickhouse/metrics.sql"))?, &[]).await?;

        Ok(())
    })
    .await
}
