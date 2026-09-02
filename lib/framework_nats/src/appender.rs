use std::time::Duration;

use async_nats::ConnectOptions;
use async_nats::jetstream::Context;
use async_nats::jetstream::ContextBuilder;
use framework::appender::ActionMessage;
use framework::appender::Appender;
use framework::appender::ConsoleAppender;
use framework::appender::MetricsMessage;
use framework::console;
use framework::json::to_json;
use framework::log::Severity;

pub const ACTION_SUBJECT: &str = "log.action";
pub const METRICS_SUBJECT: &str = "log.metrics";

pub struct NatsAppender {
    context: Context,
}

impl NatsAppender {
    pub async fn new(url: &str) -> Self {
        console!("start nats log appender, url={url}");
        let client = async_nats::connect_with_options(url, ConnectOptions::default())
            .await
            .unwrap_or_else(|e| panic!("failed to connect to log nats server, error={e}"));

        let context = ContextBuilder::new()
            .timeout(Duration::from_secs(15))
            .max_ack_inflight(20_000)
            .backpressure_on_inflight(false) // fail fast with MaxAckPending instead of waiting
            .build(client);

        Self { context }
    }

    // the ack is deliberately not awaited, the appender is a single daemon task and a server round trip
    // per message would cap the throughput of the whole app
    async fn publish(&self, subject: &'static str, payload: String) {
        if let Err(e) = self.context.publish(subject, payload.into()).await {
            console!("ERROR failed to publish log message, subject={subject}, error={e}");
        }
    }
}

impl Appender for NatsAppender {
    async fn append_action(&self, action: ActionMessage) {
        match to_json(&action) {
            Ok(payload) => self.publish(ACTION_SUBJECT, payload).await,
            Err(e) => console!("ERROR failed to serialize action, error={e}"),
        }

        if action.severity == Severity::Error {
            ConsoleAppender.append_action(action).await;
        }
    }

    async fn append_metrics(&self, metrics: MetricsMessage) {
        match to_json(&metrics) {
            Ok(payload) => self.publish(METRICS_SUBJECT, payload).await,
            Err(e) => console!("ERROR failed to serialize metrics, error={e}"),
        }

        if metrics.severity == Severity::Error {
            ConsoleAppender.append_metrics(metrics).await;
        }
    }
}
