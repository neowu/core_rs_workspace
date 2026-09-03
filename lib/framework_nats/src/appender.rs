use async_nats::Client;
use async_nats::ConnectOptions;
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
    client: Client,
}

impl NatsAppender {
    pub async fn new(url: &str) -> Self {
        console!("start nats log appender, url={url}");
        let client = async_nats::connect_with_options(url, ConnectOptions::default())
            .await
            .unwrap_or_else(|e| panic!("failed to connect to log nats server, error={e}"));

        Self { client }
    }

    // the log stream captures log.>, so a core publish still lands in the stream, without the
    // jetstream reply inbox, whose ack the appender would never await anyway, and which the stream
    // never sends since it is created with no_ack
    async fn publish(&self, subject: &'static str, payload: String) {
        if let Err(e) = self.client.publish(subject, payload.into()).await {
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

    // publish only queues the message on the client channel, the connection task owns the socket,
    // so without this the buffered messages die with the runtime
    async fn flush(&self) {
        if let Err(e) = self.client.flush().await {
            console!("ERROR failed to flush log messages, error={e}");
        }
    }
}
