use std::fmt::Debug;

use chrono::Utc;
use framework::exception::Exception;
use framework::json::to_json;
use framework::log::current_action_id;
use framework::stats;
use rdkafka::ClientConfig;
use rdkafka::message::Header;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;
use serde::Serialize;
use tracing::Instrument as _;
use tracing::debug;
use tracing::debug_span;

use crate::Topic;

pub struct Producer {
    producer: FutureProducer,
    client: &'static str,
}

impl Producer {
    // client usually be env!("CARGO_BIN_NAME")
    pub fn new(bootstrap_servers: String, client: &'static str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("compression.codec", "zstd")
            .create()
            .expect("failed to create producer");
        Self { producer, client }
    }

    pub async fn send<T>(&self, topic: &Topic<T>, key: Option<String>, message: &T) -> Result<(), Exception>
    where
        T: Serialize + Debug,
    {
        let span = debug_span!("kafka");
        async {
            let payload = to_json(message)?;

            stats!(kafka_write_messages = 1, kafka_write_bytes = payload.len());

            let mut record = FutureRecord::<String, String>::to(topic.name)
                .timestamp(Utc::now().timestamp_millis())
                .payload(&payload);

            if let Some(ref key) = key {
                record = record.key(key);
            }

            let mut headers = OwnedHeaders::new().insert(Header { key: "client", value: Some(self.client) });
            if let Some(ref_id) = current_action_id() {
                headers = headers.insert(Header { key: "ref_id", value: Some(&ref_id) });
            }
            record = record.headers(headers);

            debug!(topic = topic.name, key, payload, "send");
            let result = self.producer.send(record, Timeout::Never).await;
            if let Err((err, _)) = result {
                return Err(err.into());
            }
            Ok(())
        }
        .instrument(span)
        .await
    }
}
