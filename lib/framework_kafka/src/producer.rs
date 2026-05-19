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
    client: String,
}

impl Producer {
    pub fn new(bootstrap_servers: String, client: String) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("compression.codec", "zstd")
            .create()
            .expect("Producer creation error");
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

            let mut headers = insert_header(OwnedHeaders::new(), "client", &self.client);
            if let Some(ref ref_id) = current_action_id() {
                headers = insert_header(headers, "ref_id", ref_id);
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

fn insert_header(headers: OwnedHeaders, key: &str, value: &str) -> OwnedHeaders {
    headers.insert(Header { key, value: Some(value.as_bytes()) })
}
