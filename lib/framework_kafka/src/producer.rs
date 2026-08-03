use std::fmt::Debug;

use chrono::Utc;
use framework::console;
use framework::exception::Exception;
use framework::json::to_json;
use framework::log;
use framework::log::with_current_action;
use framework::span;
use framework::stats;
use rdkafka::ClientConfig;
use rdkafka::message::Header;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;
use serde::Serialize;

use crate::CLIENT;
use crate::REF_ID;
use crate::Topic;

pub struct Producer {
    producer: FutureProducer,
}

impl Producer {
    // client usually be env!("CARGO_BIN_NAME")
    pub fn new(bootstrap_servers: String) -> Self {
        console!("create kafka producer, broker={bootstrap_servers}");
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("compression.codec", "zstd")
            .create()
            .expect("failed to create producer");
        Self { producer }
    }

    pub async fn send<T>(&self, topic: &Topic<T>, key: Option<String>, message: &T) -> Result<(), Exception>
    where
        T: Serialize + Debug,
    {
        let _span = span!("kafka");
        let payload = to_json(message)?;

        let mut record =
            FutureRecord::<String, String>::to(topic.name).timestamp(Utc::now().timestamp_millis()).payload(&payload);

        if let Some(ref key) = key {
            record = record.key(key);
        }

        let mut headers = OwnedHeaders::new();
        if let Some((ref_id, app)) = with_current_action(|action| (action.id.to_string(), action.app)) {
            headers = headers
                .insert(Header { key: REF_ID, value: Some(&ref_id) })
                .insert(Header { key: CLIENT, value: Some(app) });
        }
        record = record.headers(headers);

        log!("send, topic={}, key={key:?}, payload={payload}", topic.name);
        stats!(kafka_write_messages = 1, kafka_write_bytes = payload.len());

        let result = self.producer.send(record, Timeout::Never).await;
        if let Err((err, _)) = result {
            return Err(err.into());
        }
        Ok(())
    }
}
