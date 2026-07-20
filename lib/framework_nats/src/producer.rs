use std::fmt::Debug;

use async_nats::HeaderMap;
use async_nats::jetstream;
use async_nats::jetstream::Context;
use framework::console;
use framework::exception::Exception;
use framework::json::to_json;
use framework::log;
use framework::log::current_action_id;
use framework::span;
use framework::stats;
use serde::Serialize;

use crate::CLIENT;
use crate::REF_ID;
use crate::Subject;

pub struct Producer {
    jetstream: Context,
    client: &'static str,
}

impl Producer {
    // client usually be env!("CARGO_BIN_NAME")
    pub async fn new(url: String, client: &'static str) -> Self {
        console!("create nats producer, url={url}");
        let connection = async_nats::connect(url).await.expect("failed to connect nats"); // fail fast on startup
        Self { jetstream: jetstream::new(connection), client }
    }

    pub async fn send<T>(&self, subject: &Subject<T>, message: &T) -> Result<(), Exception>
    where
        T: Serialize + Debug,
    {
        let _span = span!("nats");
        let payload = to_json(message)?;

        stats!(nats_write_messages = 1, nats_write_bytes = payload.len());

        let mut headers = HeaderMap::new();
        headers.insert(CLIENT, self.client);
        if let Some(ref_id) = current_action_id() {
            headers.insert(REF_ID, ref_id);
        }

        log!("send, subject={}, payload={payload}", subject.name);
        let _ack = self.jetstream.publish_with_headers(subject.name, headers, payload.into()).await?;
        Ok(())
    }
}
