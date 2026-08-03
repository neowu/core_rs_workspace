use std::fmt::Debug;

use async_nats::Client;
use async_nats::jetstream;
use async_nats::jetstream::Context;
use framework::console;
use framework::exception::Exception;
use framework::json::to_json;
use framework::log;
use framework::span;
use framework::stats;
use serde::Serialize;

use crate::Subject;
use crate::link_context;

pub struct Producer {
    context: Context,
}

impl Producer {
    // client usually be env!("CARGO_BIN_NAME")
    pub fn new(client: Client) -> Self {
        console!("create nats producer, server={}", client.server_info().server_name);
        Self { context: jetstream::new(client) }
    }

    pub async fn send<T>(&self, subject: &Subject<T>, message: &T) -> Result<(), Exception>
    where
        T: Serialize + Debug,
    {
        let _span = span!("nats");
        let headers = link_context();
        let payload = to_json(message)?;
        let len = payload.len();
        log!("send, subject={}, payload={payload}", subject.name);
        let _ack = self.context.publish_with_headers(subject.name, headers, payload.into()).await?;
        stats!(nats_write_messages = 1, nats_write_bytes = len);
        Ok(())
    }
}
