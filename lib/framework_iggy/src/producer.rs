use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Mutex;

use bytes::Bytes;
use framework::console;
use framework::exception::Exception;
use framework::json::to_json;
use framework::log;
use framework::span;
use framework::stats;
use framework::system;
use iggy::prelude::HeaderKey;
use iggy::prelude::HeaderValue;
use iggy::prelude::Identifier;
use iggy::prelude::IggyClient;
use iggy::prelude::IggyMessage;
use iggy::prelude::MessageClient as _;
use iggy::prelude::Partitioning;
use serde::Serialize;

use crate::CLIENT;
use crate::KEY;
use crate::REF_ID;
use crate::Topic;
use crate::connect;
use crate::create_stream_if_not_exists;
use crate::create_topic_if_not_exists;

pub struct Producer {
    client: IggyClient,
    stream: Identifier,
    // topics known to exist, created on first send
    topics: Mutex<HashSet<&'static str>>,
}

impl Producer {
    pub async fn new(url: &str, stream: &'static str) -> Producer {
        console!("create iggy producer, stream={stream}");
        let client = connect(url).await;
        create_stream_if_not_exists(&client, stream).await.expect("failed to create stream"); // fail fast on startup
        let stream = Identifier::named(stream).expect("invalid stream name");
        Producer { client, stream, topics: Mutex::default() }
    }

    // low level send_messages, so one producer serves every topic of its stream; each send is one request awaiting
    // the server
    pub async fn send<T>(&self, topic: &Topic<T>, key: Option<String>, message: &T) -> Result<(), Exception>
    where
        T: Serialize + Debug,
    {
        let _span = span!("iggy");
        let payload = to_json(message)?;

        let mut headers: BTreeMap<HeaderKey, HeaderValue> = BTreeMap::new();
        if let Some(app) = system::app() {
            headers.insert(HeaderKey::try_from(CLIENT)?, HeaderValue::try_from(app)?);
        }
        if let Some(ref_id) = log::current_action_id() {
            headers.insert(HeaderKey::try_from(REF_ID)?, HeaderValue::try_from(ref_id)?);
        }
        let partitioning = match key {
            Some(ref key) => {
                headers.insert(HeaderKey::try_from(KEY)?, HeaderValue::try_from(key.as_str())?);
                Partitioning::messages_key_str(key)?
            }
            None => Partitioning::balanced(),
        };

        log!("send, topic={}, key={key:?}, payload={payload}", topic.name);
        stats!(iggy_write_messages = 1, iggy_write_bytes = payload.len());

        let topic_id = Identifier::named(topic.name)?;
        if !self.topics.lock().expect("lock should not be poisoned").contains(topic.name) {
            create_topic_if_not_exists(&self.client, &self.stream, topic.name).await?;
            self.topics.lock().expect("lock should not be poisoned").insert(topic.name);
        }

        let iggy_message = IggyMessage::builder()
            .payload(Bytes::from(payload))
            .maybe_user_headers((!headers.is_empty()).then_some(headers))
            .build()?;
        self.client.send_messages(&self.stream, &topic_id, &partitioning, &mut [iggy_message]).await?;
        Ok(())
    }
}
