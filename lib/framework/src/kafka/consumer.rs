use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;
use std::time::Instant;

use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use futures::future::join_all;
use rdkafka::ClientConfig;
use rdkafka::Message as _;
use rdkafka::Timestamp;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::message::Headers;
use rdkafka::util::Timeout;
use serde::de::DeserializeOwned;
use tokio::sync::broadcast;
use tracing::debug;
use tracing::error;
use tracing::info;

use super::topic::Topic;
use crate::exception::CoreRsResult;
use crate::json::from_json;
use crate::log;

pub struct Message<T: DeserializeOwned> {
    pub key: Option<String>,
    pub payload: String,
    pub headers: HashMap<String, String>,
    pub timestamp: Option<DateTime<Utc>>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned> Message<T> {
    pub fn payload(&self) -> CoreRsResult<T> {
        from_json(&self.payload)
    }
}

trait MessageHandler<S>: Send {
    fn handle(&self, state: S, messages: Vec<BorrowedMessage>) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

impl<F, Fut, S> MessageHandler<S> for F
where
    F: Fn(S, Vec<BorrowedMessage>) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn handle(&self, state: S, messages: Vec<BorrowedMessage>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(self(state, messages))
    }
}

pub struct ConsumerConfig {
    pub poll_max_wait_time: Duration,
    pub poll_max_records: usize,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            poll_max_wait_time: Duration::from_secs(1),
            poll_max_records: 1000,
        }
    }
}

pub struct MessageConsumer<S> {
    config: ClientConfig,
    handlers: HashMap<&'static str, Box<dyn MessageHandler<S>>>,
    poll_max_wait_time: Duration,
    poll_max_records: usize,
}

impl<S> MessageConsumer<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn new(bootstrap_servers: &str, group_id: &str, config: ConsumerConfig) -> Self {
        Self {
            config: ClientConfig::new()
                .set("group.id", group_id)
                .set("bootstrap.servers", bootstrap_servers)
                .set("enable.auto.commit", "false")
                .set_log_level(RDKafkaLogLevel::Info)
                .to_owned(),
            handlers: HashMap::new(),
            poll_max_wait_time: config.poll_max_wait_time,
            poll_max_records: config.poll_max_records,
        }
    }

    pub fn add_handler<H, Fut, M>(&mut self, topic: &Topic<M>, handler: H)
    where
        H: Fn(S, Message<M>) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = CoreRsResult<()>> + Send + 'static,
        M: DeserializeOwned + Send + 'static,
    {
        let topic = topic.name;
        let handler = move |state: S, messages: Vec<BorrowedMessage>| {
            let messages: Vec<Message<M>> = messages.into_iter().map(Message::from).collect();
            handle_messages(topic, messages, handler, state)
        };

        self.handlers.insert(topic, Box::new(handler));
    }

    pub fn add_bulk_handler<H, Fut, M>(&mut self, topic: &Topic<M>, handler: H)
    where
        H: Fn(S, Vec<Message<M>>) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = CoreRsResult<()>> + Send + 'static,
        M: DeserializeOwned + Send + 'static,
    {
        let topic = topic.name;
        let handler = move |state: S, messages: Vec<BorrowedMessage>| {
            let messages: Vec<Message<M>> = messages.into_iter().map(Message::from).collect();
            handle_bulk_messages(topic, messages, handler, state)
        };

        self.handlers.insert(topic, Box::new(handler));
    }

    pub async fn start(self, state: S, mut shutdown_signel: broadcast::Receiver<()>) -> CoreRsResult<()> {
        let handlers = self.handlers;
        let consumer: BaseConsumer = self.config.create()?;
        let topics: Vec<&str> = handlers.keys().cloned().collect();
        consumer.subscribe(&topics)?;

        info!("kakfa consumer started, topics={:?}", topics);

        loop {
            match poll_message_groups(&consumer, self.poll_max_wait_time, self.poll_max_records) {
                Ok(topic_messages) => {
                    let mut handles = Vec::with_capacity(topic_messages.len());
                    for (topic, messages) in topic_messages {
                        if let Some(handler) = handlers.get(topic.as_str()) {
                            handles.push(tokio::spawn(handler.handle(state.clone(), messages)));
                        }
                    }
                    join_all(handles).await;
                    if let Err(e) = consumer.commit_consumer_state(CommitMode::Async) {
                        error!(error = ?e, "failed to commit messages");
                    }
                }
                Err(e) => {
                    error!(error = ?e, "failed to poll messages");
                    tokio::time::sleep(Duration::from_secs(5)).await
                }
            }

            if shutdown_signel.try_recv().is_ok() {
                info!("kakfa consumer stopped, topics={:?}", topics);
                return Ok(());
            }
        }
    }
}

impl<T: DeserializeOwned> From<BorrowedMessage<'_>> for Message<T> {
    fn from(message: BorrowedMessage) -> Message<T> {
        let key = message.key().map(|data| String::from_utf8_lossy(data).to_string());
        let value = message.payload().map(|data| String::from_utf8_lossy(data).to_string());

        let mut headers = HashMap::new();
        if let Some(kafka_headers) = message.headers() {
            for kafka_header in kafka_headers.iter() {
                headers.insert(
                    kafka_header.key.to_owned(),
                    kafka_header
                        .value
                        .map(|data| String::from_utf8_lossy(data).to_string())
                        .unwrap_or_default(),
                );
            }
        }

        let timestamp = match message.timestamp() {
            Timestamp::CreateTime(time) => DateTime::from_timestamp_millis(time),
            _ => None,
        };

        Message {
            key,
            payload: value.unwrap_or_default(),
            headers,
            timestamp,
            _marker: PhantomData,
        }
    }
}

fn poll_message_groups(
    consumer: &BaseConsumer,
    max_wait_time: Duration,
    max_records: usize,
) -> Result<HashMap<String, Vec<BorrowedMessage<'_>>>, KafkaError> {
    let mut messages: HashMap<String, Vec<BorrowedMessage>> = HashMap::new();
    let start_time = Instant::now();
    let mut count = 1;
    loop {
        let elapsed = start_time.elapsed();
        if elapsed >= max_wait_time {
            break;
        }

        if count >= max_records {
            break;
        }

        if let Some(result) = consumer.poll(Timeout::After(max_wait_time.saturating_sub(elapsed))) {
            let message = result?;
            let topic = message.topic().to_owned();
            messages.entry(topic).or_default().push(message);
            count += 1;
        }
    }
    Ok(messages)
}

async fn handle_bulk_messages<H, S, M, Fut>(topic: &'static str, messages: Vec<Message<M>>, handler: H, state: S)
where
    H: Fn(S, Vec<Message<M>>) -> Fut,
    Fut: Future<Output = CoreRsResult<()>>,
    M: DeserializeOwned,
{
    log::start_action("message", None, async {
        for message in messages.iter() {
            debug!(key = message.key, payload = message.payload, "[message]");
        }
        debug!(topic, "context");
        debug!(message_count = messages.len(), "stats");
        if let Some(timestamp) = messages.iter().filter_map(|message| message.timestamp).min() {
            let lag = Utc::now() - timestamp;
            debug!("lag={lag}");
        }
        handler(state, messages).await
    })
    .await;
}

struct MessageNode<M>
where
    M: DeserializeOwned,
{
    message: Message<M>,
    next: Option<Vec<MessageNode<M>>>,
}

async fn handle_messages<H, S, M, Fut>(topic: &'static str, messages: Vec<Message<M>>, handler: H, state: S)
where
    S: Clone + Send + Sync + 'static,
    H: Fn(S, Message<M>) -> Fut + Copy + Send + Sync + 'static,
    Fut: Future<Output = CoreRsResult<()>> + Send,
    M: DeserializeOwned + Send + 'static,
{
    let mut handles = Vec::with_capacity(messages.len());
    let mut nodes: HashMap<String, MessageNode<M>> = HashMap::new();
    for message in messages {
        if let Some(ref key) = message.key {
            if let Some(node) = nodes.get_mut(key) {
                if let Some(ref mut next) = node.next {
                    next.push(MessageNode { message, next: None });
                } else {
                    node.next = Some(vec![MessageNode { message, next: None }]);
                }
            } else {
                nodes.insert(key.to_owned(), MessageNode { message, next: None });
            }
        } else {
            let state = state.clone();
            handles.push(tokio::spawn(async move {
                handle_message(topic, message, handler, state).await
            }));
        }
    }

    for node in nodes.into_values() {
        let state = state.clone();
        handles.push(tokio::spawn(async move {
            handle_message(topic, node.message, handler, state.clone()).await;
            if let Some(next) = node.next {
                for next_node in next {
                    handle_message(topic, next_node.message, handler, state.clone()).await;
                }
            }
        }));
    }

    join_all(handles).await;
}

async fn handle_message<H, S, M, Fut>(topic: &'static str, message: Message<M>, handler: H, state: S)
where
    H: Fn(S, Message<M>) -> Fut,
    Fut: Future<Output = CoreRsResult<()>>,
    M: DeserializeOwned,
{
    let ref_id = message.headers.get("ref_id").map(|value| value.to_owned());
    log::start_action("message", ref_id, async {
        debug!(topic, "[message]");
        debug!(key = ?message.key, "[message]");
        debug!(
            timestamp = message
                .timestamp
                .map(|t| t.to_rfc3339_opts(SecondsFormat::Millis, true)),
            "[message]"
        );
        debug!(payload = message.payload, "[message]");
        for (key, value) in message.headers.iter() {
            debug!("[header] {}={}", key, value);
        }
        debug!(topic, key = message.key, "context");
        debug!(message_count = 1, "stats");
        if let Some(timestamp) = message.timestamp {
            let lag = Utc::now() - timestamp;
            debug!("lag={lag}");
        }
        handler(state, message).await
    })
    .await;
}
