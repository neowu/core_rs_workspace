use std::any::type_name;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;
use std::time::Instant;

use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use framework::context;
use framework::exception::Exception;
use framework::json::from_json;
use framework::log;
use framework::stats;
use futures::future::join_all;
use rdkafka::ClientConfig;
use rdkafka::Message as _;
use rdkafka::Timestamp;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer as _;
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::message::Headers as _;
use rdkafka::util::Timeout;
use serde::de::DeserializeOwned;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::Topic;

pub struct Message<T: DeserializeOwned> {
    pub key: Option<String>,
    pub payload: String,
    pub headers: HashMap<String, String>,
    pub timestamp: Option<DateTime<Utc>>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned> Message<T> {
    pub fn payload(&self) -> Result<T, Exception> {
        from_json(&self.payload)
    }
}

type MessageHandler<S> = Box<dyn Fn(S, Vec<BorrowedMessage>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

pub struct ConsumerConfig {
    pub poll_max_wait_time: Duration,
    pub poll_max_records: usize,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self { poll_max_wait_time: Duration::from_secs(1), poll_max_records: 1000 }
    }
}

pub struct MessageConsumer<S> {
    config: ClientConfig,
    handlers: HashMap<&'static str, MessageHandler<S>>,
    poll_max_wait_time: Duration,
    poll_max_records: usize,
}

impl<S> MessageConsumer<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn new(bootstrap_servers: String, group_id: String, config: &ConsumerConfig) -> Self {
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
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
        M: DeserializeOwned + Send + 'static,
        S: Clone + Send + Sync + 'static,
    {
        let topic = topic.name;
        let handler = move |state: S, messages: Vec<BorrowedMessage>| {
            let messages: Vec<Message<M>> = messages.into_iter().map(Message::from).collect();
            handle_messages(topic, messages, handler, &state)
        };

        self.handlers.insert(topic, Box::new(handler));
    }

    pub fn add_bulk_handler<H, Fut, M>(&mut self, topic: &Topic<M>, handler: H)
    where
        H: Fn(S, Vec<Message<M>>) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
        M: DeserializeOwned + Send + 'static,
    {
        let topic = topic.name;
        let handler = move |state: S, messages: Vec<BorrowedMessage>| {
            let messages: Vec<Message<M>> = messages.into_iter().map(Message::from).collect();
            handle_bulk_messages(topic, messages, handler, state)
        };

        self.handlers.insert(topic, Box::new(handler));
    }

    pub async fn start(self, state: S, shutdown_signal: CancellationToken) -> Result<(), Exception> {
        let handlers = self.handlers;
        let consumer: BaseConsumer = self.config.create()?;
        let topics: Vec<&str> = handlers.keys().copied().collect();
        consumer.subscribe(&topics)?;

        info!("kafka consumer started, topics={:?}", topics);

        loop {
            match poll_message_groups(&consumer, self.poll_max_wait_time, self.poll_max_records) {
                Ok(topic_messages) => {
                    let mut handles = Vec::with_capacity(topic_messages.len());
                    for (topic, messages) in topic_messages {
                        if let Some(handler) = handlers.get(topic.as_str()) {
                            handles.push(tokio::spawn(handler(state.clone(), messages)));
                        }
                    }
                    join_all(handles).await;
                    if let Err(e) = consumer.commit_consumer_state(CommitMode::Async) {
                        error!(error = ?e, "failed to commit messages");
                    }
                }
                Err(e) => {
                    error!(error = ?e, "failed to poll messages");
                    time::sleep(Duration::from_secs(5)).await;
                }
            }

            if shutdown_signal.is_cancelled() {
                info!("kafka consumer stopped, topics={:?}", topics);
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
                    kafka_header.value.map(|data| String::from_utf8_lossy(data).to_string()).unwrap_or_default(),
                );
            }
        }

        let timestamp = match message.timestamp() {
            Timestamp::CreateTime(time) => DateTime::from_timestamp_millis(time),
            Timestamp::NotAvailable | Timestamp::LogAppendTime(_) => None,
        };

        Message { key, payload: value.unwrap_or_default(), headers, timestamp, _marker: PhantomData }
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

fn handle_bulk_messages<H, S, M, Fut>(
    topic: &'static str,
    messages: Vec<Message<M>>,
    handler: H,
    state: S,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    S: Send + 'static,
    H: Fn(S, Vec<Message<M>>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    M: DeserializeOwned + Send + 'static,
{
    Box::pin(log::start_action("message", None, async move {
        context!(topic = topic, fn = type_name::<H>());
        let mut bytes = 0;
        for message in &messages {
            debug!(key = message.key, payload = message.payload, "[message]");
            bytes += message.payload.len();
        }
        stats!(kafka_read_messages = messages.len(), kafka_read_bytes = bytes);
        if let Some(timestamp) = messages.iter().filter_map(|message| message.timestamp).min() {
            let lag = Utc::now() - timestamp;
            debug!("lag={lag}");
        }
        handler(state, messages).await
    }))
}

struct MessageNode<M>
where
    M: DeserializeOwned,
{
    message: Message<M>,
    next: Option<Vec<MessageNode<M>>>,
}

fn handle_messages<H, S, M, Fut>(
    topic: &'static str,
    messages: Vec<Message<M>>,
    handler: H,
    state: &S,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    S: Clone + Send + 'static,
    H: Fn(S, Message<M>) -> Fut + Copy + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Exception>> + Send,
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
            handles.push(tokio::spawn(async move { handle_message(topic, message, handler, state).await }));
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

    Box::pin(async move {
        join_all(handles).await;
    })
}

async fn handle_message<H, S, M, Fut>(topic: &'static str, message: Message<M>, handler: H, state: S)
where
    H: Fn(S, Message<M>) -> Fut,
    Fut: Future<Output = Result<(), Exception>>,
    M: DeserializeOwned,
{
    let ref_id = message.headers.get("ref_id").map(String::to_owned);
    log::start_action("message", ref_id, async {
        context!(topic = topic, key = message.key, fn = type_name::<H>());
        debug!(timestamp = message.timestamp.map(|t| t.to_rfc3339_opts(SecondsFormat::Millis, true)), "[message]");
        debug!(payload = message.payload, "[message]");
        for (key, value) in &message.headers {
            debug!("[header] {}={}", key, value);
        }
        if let Some(client) = message.headers.get("client") {
            context!(client = client);
        }
        stats!(kafka_read_entries = 1, kafka_read_bytes = message.payload.len());
        if let Some(timestamp) = message.timestamp {
            let lag = Utc::now() - timestamp;
            debug!("lag={lag}");
        }
        handler(state, message).await
    })
    .await;
}
