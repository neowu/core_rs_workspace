use std::any::type_name;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use framework::console;
use framework::context;
use framework::exception;
use framework::exception::Exception;
use framework::json::from_json;
use framework::log;
use framework::log::metrics::Counter;
use framework::log::metrics::Metrics;
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
use rdkafka::message::OwnedMessage;
use rdkafka::util::Timeout;
use serde::de::DeserializeOwned;
use tokio::task::JoinSet;
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::CLIENT;
use crate::REF_ID;
use crate::Topic;

// decoded message handed to call-site handlers; the framework works with the raw rdkafka message.
pub struct Message<T: DeserializeOwned> {
    pub key: Option<String>,
    pub payload: String,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned> Message<T> {
    pub fn payload(&self) -> Result<T, Exception> {
        from_json(&self.payload)
            .map_err(|e| exception!("failed to decode message", code = "KAFKA_INVALID_MESSAGE", source = e))
    }
}

type MessageHandler<S> =
    Box<dyn Fn(S, Vec<BorrowedMessage>) -> Pin<Box<dyn Future<Output = Result<(), Exception>> + Send>> + Send>;

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
    counter: Arc<Counter>,
}

impl<S> MessageConsumer<S>
where
    S: Clone + Send + Sync + 'static,
{
    // group_id usually be env!("CARGO_BIN_NAME")
    pub fn new(bootstrap_servers: String, group_id: &'static str, config: &ConsumerConfig) -> Self {
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
            counter: Arc::new(Counter::new()),
        }
    }

    pub fn consumer_metrics(&self) -> impl Fn(&mut Metrics) + use<S> {
        let counter = Arc::clone(&self.counter);
        move |metrics| {
            metrics.stats.push(("active_message_handlers", counter.max() as u64));
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
        let counter = Arc::clone(&self.counter);
        let handler = move |state: S, messages: Vec<BorrowedMessage>| {
            let messages: Vec<OwnedMessage> = messages.iter().map(BorrowedMessage::detach).collect();
            handle_messages(topic, messages, handler, &state, &counter)
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
        let counter = Arc::clone(&self.counter);
        let handler = move |state: S, messages: Vec<BorrowedMessage>| {
            let messages: Vec<OwnedMessage> = messages.iter().map(BorrowedMessage::detach).collect();
            handle_bulk_messages(topic, messages, handler, state, Arc::clone(&counter))
        };

        self.handlers.insert(topic, Box::new(handler));
    }

    pub async fn start(self, state: S, shutdown_signal: CancellationToken) {
        let handlers = self.handlers;
        let consumer: BaseConsumer = self.config.create().expect("failed to create consumer"); // fail fast on startup
        let topics: Vec<&str> = handlers.keys().copied().collect();
        consumer.subscribe(&topics).expect("failed to subscribe topic"); // fail fast on startup

        console!("kafka consumer started, topics={:?}", topics);

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
                        console!("ERROR failed to commit messages, error={e:?}");
                    }
                }
                Err(e) => {
                    console!("ERROR failed to poll messages, error={e:?}");
                    time::sleep(Duration::from_secs(5)).await;
                }
            }

            if shutdown_signal.is_cancelled() {
                console!("kafka consumer stopped, topics={:?}", topics);
                return;
            }
        }
    }
}

impl<T: DeserializeOwned> From<&OwnedMessage> for Message<T> {
    fn from(message: &OwnedMessage) -> Message<T> {
        let key = message.key().map(|data| String::from_utf8_lossy(data).to_string());
        let payload = message.payload().map(|data| String::from_utf8_lossy(data).to_string()).unwrap_or_default();
        Message { key, payload, _marker: PhantomData }
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
    raw_messages: Vec<OwnedMessage>,
    handler: H,
    state: S,
    counter: Arc<Counter>,
) -> Pin<Box<dyn Future<Output = Result<(), Exception>> + Send>>
where
    S: Send + 'static,
    H: Fn(S, Vec<Message<M>>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    M: DeserializeOwned + Send + 'static,
{
    let ref_id = raw_messages
        .iter()
        .map(|raw| header(raw, REF_ID).map(str::to_owned))
        .collect::<Option<HashSet<String>>>()
        .map(|set| set.into_iter().collect::<Vec<String>>());

    Box::pin(log::start_action("message", ref_id, async move {
        let _counter = counter.increase();
        context!(topic = topic, fn = type_name::<H>());
        let mut bytes = 0;
        let messages: Vec<Message<M>> = raw_messages
            .iter()
            .map(|raw| {
                let message = Message::from(raw);
                log!("[message] key={:?}, payload={}", message.key, message.payload);
                bytes += message.payload.len();
                message
            })
            .collect();
        stats!(kafka_read_messages = messages.len(), kafka_read_bytes = bytes);
        if let Some(timestamp) = raw_messages.iter().filter_map(timestamp).min() {
            log!("[message] timestamp={:?}", timestamp.to_rfc3339_opts(SecondsFormat::Millis, true));
            let lag = Utc::now() - timestamp;
            stats!(kafka_consumer_lag = lag.num_nanoseconds().unwrap_or_default());
        }
        if let Some(clients) = raw_messages
            .iter()
            .map(|raw| header(raw, CLIENT).map(str::to_owned))
            .collect::<Option<HashSet<String>>>()
            .map(|set| set.into_iter().collect::<Vec<String>>())
        {
            context!(client = clients);
        }
        handler(state, messages).await
    }))
}

struct MessageNode {
    message: OwnedMessage,
    next: Option<Vec<MessageNode>>,
}

fn handle_messages<H, S, M, Fut>(
    topic: &'static str,
    messages: Vec<OwnedMessage>,
    handler: H,
    state: &S,
    counter: &Arc<Counter>,
) -> Pin<Box<dyn Future<Output = Result<(), Exception>> + Send>>
where
    S: Clone + Send + 'static,
    H: Fn(S, Message<M>) -> Fut + Copy + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    M: DeserializeOwned + Send + 'static,
{
    let mut handles = JoinSet::new();
    let mut nodes: HashMap<String, MessageNode> = HashMap::new();
    for message in messages {
        if let Some(key) = message.key().map(|data| String::from_utf8_lossy(data).to_string()) {
            if let Some(node) = nodes.get_mut(&key) {
                if let Some(ref mut next) = node.next {
                    next.push(MessageNode { message, next: None });
                } else {
                    node.next = Some(vec![MessageNode { message, next: None }]);
                }
            } else {
                nodes.insert(key, MessageNode { message, next: None });
            }
        } else {
            let state = state.clone();
            let counter = Arc::clone(counter);
            handles.spawn(async move {
                let _counter = counter.increase();
                handle_message(topic, message, handler, state).await;
            });
        }
    }

    for node in nodes.into_values() {
        let state = state.clone();
        let counter = Arc::clone(counter);
        handles.spawn(async move {
            let _counter = counter.increase();
            handle_message(topic, node.message, handler, state.clone()).await;
            if let Some(next) = node.next {
                for next_node in next {
                    handle_message(topic, next_node.message, handler, state.clone()).await;
                }
            }
        });
    }

    Box::pin(async move {
        handles.join_all().await;
        Ok(())
    })
}

async fn handle_message<H, S, M, Fut>(topic: &'static str, raw_message: OwnedMessage, handler: H, state: S)
where
    H: Fn(S, Message<M>) -> Fut,
    Fut: Future<Output = Result<(), Exception>>,
    M: DeserializeOwned,
{
    let ref_id = header(&raw_message, REF_ID).map(|id| vec![id.to_owned()]);
    let _result = log::start_action("message", ref_id, async {
        let message = Message::<M>::from(&raw_message);
        context!(topic = topic, key = format!("{:?}", message.key), fn = type_name::<H>());
        log!("[message] payload={}", message.payload);
        stats!(kafka_read_entries = 1, kafka_read_bytes = message.payload.len());
        if let Some(timestamp) = timestamp(&raw_message) {
            log!("[message] timestamp={:?}", timestamp.to_rfc3339_opts(SecondsFormat::Millis, true));
            let lag = Utc::now() - timestamp;
            stats!(kafka_consumer_lag = lag.num_nanoseconds().unwrap_or_default());
        }
        if let Some(client) = header(&raw_message, CLIENT) {
            context!(client = client);
        }
        handler(state, message).await
    })
    .await;
}

// ref_id and client headers are set and consumed by the framework only.
fn header<'a>(message: &'a OwnedMessage, name: &str) -> Option<&'a str> {
    let headers = message.headers()?;
    // headers are framework-written utf8; from_utf8 borrows on the happy path, falling back to "".
    headers
        .iter()
        .find(|header| header.key == name)
        .and_then(|header| header.value)
        .map(|data| from_utf8(data).unwrap_or_default())
}

fn timestamp(message: &OwnedMessage) -> Option<DateTime<Utc>> {
    match message.timestamp() {
        Timestamp::CreateTime(time) => DateTime::from_timestamp_millis(time),
        Timestamp::NotAvailable | Timestamp::LogAppendTime(_) => None,
    }
}
