use std::any::type_name;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::Duration;

use framework::console;
use framework::context;
use framework::exception;
use framework::exception::Exception;
use framework::json::from_json;
use framework::log;
use framework::metrics::Counter;
use framework::metrics::Metrics;
use framework::stats;
use framework::time::DateTime;
use futures::FutureExt as _;
use futures::StreamExt as _;
use futures::future::join_all;
use rdkafka::ClientConfig;
use rdkafka::Message as _;
use rdkafka::Timestamp;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer as _;
use rdkafka::consumer::StreamConsumer;
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::message::Headers as _;
use rdkafka::message::OwnedMessage;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::CLIENT;
use crate::REF_ID;
use crate::Topic;

// decoded message handed to call-site handlers; the framework works with the raw rdkafka message.
pub struct Message<T> {
    pub key: Option<String>,
    pub payload: T,
}

type MessageHandler<S> = Box<dyn Fn(S, Vec<BorrowedMessage>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub struct ConsumerConfig {
    // max in-flight handlers (semaphore size) across topics: each add_handler key group and each
    // add_bulk_handler call holds one permit.
    pub max_concurrency: usize,
    pub poll_max_wait_time: Duration,
    pub poll_max_records: usize,
    // "latest" or "earliest", where to start when the group has no committed offset for a partition
    pub auto_offset_reset: &'static str,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 100,
            poll_max_wait_time: Duration::from_secs(1),
            poll_max_records: 1000,
            auto_offset_reset: "latest",
        }
    }
}

pub struct MessageConsumer<S> {
    config: ClientConfig,
    handlers: HashMap<&'static str, MessageHandler<S>>,
    poll_max_wait_time: Duration,
    poll_max_records: usize,
    counter: Arc<Counter>,
    semaphore: Arc<Semaphore>,
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
                .set("auto.offset.reset", config.auto_offset_reset)
                .set_log_level(RDKafkaLogLevel::Info)
                .to_owned(),
            handlers: HashMap::new(),
            poll_max_wait_time: config.poll_max_wait_time,
            poll_max_records: config.poll_max_records,
            counter: Arc::default(),
            semaphore: Arc::new(Semaphore::new(config.max_concurrency)),
        }
    }

    pub fn metrics(&self) -> impl Fn(&mut Metrics) + use<S> {
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
        let semaphore = Arc::clone(&self.semaphore);
        let wrapper: MessageHandler<S> = Box::new(move |state: S, messages: Vec<BorrowedMessage>| {
            let messages: Vec<OwnedMessage> = messages.iter().map(BorrowedMessage::detach).collect();
            Box::pin(handle_messages(topic, messages, handler, state, Arc::clone(&counter), Arc::clone(&semaphore)))
        });

        self.handlers.insert(topic, wrapper);
    }

    pub fn add_bulk_handler<H, Fut, M>(&mut self, topic: &Topic<M>, handler: H)
    where
        H: Fn(S, Vec<Message<M>>) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
        M: DeserializeOwned + Send + 'static,
    {
        let topic = topic.name;
        let counter = Arc::clone(&self.counter);
        let semaphore = Arc::clone(&self.semaphore);
        let wrapper: MessageHandler<S> = Box::new(move |state: S, messages: Vec<BorrowedMessage>| {
            let messages: Vec<OwnedMessage> = messages.iter().map(BorrowedMessage::detach).collect();
            Box::pin(handle_bulk_messages(
                topic,
                messages,
                handler,
                state,
                Arc::clone(&counter),
                Arc::clone(&semaphore),
            ))
        });

        self.handlers.insert(topic, wrapper);
    }

    pub async fn start(self, state: S, shutdown_signal: CancellationToken) {
        let topics: Vec<&str> = self.handlers.keys().copied().collect();
        console!(
            "start kafka consumer, broker={}, topics={:?}",
            self.config.get("bootstrap.servers").expect("broker must not be null"),
            topics
        );

        let consumer: StreamConsumer = self.config.create().expect("failed to create consumer"); // fail fast on startup
        consumer.subscribe(&topics).expect("failed to subscribe topic"); // fail fast on startup

        loop {
            let mut topic_messages = HashMap::new();
            let result = poll_message_groups(
                &consumer,
                &mut topic_messages,
                self.poll_max_wait_time,
                self.poll_max_records,
                &shutdown_signal,
            )
            .await;

            // messages polled before an error are still handled, otherwise the next commit would skip them
            if !topic_messages.is_empty() {
                let mut handles = Vec::with_capacity(topic_messages.len());
                for (topic, messages) in topic_messages {
                    if let Some(handler) = self.handlers.get(topic.as_str()) {
                        handles.push(tokio::spawn(handler(state.clone(), messages)));
                    }
                }
                join_all(handles).await;
                if let Err(e) = consumer.commit_consumer_state(CommitMode::Async) {
                    console!("ERROR failed to commit messages, error={e:?}");
                }
            }

            if let Err(e) = result {
                console!("ERROR failed to poll messages, error={e:?}");
                tokio::select! {
                    () = time::sleep(Duration::from_secs(5)) => {}
                    () = shutdown_signal.cancelled() => {}
                }
            }

            if shutdown_signal.is_cancelled() {
                console!("kafka consumer stopped, topics={:?}", topics);
                return;
            }
        }
    }
}

// collects until max_records, max_wait_time, or shutdown; StreamConsumer polls librdkafka without blocking
async fn poll_message_groups<'a>(
    consumer: &'a StreamConsumer,
    messages: &mut HashMap<String, Vec<BorrowedMessage<'a>>>,
    max_wait_time: Duration,
    max_records: usize,
    shutdown_signal: &CancellationToken,
) -> Result<(), KafkaError> {
    let deadline = Instant::now() + max_wait_time;
    let mut stream = consumer.stream();
    let mut count = 0;
    while count < max_records {
        tokio::select! {
            result = stream.next() => {
                let message = result.expect("kafka streams never terminate")?;
                let topic = message.topic().to_owned();
                messages.entry(topic).or_default().push(message);
                count += 1;
            }
            () = time::sleep_until(deadline) => break,
            () = shutdown_signal.cancelled() => break,
        }
    }
    Ok(())
}

// the bulk handler holds one permit, as a key group does
async fn handle_bulk_messages<H, S, M, Fut>(
    topic: &'static str,
    raw_messages: Vec<OwnedMessage>,
    handler: H,
    state: S,
    counter: Arc<Counter>,
    semaphore: Arc<Semaphore>,
) where
    S: Send + 'static,
    H: Fn(S, Vec<Message<M>>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    M: DeserializeOwned + Send + 'static,
{
    // acquired outside the action, so the wait is not counted in its elapsed
    let _permit = semaphore.acquire().await.expect("semaphore should not close");
    let ref_id = raw_messages
        .iter()
        .map(|raw| header(raw, REF_ID).map(str::to_owned))
        .collect::<Option<HashSet<String>>>()
        .map(|set| set.into_iter().collect::<Vec<String>>());

    let _result = log::action("message", ref_id, async move {
        let _counter = counter.increase();
        context!(topic = topic, fn = type_name::<H>());
        let mut bytes = 0;
        let mut messages: Vec<Message<M>> = Vec::with_capacity(raw_messages.len());
        for raw in &raw_messages {
            let key = key(raw);
            let payload = payload(raw);
            log!("[message] key={:?}, payload={}", key, payload);
            bytes += payload.len();
            match from_json::<M>(&payload) {
                Ok(payload) => messages.push(Message { key, payload }),
                Err(e) => {
                    log!(
                        exception = exception!("failed to decode message", code = "KAFKA_INVALID_MESSAGE", source = e)
                    );
                }
            }
        }
        stats!(kafka_read_messages = messages.len(), kafka_read_bytes = bytes);
        if let Some(timestamp) = raw_messages.iter().filter_map(timestamp).min() {
            log!("[message] timestamp={}", timestamp.to_rfc3339());
            let lag = (DateTime::now() - timestamp).as_nanos();
            if lag > 0 {
                stats!(kafka_consumer_lag = lag);
            }
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
    })
    .await;
}

// messages with the same key are handled sequentially in order, each key group (or unkeyed message) runs in its
// own task, bounded by the consumer semaphore
async fn handle_messages<H, S, M, Fut>(
    topic: &'static str,
    messages: Vec<OwnedMessage>,
    handler: H,
    state: S,
    counter: Arc<Counter>,
    semaphore: Arc<Semaphore>,
) where
    S: Clone + Send + 'static,
    H: Fn(S, Message<M>) -> Fut + Copy + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    M: DeserializeOwned + Send + 'static,
{
    let mut key_groups: HashMap<String, Vec<OwnedMessage>> = HashMap::new();
    let mut groups: Vec<Vec<OwnedMessage>> = Vec::new();
    for message in messages {
        if let Some(key) = key(&message) {
            key_groups.entry(key).or_default().push(message);
        } else {
            groups.push(vec![message]);
        }
    }
    groups.extend(key_groups.into_values());

    let mut handles = JoinSet::new();
    for group in groups {
        let permit = Arc::clone(&semaphore).acquire_owned().await.expect("semaphore should not close");
        let state = state.clone();
        let counter = Arc::clone(&counter);
        handles.spawn(async move {
            let _permit = permit;
            let _counter = counter.increase();
            for message in group {
                handle_message(topic, message, handler, state.clone()).await;
            }
        });
    }
    handles.join_all().await;
}

fn handle_message<H, S, M, Fut>(
    topic: &'static str,
    raw_message: OwnedMessage,
    handler: H,
    state: S,
) -> impl Future<Output = ()>
where
    H: Fn(S, Message<M>) -> Fut,
    Fut: Future<Output = Result<(), Exception>>,
    M: DeserializeOwned,
{
    let ref_id = header(&raw_message, REF_ID).map(|id| vec![id.to_owned()]);
    log::action("message", ref_id, async move {
        let key = key(&raw_message);
        let payload = payload(&raw_message);
        context!(topic = topic, key = format!("{:?}", key), fn = type_name::<H>());
        log!("[message] payload={}", payload);
        stats!(kafka_read_entries = 1, kafka_read_bytes = payload.len());
        if let Some(timestamp) = timestamp(&raw_message) {
            log!("[message] timestamp={}", timestamp.to_rfc3339());
            let lag = (DateTime::now() - timestamp).as_nanos();
            if lag > 0 {
                stats!(kafka_consumer_lag = lag);
            }
        }
        if let Some(client) = header(&raw_message, CLIENT) {
            context!(client = client);
        }
        let payload: M = from_json(&payload)
            .map_err(|e| exception!("failed to decode message", code = "KAFKA_INVALID_MESSAGE", source = e))?;
        handler(state, Message { key, payload }).await
    })
    .map(drop)
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

fn key(message: &OwnedMessage) -> Option<String> {
    message.key().map(|data| String::from_utf8_lossy(data).to_string())
}

fn payload(message: &OwnedMessage) -> String {
    message.payload().map(|data| String::from_utf8_lossy(data).to_string()).unwrap_or_default()
}

fn timestamp(message: &OwnedMessage) -> Option<DateTime> {
    match message.timestamp() {
        Timestamp::CreateTime(time) => DateTime::from_unix_timestamp_nanos(time as i128 * 1_000_000).ok(),
        Timestamp::NotAvailable | Timestamp::LogAppendTime(_) => None,
    }
}
